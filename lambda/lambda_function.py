"""
CGV 서울 IMAX 예매 오픈 알리미 - AWS Lambda (서울 리전) 버전, 구독자별 극장 선택

하나의 Lambda 가 세 가지 일을 합니다.
  1) EventBridge 1분 스케줄  -> 구독자가 있는 극장만 조회, 새 IMAX 회차를 구독자별로 알림
     매분: 새 날짜 + 아직 IMAX가 없는 날짜만 확인 (빠른 조회)
     5분마다: 모든 날짜 확인 (이미 IMAX가 있는 날짜에 회차가 추가되는 경우)
     (실행이 겹치면 건너뜀, 연속 실패 시 조회 간격을 2,4,8..최대 30분으로 늘림)
  2) Function URL (텔레그램 웹훅) -> 구독자가 봇에서 극장을 고르는 명령/버튼 처리
  3) 콘솔 테스트 이벤트       -> {"test_notify": true}, {"setup_webhook": "<Function URL>"}
조회만 합니다 (로그인/예매/결제 없음).

환경변수
  STATE_BUCKET        상태/구독자 파일을 저장할 S3 버킷 (필수)
  TELEGRAM_BOT_TOKEN  텔레그램 봇 토큰 (필수)
  TELEGRAM_CHAT_ID    관리자 chat id - 실패 경고 수신, 최초 실행 시 전체 극장 구독으로 등록
  MOVIE_KEYWORD       특정 영화만 받으려면 제목 일부 (선택)

S3 파일
  cgv-imax-state.json   이미 본 회차, 추적 중인 극장, 연속 실패 횟수
  scan.lock             실행 중 표시 (겹침 방지)
  subscribers.json      구독자별 선택 극장
"""

import base64
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError, ParamValidationError

# 서울 IMAX 극장 (CGV 특별관 목록 기준)
SEOUL_IMAX = {
    "0013": "용산아이파크몰",
    "0040": "압구정",
    "0059": "영등포타임스퀘어",
    "0074": "왕십리",
    "0199": "천호",
}
CO_CD = "A420"
BASE = "https://cgv.co.kr/api/v1/booking"
BOOKING_URL = "https://cgv.co.kr/cnm/movieBook/cinema"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (personal open-alert script)",
    "Accept": "application/json",
    "Referer": BOOKING_URL,
}
REQUEST_GAP_SEC = 0.3
FAIL_ALERT_AT = 8  # 백오프 포함 약 30분 이상 연속 실패 시 관리자에게 한 번 경고
MAX_BACKOFF_MIN = 30
LOCK_KEY = "scan.lock"
LOCK_TTL_SEC = 150  # 이보다 오래된 잠금은 비정상 종료로 보고 무시
FULL_SCAN_SEC = 290  # 전체 날짜 조회 간격 (약 5분)
STATE_KEY = "cgv-imax-state.json"
SUBS_KEY = "subscribers.json"
STATE_VERSION = 3  # v3: 추적 극장(tracked) 추가
KST = timezone(timedelta(hours=9))

ON = "✅"
OFF = "⬜"
ICON_OPEN = "\U0001F3AC"
ICON_TEST = "\U0001F514"
ICON_WARN = "⚠️"

s3 = boto3.client("s3")


def now_kst():
    return datetime.now(KST)


def bucket():
    return os.environ["STATE_BUCKET"]


def fmt_time(t):
    t = str(t or "")
    return f"{t[:2]}:{t[2:4]}" if len(t) >= 4 else t


def fmt_date(ymd):
    d = datetime.strptime(ymd, "%Y%m%d")
    return f"{d.month}/{d.day}({'월화수목금토일'[d.weekday()]})"


def site_names(nos):
    return ", ".join(SEOUL_IMAX[n] for n in SEOUL_IMAX if n in nos) or "없음"


# ───────────────────────── 텔레그램 ─────────────────────────
class TelegramError(Exception):
    def __init__(self, code, desc):
        super().__init__(f"{code} {desc}")
        self.code, self.desc = code, desc


def tg(method, **params):
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=json.dumps(params, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            return json.load(res)
    except urllib.error.HTTPError as e:
        try:
            desc = json.load(e).get("description", "")
        except Exception:
            desc = ""
        raise TelegramError(e.code, desc) from None


def send(chat_id, text, **extra):
    print(f"-> {chat_id}: {text[:200]}")
    return tg("sendMessage", chat_id=chat_id, text=text[:4000],
              disable_web_page_preview=True, **extra)


def admin_notify(text):
    admin = os.getenv("TELEGRAM_CHAT_ID")
    if admin:
        try:
            send(admin, text)
        except Exception as e:
            print(f"관리자 알림 실패: {e}")


def webhook_secret():
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    return hashlib.sha256(f"cgv-webhook:{token}".encode()).hexdigest()[:48]


# ───────────────────────── S3 ─────────────────────────
def s3_get(key):
    """(dict, etag) 또는 (None, None)"""
    try:
        obj = s3.get_object(Bucket=bucket(), Key=key)
    except s3.exceptions.NoSuchKey:
        return None, None
    return json.loads(obj["Body"].read()), obj.get("ETag")


def s3_put(key, data, etag=None, create_only=False):
    kwargs = dict(Bucket=bucket(), Key=key, ContentType="application/json",
                  Body=json.dumps(data, ensure_ascii=False, indent=1).encode("utf-8"))
    cond = {"IfMatch": etag} if etag else ({"IfNoneMatch": "*"} if create_only else {})
    try:
        s3.put_object(**kwargs, **cond)
    except ParamValidationError:  # 조건부 쓰기를 모르는 오래된 boto3
        s3.put_object(**kwargs)


def default_subs():
    admin = os.getenv("TELEGRAM_CHAT_ID")
    if not admin:
        return {}
    return {str(admin): {"sites": list(SEOUL_IMAX), "name": "관리자",
                         "joined": now_kst().isoformat(timespec="seconds")}}


def load_subs():
    data, etag = s3_get(SUBS_KEY)
    if data is None:
        return default_subs(), None
    return data.get("subs", {}), etag


def update_subs(fn):
    """구독자 파일을 읽고 fn(subs) 로 수정해 저장. 동시 수정 시 재시도."""
    for _ in range(4):
        subs, etag = load_subs()
        result = fn(subs)
        try:
            s3_put(SUBS_KEY, {"v": 1, "subs": subs}, etag=etag, create_only=etag is None)
            return subs, result
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("PreconditionFailed", "ConditionalRequestConflict"):
                time.sleep(0.2)
                continue
            raise
    raise RuntimeError("구독자 파일 저장 충돌")


# ───────────────────────── 봇 (웹훅) ─────────────────────────
HELP = ("CGV 서울 IMAX 예매 오픈 알리미예요.\n"
        "선택한 극장에 새 IMAX 회차가 열리면 1~2분 안에 알려드려요.\n\n"
        "/settings - 알림 받을 극장 고르기\n"
        "/status - 지금 구독 중인 극장\n"
        "/stop - 알림 끄기")


def keyboard(selected):
    rows = [[{"text": f"{ON if no in selected else OFF} {name}", "callback_data": f"t:{no}"}]
            for no, name in SEOUL_IMAX.items()]
    rows.append([{"text": "전체 선택", "callback_data": "all"},
                 {"text": "전체 해제", "callback_data": "none"}])
    rows.append([{"text": "완료", "callback_data": "done"}])
    return {"inline_keyboard": rows}


def settings_text(selected):
    return ("알림 받을 극장을 눌러서 켜고 끄세요.\n"
            f"현재: {site_names(selected)}")


def handle_message(msg):
    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()
    cmd = text.split()[0].split("@")[0].lower() if text else ""
    subs, _ = load_subs()
    mine = subs.get(chat_id, {}).get("sites", [])

    if cmd in ("/start", "/settings"):
        if cmd == "/start":
            send(chat_id, HELP)
        send(chat_id, settings_text(mine), reply_markup=keyboard(mine))
    elif cmd == "/status":
        send(chat_id, f"구독 중인 극장: {site_names(mine)}\n변경: /settings")
    elif cmd == "/stop":
        update_subs(lambda s: s.pop(chat_id, None))
        send(chat_id, "알림을 껐어요. 다시 받으려면 /start 를 눌러 주세요.")
    else:
        send(chat_id, HELP)


def handle_callback(cb):
    chat_id = str(cb["message"]["chat"]["id"])
    message_id = cb["message"]["message_id"]
    data = cb.get("data", "")
    who = cb.get("from", {}).get("first_name", "")

    def apply(subs):
        entry = subs.get(chat_id) or {"sites": [], "name": who,
                                      "joined": now_kst().isoformat(timespec="seconds")}
        sites = set(entry["sites"])
        note = ""
        if data.startswith("t:") and data[2:] in SEOUL_IMAX:
            no = data[2:]
            if no in sites:
                sites.discard(no)
                note = f"{SEOUL_IMAX[no]} 알림 끔"
            else:
                sites.add(no)
                note = f"{SEOUL_IMAX[no]} 알림 켬"
        elif data == "all":
            sites, note = set(SEOUL_IMAX), "전체 선택"
        elif data == "none":
            sites, note = set(), "전체 해제"
        entry["sites"] = [n for n in SEOUL_IMAX if n in sites]
        if entry["sites"]:
            subs[chat_id] = entry
        else:
            subs.pop(chat_id, None)
        return entry["sites"], note

    if data == "done":
        subs, _ = load_subs()
        mine = subs.get(chat_id, {}).get("sites", [])
        tg("answerCallbackQuery", callback_query_id=cb["id"])
        tg("editMessageText", chat_id=chat_id, message_id=message_id,
           text=(f"설정 완료! 알림 받는 극장: {site_names(mine)}\n"
                 "새 IMAX 회차가 열리면 알려드릴게요. 변경: /settings")
           if mine else "선택한 극장이 없어서 알림이 오지 않아요. /settings 로 다시 고를 수 있어요.")
        return

    _, (mine, note) = update_subs(apply)
    tg("answerCallbackQuery", callback_query_id=cb["id"], text=note)
    try:
        tg("editMessageText", chat_id=chat_id, message_id=message_id,
           text=settings_text(mine), reply_markup=keyboard(mine))
    except TelegramError as e:
        if "not modified" not in e.desc:
            raise


def handle_webhook(event):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    if not hmac.compare_digest(headers.get("x-telegram-bot-api-secret-token", ""), webhook_secret()):
        return {"statusCode": 403, "body": "forbidden"}
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    update = json.loads(body)
    try:
        if "callback_query" in update:
            handle_callback(update["callback_query"])
        elif "message" in update:
            handle_message(update["message"])
    except Exception as e:  # 텔레그램이 같은 업데이트를 계속 재전송하지 않도록 200 응답
        print(f"웹훅 처리 실패: {e}")
    return {"statusCode": 200, "body": "ok"}


def setup_webhook(url):
    r1 = tg("setWebhook", url=url, secret_token=webhook_secret(),
            allowed_updates=["message", "callback_query"], drop_pending_updates=True)
    r2 = tg("setMyCommands", commands=[
        {"command": "settings", "description": "알림 받을 극장 고르기"},
        {"command": "status", "description": "구독 중인 극장 보기"},
        {"command": "stop", "description": "알림 끄기"},
    ])
    return {"setWebhook": r1, "setMyCommands": r2}


# ───────────────────────── CGV 조회 ─────────────────────────
def get_json(path, **params):
    url = f"{BASE}/{path}?" + urllib.parse.urlencode({"coCd": CO_CD, **params})
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as res:
        body = json.load(res)
    if body.get("statusCode") != 0:
        raise RuntimeError(f"{path}: {body.get('statusMessage')}")
    return body.get("data") or []


def is_target(s, movie_kw):
    hall = " ".join(str(s.get(k) or "") for k in
                    ("scnsNm", "expoScnsNm", "tcscnsGradNm", "movkndDsplNm")).lower()
    movie = " ".join(str(s.get(k) or "") for k in ("movNm", "prodNm", "expoProdNm")).lower()
    return ("imax" in hall or "아이맥스" in hall) and (not movie_kw or movie_kw.lower() in movie)


def show_key(site_no, s):
    return f"{s['scnYmd']}|{site_no}|{s['scnsNo']}|{s['scnSseq']}|{s.get('scnsrtTm')}"


def key_site(key):
    return key.split("|")[1]


def scan_site(site_no, today, movie_kw, skip_dates=()):
    """skip_dates 에 있는 날짜는 건너뜀. (찾은 회차, 확인한 날짜, IMAX가 있는 날짜) 반환"""
    found, imax_dates = {}, set()
    dates = [d["scnYmd"] for d in get_json("searchSiteScnscYmdListBySite", siteNo=site_no)
             if d["scnYmd"] >= today]
    checked = [d for d in dates if d not in skip_dates]
    for ymd in checked:
        time.sleep(REQUEST_GAP_SEC)
        for s in get_json("searchMovScnInfo", siteNo=site_no, scnYmd=ymd, rtctlScopCd="08"):
            if is_target(s, movie_kw):
                found[show_key(site_no, s)] = s
                imax_dates.add(ymd)
    return found, set(dates), imax_dates


def load_state():
    data, _ = s3_get(STATE_KEY)
    if not data:
        return {"seen": set(), "tracked": set(), "failures": 0, "imax_dates": {}, "full_at": 0}
    st = {"seen": set(data.get("seen", [])), "failures": int(data.get("failures", 0)),
          "tracked": set(data.get("tracked", [])),
          "imax_dates": {k: set(v) for k, v in (data.get("imax_dates") or {}).items()},
          "full_at": float(data.get("full_at", 0))}
    if data.get("v") == 2:  # 서울 5곳 전체를 추적하던 이전 버전
        st["tracked"] = set(SEOUL_IMAX)
    elif data.get("v") != STATE_VERSION:  # 그보다 오래된 형식은 처음부터
        st["seen"], st["tracked"] = set(), set()
    return st


def save_state(st):
    today = now_kst().strftime("%Y%m%d")
    s3_put(STATE_KEY, {
        "v": STATE_VERSION,
        "seen": sorted(k for k in st["seen"] if k[:8] >= today and key_site(k) in st["tracked"]),
        "tracked": sorted(st["tracked"]),
        "failures": st["failures"],
        "imax_dates": {k: sorted(d for d in v if d >= today)
                       for k, v in st["imax_dates"].items() if k in st["tracked"]},
        "full_at": st["full_at"],
        "updated": now_kst().isoformat(timespec="seconds"),
    })


def build_alert(shows):
    grouped = {}
    for site_no, s in sorted(shows, key=lambda x: (list(SEOUL_IMAX).index(x[0]), x[1]["scnYmd"],
                                                   str(x[1].get("scnsrtTm")))):
        grouped.setdefault(site_no, {}).setdefault(s["scnYmd"], {}).setdefault(
            s.get("movNm") or s.get("prodNm"), []).append(fmt_time(s.get("scnsrtTm")))
    lines = []
    for site_no, by_day in grouped.items():
        lines.append(f"[{SEOUL_IMAX[site_no]}]")
        for ymd, movies in by_day.items():
            for mv, times in movies.items():
                lines.append(f"• {fmt_date(ymd)} {mv}: {', '.join(times)}")
    names = ", ".join(SEOUL_IMAX[n] for n in grouped)
    return (f"{ICON_OPEN} IMAX 예매 오픈! {names} 새 회차 {len(shows)}개\n"
            + "\n".join(lines[:50]) + f"\n{BOOKING_URL}\n\n알림 극장 변경: /settings")


def acquire_lock():
    body = json.dumps({"at": time.time()}).encode()
    try:
        s3.put_object(Bucket=bucket(), Key=LOCK_KEY, Body=body, IfNoneMatch="*")
        return True
    except ParamValidationError:
        pass
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("PreconditionFailed", "ConditionalRequestConflict"):
            raise
    data, _ = s3_get(LOCK_KEY)
    if data and time.time() - float(data.get("at", 0)) < LOCK_TTL_SEC:
        return False  # 이전 실행이 아직 도는 중
    s3.put_object(Bucket=bucket(), Key=LOCK_KEY, Body=body)  # 오래된 잠금은 덮어씀
    return True


def release_lock():
    try:
        # 삭제 권한 없이도 되도록 '해제됨(at=0)'으로 덮어씀
        s3.put_object(Bucket=bucket(), Key=LOCK_KEY, Body=json.dumps({"at": 0}).encode())
    except Exception as e:
        print(f"잠금 해제 실패: {e}")


def backoff_skip(failures):
    """연속 실패 중이면 2^n 분 간격으로만 조회 (최대 30분)"""
    if failures <= 0:
        return False
    every = min(2 ** failures, MAX_BACKOFF_MIN)
    minute = int(time.time() // 60)
    return minute % every != 0


def run_scan():
    if not acquire_lock():
        return "skip: previous run still in progress"
    try:
        return _run_scan()
    finally:
        release_lock()


def _run_scan():
    subs, subs_etag = load_subs()
    if subs_etag is None and subs:  # 첫 실행: 관리자를 전체 극장 구독자로 저장
        update_subs(lambda s: None)
    needed = [n for n in SEOUL_IMAX if any(n in u.get("sites", []) for u in subs.values())]
    st = load_state()
    if backoff_skip(st["failures"]):
        return f"skip: backoff (failures {st['failures']})"
    movie_kw = os.getenv("MOVIE_KEYWORD", "").strip()
    today = now_kst().strftime("%Y%m%d")

    if not needed:
        st["tracked"] = set()
        st["failures"] = 0
        save_state(st)
        return "no subscribers"

    full = time.time() - st["full_at"] >= FULL_SCAN_SEC
    found, ok_sites, errors = {}, set(), []
    for no in needed:
        # 빠른 조회: 이미 IMAX가 있는 날짜는 건너뜀 (새로 추적하는 극장은 항상 전체 조회)
        skip = st["imax_dates"].get(no, set()) if (not full and no in st["tracked"]) else set()
        try:
            f, open_dates, imax_dates = scan_site(no, today, movie_kw, skip)
            found.update(f)
            prev = st["imax_dates"].get(no, set())
            st["imax_dates"][no] = (imax_dates if not skip else (prev & open_dates) | imax_dates)
            ok_sites.add(no)
        except Exception as e:
            errors.append(f"{SEOUL_IMAX[no]}: {e}")
            print(f"[{SEOUL_IMAX[no]}] 조회 실패: {e}")

    if not ok_sites:
        st["failures"] += 1
        if st["failures"] == FAIL_ALERT_AT:
            admin_notify(f"{ICON_WARN} CGV 알리미 조회가 계속 실패하고 있어요\n"
                         + "; ".join(errors)[:300] + "\nCGV API가 바뀌었거나 접속이 막혔을 수 있어요.")
        st["tracked"] &= set(needed)
        save_state(st)
        return f"fail {st['failures']}: {'; '.join(errors)}"

    if st["failures"] >= FAIL_ALERT_AT:
        admin_notify("✅ CGV 알리미 조회가 다시 정상화됐어요")
    st["failures"] = 0

    # 이전부터 추적하던 극장의 새 회차만 알림. 새로 구독자가 생긴 극장은 조용히 기준선만 저장
    new = {k: s for k, s in found.items() if k not in st["seen"] and key_site(k) in st["tracked"]}
    newly_tracked = ok_sites - st["tracked"]

    sent = 0
    blocked = []
    if new:
        for chat_id, u in subs.items():
            mine = [(key_site(k), s) for k, s in new.items() if key_site(k) in u.get("sites", [])]
            if not mine:
                continue
            try:
                send(chat_id, build_alert(mine))
                sent += 1
            except TelegramError as e:
                print(f"{chat_id} 전송 실패: {e}")
                if e.code == 403:  # 봇을 차단하거나 대화를 삭제한 사용자
                    blocked.append(chat_id)
            except Exception as e:
                print(f"{chat_id} 전송 실패: {e}")
    if blocked:
        update_subs(lambda s: [s.pop(c, None) for c in blocked])

    st["seen"] |= set(found)
    if full and not errors:
        st["full_at"] = time.time()
    # 이번에 실패한 극장은 추적 상태를 유지 (다음 성공 때 놓친 회차를 알림)
    st["tracked"] = ok_sites | (st["tracked"] & set(needed))
    save_state(st)

    result = (f"{'full' if full else 'quick'}: subs {len(subs)}, sites {len(needed)}, "
              f"imax {len(found)}, new {len(new)}, "
              f"sent {sent}, baselined {sorted(newly_tracked)}")
    if errors:
        result += f", partial errors {len(errors)}"
    return result


# ───────────────────────── 진입점 ─────────────────────────
def lambda_handler(event, context):
    event = event or {}
    if "http" in (event.get("requestContext") or {}):  # Function URL (텔레그램 웹훅)
        return handle_webhook(event)
    if event.get("test_notify"):
        admin_notify(f"{ICON_TEST} CGV 알리미 테스트\n알림이 정상적으로 도착했어요.")
        return {"result": "test notify sent"}
    if event.get("setup_webhook"):
        return setup_webhook(event["setup_webhook"])
    result = run_scan()
    print(result)
    return {"result": result}
