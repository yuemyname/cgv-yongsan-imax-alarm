"""
CGV 서울 IMAX 예매 오픈 알리미 - AWS Lambda (서울 리전) 버전, 구독자별 극장/영화 선택

하나의 Lambda 가 세 가지 일을 합니다.
  1) EventBridge 1분 스케줄  -> 구독자가 있는 극장만 조회, 새 IMAX 회차를 구독자별로 알림
     기본: 매분 모든 날짜 확인 (극장들은 동시에 조회)
     FULL_SCAN_SEC 를 주면: 그 간격으로만 전체 조회, 나머지는 새 날짜 + IMAX 없는 날짜만 (빠른 조회)
     (실행이 겹치면 건너뜀, 연속 실패 시 조회 간격을 2,4,8..최대 30분으로 늘림)
     CGV 요청이 실패하면 관리자에게 바로 알림 (어느 극장, 어느 API, HTTP 코드). 같은 오류는
     정상화될 때까지 한 번만 알리고, 정상화되면 복구 알림. 로그에는 "[API 오류]" 로 남김
     구독자가 고른 영화만 알림. 단, 극장에 처음 IMAX 예매가 열린 영화는 영화 선택과 상관없이 알림
  2) Function URL (텔레그램 웹훅) -> 구독자가 봇에서 극장/영화를 고르는 명령/버튼 처리
  3) 콘솔 테스트 이벤트       -> {"test_notify": true}, {"setup_webhook": "<Function URL>"}
조회만 합니다 (로그인/예매/결제 없음).

환경변수
  STATE_BUCKET        상태/구독자 파일을 저장할 S3 버킷 (필수)
  TELEGRAM_BOT_TOKEN  텔레그램 봇 토큰 (필수)
  TELEGRAM_CHAT_ID    관리자 chat id - 실패 경고 수신, 최초 실행 시 전체 극장 구독으로 등록
  MOVIE_KEYWORD       특정 영화만 받으려면 제목 일부 (선택)
  FULL_SCAN_SEC       전체 날짜 조회 간격(초). 기본 0 = 매번 전체 조회, 290 = 약 5분마다 (선택)

S3 파일
  cgv-imax-state.json   이미 본 회차, 추적 중인 극장, 극장별로 본 IMAX 영화, 연속 실패 횟수
  scan.lock             실행 중 표시 (겹침 방지)
  subscribers.json      구독자별 선택 극장/영화 (movies 가 없으면 전체 영화)
"""

import base64
from concurrent.futures import ThreadPoolExecutor
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
MAX_BACKOFF_MIN = 30
LOCK_KEY = "scan.lock"
LOCK_TTL_SEC = 150  # 이보다 오래된 잠금은 비정상 종료로 보고 무시
FULL_SCAN_SEC = int(os.getenv("FULL_SCAN_SEC", "0") or 0)  # 0 = 매번 전체 조회
STATE_KEY = "cgv-imax-state.json"
SUBS_KEY = "subscribers.json"
STATE_VERSION = 4  # v3: 추적 극장(tracked), v4: 극장별로 본 IMAX 영화(known)
KNOWN_KEEP_DAYS = 60  # 마지막 상영일이 이보다 오래 지난 영화는 잊음 (재개봉하면 다시 '첫 오픈')
KST = timezone(timedelta(hours=9))

ON = "✅"
OFF = "⬜"
ICON_OPEN = "\U0001F3AC"
ICON_TEST = "\U0001F514"
ICON_WARN = "⚠️"
ICON_NEW = "\U0001F195"

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


def mov_no(s):
    return str(s.get("movNo") or s.get("movNm") or s.get("prodNm") or "")


def mov_name(s):
    return s.get("movNm") or s.get("prodNm") or s.get("expoProdNm") or mov_no(s)


def movie_names(movies):
    """구독자의 movies 값(None = 전체, dict = 고른 영화) 을 글로"""
    if movies is None:
        return "전체 영화"
    return ", ".join(sorted(movies.values())) or "없음 (처음 열리는 영화만)"


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
        "선택한 극장에 새 IMAX 회차가 열리면 1~2분 안에 알려드려요.\n"
        "처음으로 IMAX 예매가 열린 영화는 영화 선택과 상관없이 알려드려요.\n\n"
        "/settings - 알림 받을 극장 고르기\n"
        "/movies - 알림 받을 영화 고르기\n"
        "/status - 지금 설정 보기\n"
        "/stop - 알림 끄기")
FIRST_NOTE = "처음으로 IMAX 예매가 열리는 영화는 항상 알려드려요."


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


def current_movies():
    """지금 서울 IMAX 극장에서 예매가 열려 있는 IMAX 영화 {movNo: 이름}"""
    today = now_kst().strftime("%Y%m%d")
    out = {}
    for by_mov in load_state()["known"].values():
        for no, (name, last) in by_mov.items():
            if last >= today:
                out[no] = name
    return out


def movie_keyboard(movies, showing):
    """movies: None = 전체 영화, dict = 고른 영화. showing: 지금 열린 IMAX 영화"""
    listed = dict(showing)
    listed.update(movies or {})  # 골랐지만 지금은 상영이 없는 영화도 끌 수 있게
    rows = [[{"text": f"{ON if movies is None else OFF} 전체 영화 (새로 열리는 영화 포함)",
              "callback_data": "ma"}]]
    for no, name in sorted(listed.items(), key=lambda x: x[1])[:40]:
        mark = ON if movies is not None and no in movies else OFF
        tail = "" if no in showing else " (상영 없음)"
        rows.append([{"text": f"{mark} {name}{tail}"[:60], "callback_data": f"m:{no}"[:64]}])
    rows.append([{"text": "완료", "callback_data": "mdone"}])
    return {"inline_keyboard": rows}


def movies_text(movies, showing):
    head = ("알림 받을 영화를 고르세요. 영화를 하나라도 고르면 고른 영화만 알려드려요.\n"
            if showing or movies else "아직 확인된 IMAX 영화가 없어요.\n")
    return f"{head}현재: {movie_names(movies)}\n{FIRST_NOTE}"


def status_text(entry):
    return (f"극장: {site_names(entry.get('sites', []))}\n"
            f"영화: {movie_names(entry.get('movies'))}\n{FIRST_NOTE}\n"
            "변경: /settings (극장), /movies (영화)")


def handle_message(msg):
    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()
    cmd = text.split()[0].split("@")[0].lower() if text else ""
    subs, _ = load_subs()
    entry = subs.get(chat_id, {})
    mine = entry.get("sites", [])

    if cmd in ("/start", "/settings"):
        if cmd == "/start":
            send(chat_id, HELP)
        send(chat_id, settings_text(mine), reply_markup=keyboard(mine))
    elif cmd == "/movies":
        showing = current_movies()
        movies = entry.get("movies")
        send(chat_id, movies_text(movies, showing), reply_markup=movie_keyboard(movies, showing))
    elif cmd == "/status":
        send(chat_id, status_text(entry))
    elif cmd == "/stop":
        update_subs(lambda s: s.pop(chat_id, None))
        send(chat_id, "알림을 껐어요. 다시 받으려면 /start 를 눌러 주세요.")
    else:
        send(chat_id, HELP)


def _edit(chat_id, message_id, **params):
    try:
        tg("editMessageText", chat_id=chat_id, message_id=message_id, **params)
    except TelegramError as e:
        if "not modified" not in e.desc:
            raise


def handle_callback(cb):
    chat_id = str(cb["message"]["chat"]["id"])
    message_id = cb["message"]["message_id"]
    data = cb.get("data", "")
    who = cb.get("from", {}).get("first_name", "")
    showing = None
    if data == "ma" or data.startswith("m:") or data.startswith("am:"):
        showing = current_movies()

    def new_entry():
        return {"sites": [], "name": who, "joined": now_kst().isoformat(timespec="seconds")}

    def keep(subs, entry):
        if entry["sites"] or entry.get("movies") is not None:
            subs[chat_id] = entry
        else:
            subs.pop(chat_id, None)

    def apply_sites(subs):
        entry = subs.get(chat_id) or new_entry()
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
        keep(subs, entry)
        return entry["sites"], note

    def apply_movies(subs):
        entry = subs.get(chat_id) or new_entry()
        movies = entry.get("movies")
        note = ""
        if data == "ma":
            movies, note = None, "전체 영화 알림"
        else:
            no = data.split(":", 1)[1]
            name = (showing or {}).get(no) or (movies or {}).get(no) or no
            if data.startswith("am:"):  # 알림 메시지의 '이 영화 알림 받기' 버튼
                if movies is None:
                    note = "이미 전체 영화 알림을 받고 있어요"
                else:
                    movies = {**movies, no: name}
                    note = f"{name} 알림 켬"
            elif movies is None:  # 전체 -> 이 영화만
                movies, note = {no: name}, f"{name}만 알림"
            elif no in movies:
                movies = {k: v for k, v in movies.items() if k != no}
                note = f"{name} 알림 끔"
            else:
                movies = {**movies, no: name}
                note = f"{name} 알림 켬"
        if movies is None:
            entry.pop("movies", None)
        else:
            entry["movies"] = movies
        keep(subs, entry)
        return entry.get("movies"), note

    if data in ("done", "mdone"):
        subs, _ = load_subs()
        entry = subs.get(chat_id, {})
        tg("answerCallbackQuery", callback_query_id=cb["id"])
        if not entry.get("sites"):
            text = "선택한 극장이 없어서 알림이 오지 않아요. /settings 로 다시 고를 수 있어요."
        elif data == "done":
            text = (f"설정 완료! 알림 받는 극장: {site_names(entry['sites'])}\n"
                    f"영화: {movie_names(entry.get('movies'))} (변경: /movies)\n"
                    "새 IMAX 회차가 열리면 알려드릴게요. 극장 변경: /settings")
        else:
            text = f"설정 완료!\n{status_text(entry)}"
        _edit(chat_id, message_id, text=text)
        return

    if data.startswith("am:"):
        _, (movies, note) = update_subs(apply_movies)
        tg("answerCallbackQuery", callback_query_id=cb["id"], text=note)
        # 누른 버튼만 알림 메시지에서 지움
        rows = (cb["message"].get("reply_markup") or {}).get("inline_keyboard", [])
        rows = [r for r in rows if not any(b.get("callback_data") == data for b in r)]
        try:
            tg("editMessageReplyMarkup", chat_id=chat_id, message_id=message_id,
               reply_markup={"inline_keyboard": rows})
        except TelegramError as e:
            if "not modified" not in e.desc:
                raise
        return

    if data == "ma" or data.startswith("m:"):
        _, (movies, note) = update_subs(apply_movies)
        tg("answerCallbackQuery", callback_query_id=cb["id"], text=note)
        _edit(chat_id, message_id, text=movies_text(movies, showing),
              reply_markup=movie_keyboard(movies, showing))
        return

    _, (mine, note) = update_subs(apply_sites)
    tg("answerCallbackQuery", callback_query_id=cb["id"], text=note)
    _edit(chat_id, message_id, text=settings_text(mine), reply_markup=keyboard(mine))


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
        {"command": "movies", "description": "알림 받을 영화 고르기"},
        {"command": "status", "description": "지금 설정 보기"},
        {"command": "stop", "description": "알림 끄기"},
    ])
    return {"setWebhook": r1, "setMyCommands": r2}


# ───────────────────────── CGV 조회 ─────────────────────────
API_DESC = {
    "searchSiteScnscYmdListBySite": "날짜 목록",
    "searchMovScnInfo": "상영 회차",
}


class ApiError(Exception):
    """CGV API 요청 실패. 어느 API, 어떤 파라미터, HTTP 코드, 이유를 담음"""

    def __init__(self, api, params, status, reason):
        self.api, self.params, self.status, self.reason = api, params, status, reason
        super().__init__(self.describe())

    def describe(self):
        where = " ".join(f"{k}={v}" for k, v in self.params.items() if k in ("siteNo", "scnYmd"))
        code = f"HTTP {self.status}" if self.status else "연결 실패"
        return f"{self.api}({API_DESC.get(self.api, '')}) {where} -> {code} {self.reason}".strip()

    def short(self):
        """관리자 알림용 한 줄"""
        day = self.params.get("scnYmd")
        what = API_DESC.get(self.api, self.api) + (f" {fmt_date(day)}" if day else "")
        code = f"HTTP {self.status}" if self.status else "연결 실패"
        return f"{self.api} ({what}) -> {code} {self.reason}".strip()


def get_json(path, **params):
    url = f"{BASE}/{path}?" + urllib.parse.urlencode({"coCd": CO_CD, **params})
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            raw = res.read()
    except urllib.error.HTTPError as e:
        try:
            page = e.read(4000).decode("utf-8", "ignore")
        except Exception:
            page = ""
        reason = str(e.reason or "")
        if "이용이 제한" in page:
            reason += " (CGV 차단 페이지)"
        raise ApiError(path, params, e.code, reason) from None
    except (urllib.error.URLError, OSError) as e:  # 타임아웃, DNS, 연결 끊김
        raise ApiError(path, params, None, str(getattr(e, "reason", e))) from None
    try:
        body = json.loads(raw)
    except ValueError:
        raise ApiError(path, params, 200, "JSON이 아닌 응답 (차단 페이지일 수 있음)") from None
    if body.get("statusCode") != 0:
        raise ApiError(path, params, 200,
                       f"statusCode {body.get('statusCode')}: {body.get('statusMessage')}")
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
        return {"seen": set(), "tracked": set(), "failures": 0, "imax_dates": {}, "full_at": 0,
                "known": {}, "alerted": None}
    st = {"seen": set(data.get("seen", [])), "failures": int(data.get("failures", 0)),
          "tracked": set(data.get("tracked", [])),
          "imax_dates": {k: set(v) for k, v in (data.get("imax_dates") or {}).items()},
          "full_at": float(data.get("full_at", 0)),
          "alerted": data.get("alerted"),  # 마지막으로 관리자에게 알린 오류 (정상이면 None)
          # 극장별로 IMAX 예매가 열린 적 있는 영화 {극장: {movNo: [이름, 마지막 상영일]}}
          # v3 에서 올라오면 비어 있음 -> 다음 조회에서 알림 없이 채움
          "known": {k: {m: list(v) for m, v in by.items()}
                    for k, by in (data.get("known") or {}).items()}}
    if data.get("v") == 2:  # 서울 5곳 전체를 추적하던 이전 버전
        st["tracked"] = set(SEOUL_IMAX)
    elif data.get("v") not in (3, STATE_VERSION):  # 그보다 오래된 형식은 처음부터
        st["seen"], st["tracked"] = set(), set()
    return st


def save_state(st):
    today = now_kst().strftime("%Y%m%d")
    forget = (now_kst() - timedelta(days=KNOWN_KEEP_DAYS)).strftime("%Y%m%d")
    s3_put(STATE_KEY, {
        "v": STATE_VERSION,
        "seen": sorted(k for k in st["seen"] if k[:8] >= today and key_site(k) in st["tracked"]),
        "tracked": sorted(st["tracked"]),
        "failures": st["failures"],
        "alerted": st.get("alerted"),
        "imax_dates": {k: sorted(d for d in v if d >= today)
                       for k, v in st["imax_dates"].items() if k in st["tracked"]},
        "known": {k: {m: v for m, v in by.items() if v[1] >= forget}
                  for k, by in st["known"].items() if k in st["tracked"]},
        "full_at": st["full_at"],
        "updated": now_kst().isoformat(timespec="seconds"),
    })


def build_alert(shows, firsts=()):
    """shows: [(극장, 회차)], firsts: 이번에 처음 IMAX 예매가 열린 (극장, movNo)"""
    grouped = {}
    for site_no, s in sorted(shows, key=lambda x: (list(SEOUL_IMAX).index(x[0]), x[1]["scnYmd"],
                                                   str(x[1].get("scnsrtTm")))):
        name = mov_name(s) + (f" {ICON_NEW}" if (site_no, mov_no(s)) in firsts else "")
        grouped.setdefault(site_no, {}).setdefault(s["scnYmd"], {}).setdefault(
            name, []).append(fmt_time(s.get("scnsrtTm")))
    lines = []
    for site_no, by_day in grouped.items():
        lines.append(f"[{SEOUL_IMAX[site_no]}]")
        for ymd, movies in by_day.items():
            for mv, times in movies.items():
                lines.append(f"• {fmt_date(ymd)} {mv}: {', '.join(times)}")
    names = ", ".join(SEOUL_IMAX[n] for n in grouped)
    first_names = sorted({mov_name(s) for site_no, s in shows if (site_no, mov_no(s)) in firsts})
    head = (f"{ICON_NEW} 새 영화 IMAX 예매 첫 오픈! {', '.join(first_names)}\n" if first_names else "")
    return (head + f"{ICON_OPEN} IMAX 예매 오픈! {names} 새 회차 {len(shows)}개\n"
            + "\n".join(lines[:50]) + f"\n{BOOKING_URL}\n\n변경: /settings (극장), /movies (영화)")


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


def notify_errors(st, failed, all_failed):
    """CGV 요청 실패를 관리자에게 바로 알림. 같은 오류는 정상화될 때까지 한 번만, 정상화되면 복구 알림"""
    if not failed:
        if st.get("alerted"):
            admin_notify("✅ CGV 조회가 다시 정상이에요")
            st["alerted"] = None
        return
    sig = sorted(f"{no}|{getattr(e, 'api', type(e).__name__)}|{getattr(e, 'status', '')}"
                 for no, e in failed)
    if sig == st.get("alerted"):
        return  # 이미 알린 오류
    lines = []
    for no, e in sorted(failed, key=lambda x: list(SEOUL_IMAX).index(x[0])):
        detail = e.short() if isinstance(e, ApiError) else f"{type(e).__name__}: {e}"
        lines.append(f"• {SEOUL_IMAX[no]}: {detail}"[:300])
    if all_failed:
        tail = (f"모든 극장 조회 실패 (연속 {st['failures']}회). "
                f"다음 조회는 약 {min(2 ** st['failures'], MAX_BACKOFF_MIN)}분 뒤예요.")
    else:
        tail = "나머지 극장은 정상 조회됐어요."
    if any(getattr(e, "status", None) == 429 for _, e in failed):
        tail += "\n429 = CGV가 요청이 너무 많다고 거절한 거예요."
    admin_notify(f"{ICON_WARN} CGV 조회 실패 {now_kst().strftime('%H:%M')}\n" + "\n".join(lines)
                 + f"\n{tail}\n로그: CloudWatch /aws/lambda/cgv-test 에서 \"API 오류\" 검색")
    st["alerted"] = sig


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
    # 빠른 조회일 때만 이미 IMAX가 있는 날짜를 건너뜀 (새로 추적하는 극장은 항상 전체 조회)
    skips = {no: (st["imax_dates"].get(no, set()) if (not full and no in st["tracked"]) else set())
             for no in needed}
    # 극장들은 동시에 조회 (극장 안에서는 날짜를 순서대로, 요청 사이 간격 유지)
    with ThreadPoolExecutor(max_workers=len(needed)) as pool:
        futures = {no: pool.submit(scan_site, no, today, movie_kw, skips[no]) for no in needed}
    found, ok_sites, errors, failed = {}, set(), [], []
    for no in needed:
        try:
            f, open_dates, imax_dates = futures[no].result()
            found.update(f)
            prev = st["imax_dates"].get(no, set())
            st["imax_dates"][no] = (imax_dates if not skips[no] else (prev & open_dates) | imax_dates)
            ok_sites.add(no)
        except Exception as e:
            errors.append(f"{SEOUL_IMAX[no]}: {e}")
            failed.append((no, e))
            print(f"[API 오류] {SEOUL_IMAX[no]} | {e}")

    if not ok_sites:
        st["failures"] += 1
        notify_errors(st, failed, all_failed=True)
        st["tracked"] &= set(needed)
        save_state(st)
        return f"fail {st['failures']}: {'; '.join(errors)}"

    st["failures"] = 0
    notify_errors(st, failed, all_failed=False)

    # 이전부터 추적하던 극장의 새 회차만 알림. 새로 구독자가 생긴 극장은 조용히 기준선만 저장
    new = {k: s for k, s in found.items() if k not in st["seen"] and key_site(k) in st["tracked"]}
    newly_tracked = ok_sites - st["tracked"]

    # 극장별로 처음 IMAX 예매가 열린 영화. 영화 목록을 처음 만드는 극장(신규 추적, 업그레이드 직후)은 제외
    firsts = set()
    for k, s in found.items():
        site_no, no = key_site(k), mov_no(s)
        by_mov = st["known"].get(site_no)
        if by_mov is not None and no not in by_mov and site_no in st["tracked"]:
            firsts.add((site_no, no))
    for no_site in ok_sites:
        st["known"].setdefault(no_site, {})
    for k, s in found.items():
        entry = st["known"][key_site(k)].setdefault(mov_no(s), [mov_name(s), s["scnYmd"]])
        entry[0] = mov_name(s)
        entry[1] = max(entry[1], s["scnYmd"])

    sent = 0
    blocked = []
    if new:
        for chat_id, u in subs.items():
            movies = u.get("movies")  # None = 전체 영화
            mine = [(key_site(k), s) for k, s in new.items()
                    if key_site(k) in u.get("sites", [])
                    and (movies is None or mov_no(s) in movies or (key_site(k), mov_no(s)) in firsts)]
            if not mine:
                continue
            extra = {}
            if movies is not None:  # 고르지 않은 첫 오픈 영화는 바로 추가할 수 있게 버튼
                add = {mov_no(s): mov_name(s) for site_no, s in mine if mov_no(s) not in movies}
                if add:
                    extra["reply_markup"] = {"inline_keyboard": [
                        [{"text": f"➕ {name} 알림 받기"[:60], "callback_data": f"am:{no}"[:64]}]
                        for no, name in sorted(add.items(), key=lambda x: x[1])[:8]]}
            try:
                send(chat_id, build_alert(mine, firsts), **extra)
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
              f"first {sorted(no for _, no in firsts)}, "
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
