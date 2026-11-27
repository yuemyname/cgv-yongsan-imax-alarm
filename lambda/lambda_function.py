"""
CGV 서울 IMAX 예매 오픈 알리미 - AWS Lambda (서울 리전) 버전

EventBridge 가 5분마다 실행하고, 이미 알린 회차 목록은 S3 에 저장합니다.
조회만 합니다 (로그인/예매/결제 없음).

환경변수
  STATE_BUCKET        상태 파일을 저장할 S3 버킷 이름 (필수)
  TELEGRAM_BOT_TOKEN  텔레그램 봇 토큰 (없으면 로그에만 출력)
  TELEGRAM_CHAT_ID    텔레그램 chat id
  SITE_NOS            감시할 극장 번호 (쉼표 구분). 비우면 서울 IMAX 5곳 전체
  MOVIE_KEYWORD       특정 영화만 받으려면 제목 일부 (예: 듄). 비우면 전체

테스트 이벤트
  {"test_notify": true}  -> 텔레그램 알림만 테스트
"""

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

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
REQUEST_GAP_SEC = 0.5
FAIL_ALERT_AT = 6  # 5분 간격이면 약 30분 연속 실패 시 한 번 경고
STATE_KEY = "cgv-imax-state.json"
STATE_VERSION = 2  # v2: 키에 극장 번호 포함
KST = timezone(timedelta(hours=9))

ICON_START = "✅"          # check mark
ICON_OPEN = "\U0001F3AC"       # clapper board
ICON_TEST = "\U0001F514"       # bell
ICON_WARN = "⚠️"     # warning

s3 = boto3.client("s3")


def now_kst():
    return datetime.now(KST)


def target_sites():
    raw = os.getenv("SITE_NOS", "").strip()
    if not raw:
        return dict(SEOUL_IMAX)
    return {no.strip(): SEOUL_IMAX.get(no.strip(), no.strip())
            for no in raw.split(",") if no.strip()}


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


def fmt_time(t):
    t = str(t or "")
    return f"{t[:2]}:{t[2:4]}" if len(t) >= 4 else t


def fmt_date(ymd):
    d = datetime.strptime(ymd, "%Y%m%d")
    return f"{d.month}/{d.day}({'월화수목금토일'[d.weekday()]})"


def notify(title, lines):
    text = title + ("\n" + "\n".join(lines) if lines else "")
    print(text)
    token, chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not (token and chat):
        print("텔레그램 미설정 -> 로그에만 출력")
        return
    data = urllib.parse.urlencode({"chat_id": chat, "text": text[:4000],
                                   "disable_web_page_preview": "true"}).encode()
    try:
        urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=15)
    except Exception as e:
        print(f"텔레그램 전송 실패: {e}")


def load_state(bucket):
    try:
        obj = s3.get_object(Bucket=bucket, Key=STATE_KEY)
        st = json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {"seen": set(), "failures": 0}, False
    failures = int(st.get("failures", 0))
    if st.get("v") != STATE_VERSION:  # 예전 형식이면 기준선을 새로 잡음
        return {"seen": set(), "failures": failures}, False
    return {"seen": set(st.get("seen", [])), "failures": failures}, True


def save_state(bucket, st):
    today = now_kst().strftime("%Y%m%d")
    body = json.dumps({"v": STATE_VERSION,
                       "seen": sorted(k for k in st["seen"] if k[:8] >= today),
                       "failures": st["failures"],
                       "updated": now_kst().isoformat(timespec="seconds")},
                      ensure_ascii=False, indent=1)
    s3.put_object(Bucket=bucket, Key=STATE_KEY, Body=body.encode("utf-8"),
                  ContentType="application/json")


def scan_site(site_no, today, movie_kw):
    found = {}
    dates = [d["scnYmd"] for d in get_json("searchSiteScnscYmdListBySite", siteNo=site_no)
             if d["scnYmd"] >= today]
    for ymd in dates:
        time.sleep(REQUEST_GAP_SEC)
        for s in get_json("searchMovScnInfo", siteNo=site_no, scnYmd=ymd, rtctlScopCd="08"):
            if is_target(s, movie_kw):
                found[show_key(site_no, s)] = (site_no, s)
    return found


def check(st, has_baseline, sites, movie_kw):
    today = now_kst().strftime("%Y%m%d")
    found, errors = {}, []
    for site_no, name in sites.items():
        try:
            found.update(scan_site(site_no, today, movie_kw))
        except Exception as e:
            errors.append(f"{name}: {e}")
            print(f"[{name}] 조회 실패: {e}")
    if errors and len(errors) == len(sites):
        raise RuntimeError("; ".join(errors))

    if not has_baseline:
        lines = []
        for site_no, name in sites.items():
            days = sorted({s["scnYmd"] for k, (no, s) in found.items() if no == site_no})
            lines.append(f"• {name}: {', '.join(fmt_date(d) for d in days) or '열린 회차 없음'}")
        lines.append("이제부터 새로 열리는 IMAX 회차만 알려드릴게요.")
        notify(f"{ICON_START} CGV 서울 IMAX 알리미가 시작됐어요 ({len(sites)}곳)", lines)
        result = f"baseline {len(found)}"
    else:
        new = [found[k] for k in found if k not in st["seen"]]
        if new:
            grouped = {}
            for site_no, s in sorted(new, key=lambda x: (x[0], x[1]["scnYmd"], str(x[1].get("scnsrtTm")))):
                grouped.setdefault(site_no, {}).setdefault(s["scnYmd"], {}).setdefault(
                    s.get("movNm") or s.get("prodNm"), []).append(fmt_time(s.get("scnsrtTm")))
            lines = []
            for site_no, by_day in grouped.items():
                lines.append(f"[{sites.get(site_no, site_no)}]")
                for ymd, movies in by_day.items():
                    for mv, times in movies.items():
                        lines.append(f"• {fmt_date(ymd)} {mv}: {', '.join(times)}")
            lines.append(BOOKING_URL)
            names = ", ".join(sites.get(no, no) for no in grouped)
            notify(f"{ICON_OPEN} IMAX 예매 오픈! {names} 새 회차 {len(new)}개", lines[:50])
            result = f"new {len(new)}"
        else:
            result = f"no change (imax {len(found)})"
    if errors:
        result += f" / partial errors: {len(errors)}"
    st["seen"] |= set(found)
    return result


def lambda_handler(event, context):
    event = event or {}
    if event.get("test_notify"):
        notify(f"{ICON_TEST} CGV 알리미 테스트", ["알림이 정상적으로 도착했어요."])
        return {"result": "test notify sent"}

    bucket = os.environ["STATE_BUCKET"]
    sites = target_sites()
    st, has_baseline = load_state(bucket)
    try:
        result = check(st, has_baseline, sites, os.getenv("MOVIE_KEYWORD", "").strip())
        if st["failures"] >= FAIL_ALERT_AT:
            notify(f"{ICON_START} CGV 알리미 조회가 다시 정상화됐어요", [])
        st["failures"] = 0
    except Exception as e:
        st["failures"] += 1
        result = f"fail {st['failures']}: {e}"
        if st["failures"] == FAIL_ALERT_AT:
            notify(f"{ICON_WARN} CGV 알리미 조회가 계속 실패하고 있어요",
                   [str(e)[:300], "CGV API가 바뀌었거나 접속이 막혔을 수 있어요."])
    save_state(bucket, st)
    print(result)
    return {"result": result}
