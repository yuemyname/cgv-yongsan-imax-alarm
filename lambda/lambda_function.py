"""
CGV 용산아이파크몰 IMAX 예매 오픈 알리미 - AWS Lambda (서울 리전) 버전

EventBridge 가 5분마다 실행하고, 이미 알린 회차 목록은 S3 에 저장합니다.
조회만 합니다 (로그인/예매/결제 없음).

환경변수
  STATE_BUCKET        상태 파일을 저장할 S3 버킷 이름 (필수)
  TELEGRAM_BOT_TOKEN  텔레그램 봇 토큰 (없으면 로그에만 출력)
  TELEGRAM_CHAT_ID    텔레그램 chat id
  MOVIE_KEYWORD       특정 영화만 받으려면 제목 일부 (예: 듄). 비우면 전체

테스트 이벤트
  {"test_notify": true}  → 텔레그램 알림만 테스트
"""

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

SITE_NO = "0013"  # CGV 용산아이파크몰
CO_CD = "A420"
BASE = "https://cgv.co.kr/api/v1/booking"
BOOKING_URL = "https://cgv.co.kr/cnm/movieBook/cinema"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (personal open-alert script)",
    "Accept": "application/json",
    "Referer": BOOKING_URL,
}
REQUEST_GAP_SEC = 1.0
FAIL_ALERT_AT = 6  # 5분 간격이면 약 30분 연속 실패 시 한 번 경고
STATE_KEY = "cgv-imax-state.json"
KST = timezone(timedelta(hours=9))

s3 = boto3.client("s3")


def now_kst():
    return datetime.now(KST)


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


def show_key(s):
    return f"{s['scnYmd']}|{s['scnsNo']}|{s['scnSseq']}|{s.get('scnsrtTm')}"


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
        print("텔레그램 미설정 → 로그에만 출력")
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
        return {"seen": set(st.get("seen", [])), "failures": int(st.get("failures", 0))}, True
    except s3.exceptions.NoSuchKey:
        return {"seen": set(), "failures": 0}, False


def save_state(bucket, st):
    today = now_kst().strftime("%Y%m%d")
    body = json.dumps({"seen": sorted(k for k in st["seen"] if k[:8] >= today),
                       "failures": st["failures"],
                       "updated": now_kst().isoformat(timespec="seconds")},
                      ensure_ascii=False, indent=1)
    s3.put_object(Bucket=bucket, Key=STATE_KEY, Body=body.encode("utf-8"),
                  ContentType="application/json")


def check(st, has_baseline, movie_kw):
    today = now_kst().strftime("%Y%m%d")
    dates = [d["scnYmd"] for d in get_json("searchSiteScnscYmdListBySite", siteNo=SITE_NO)
             if d["scnYmd"] >= today]
    found = {}
    for ymd in dates:
        time.sleep(REQUEST_GAP_SEC)
        for s in get_json("searchMovScnInfo", siteNo=SITE_NO, scnYmd=ymd, rtctlScopCd="08"):
            if is_target(s, movie_kw):
                found[show_key(s)] = s

    new = sorted((found[k] for k in found if k not in st["seen"]),
                 key=lambda s: (s["scnYmd"], str(s.get("scnsrtTm"))))
    if not has_baseline:
        open_days = sorted({s["scnYmd"] for s in found.values()})
        notify("✅ CGV 용산 IMAX 알리미가 시작됐어요",
               [f"현재 열린 IMAX 날짜: {', '.join(fmt_date(d) for d in open_days) or '없음'}",
                "이제부터 새로 열리는 회차만 알려드릴게요."])
        result = f"baseline {len(found)}"
    elif new:
        lines, by_day = [], {}
        for s in new:
            by_day.setdefault(s["scnYmd"], {}).setdefault(
                s.get("movNm") or s.get("prodNm"), []).append(fmt_time(s.get("scnsrtTm")))
        for ymd, movies in by_day.items():
            for mv, times in movies.items():
                lines.append(f"• {fmt_date(ymd)} {mv}: {', '.join(times)}")
        lines.append(BOOKING_URL)
        notify(f"🎬 용산 IMAX 예매 오픈! 새 회차 {len(new)}개", lines[:40])
        result = f"new {len(new)}"
    else:
        result = f"no change (dates {len(dates)}, imax {len(found)})"
    st["seen"] |= set(found)
    return result


def lambda_handler(event, context):
    event = event or {}
    if event.get("test_notify"):
        notify("🔔 CGV 알리미 테스트", ["알림이 정상적으로 도착했어요."])
        return {"result": "test notify sent"}

    bucket = os.environ["STATE_BUCKET"]
    st, has_baseline = load_state(bucket)
    try:
        result = check(st, has_baseline, os.getenv("MOVIE_KEYWORD", "").strip())
        if st["failures"] >= FAIL_ALERT_AT:
            notify("✅ CGV 알리미 조회가 다시 정상화됐어요", [])
        st["failures"] = 0
    except Exception as e:
        st["failures"] += 1
        result = f"fail {st['failures']}: {e}"
        if st["failures"] == FAIL_ALERT_AT:
            notify("⚠️ CGV 알리미 조회가 계속 실패하고 있어요",
                   [str(e)[:300], "CGV API가 바뀌었거나 접속이 막혔을 수 있어요."])
    save_state(bucket, st)
    print(result)
    return {"result": result}
