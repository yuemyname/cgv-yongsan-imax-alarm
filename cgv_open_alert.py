#!/usr/bin/env python3
"""
CGV 용산아이파크몰 IMAX 예매 오픈 알리미

CGV 웹사이트가 쓰는 공개 시간표 API를 조회해서, 용산아이파크몰 IMAX관에
새 상영 회차가 생기면 텔레그램으로 알려줍니다.
- 조회만 합니다. 로그인, 예매, 결제는 하지 않습니다.
- 표준 라이브러리만 사용 (Python 3.8+).

GitHub Actions 에서는 --once 로 한 번 조회하고 종료하며,
이전 결과는 --state-file 에 저장해 다음 실행과 비교합니다.

로컬 실행:
  python cgv_open_alert.py --once          # 한 번 조회
  python cgv_open_alert.py                 # 5분 간격으로 계속 감시
  python cgv_open_alert.py --test-notify   # 텔레그램 알림만 테스트

환경변수:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  (없으면 콘솔 출력만)
"""

import argparse
import json
import os
import random
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ───────────── 감시 대상 (용산아이파크몰 IMAX 고정) ─────────────
SITE_NO = "0013"              # CGV 용산아이파크몰
SITE_NM = "용산아이파크몰"
HALL_KEYWORD = "imax"         # 관 이름/등급/포맷 중 하나에 포함되면 IMAX로 판단
MOVIE_KEYWORD = ""            # 특정 영화만 원하면 예: "듄" (비우면 모든 영화)
# ──────────────────────────────────────────────────────────────

BASE = "https://cgv.co.kr/api/v1/booking"
CO_CD = "A420"
KST = timezone(timedelta(hours=9))
HEADERS = {
    "User-Agent": "Mozilla/5.0 (personal open-alert script)",
    "Accept": "application/json",
    "Referer": "https://cgv.co.kr/cnm/movieBook/cinema",
}
REQUEST_GAP_SEC = 1.5         # 한 번 조회할 때 요청 사이 간격
FAIL_ALERT_AT = 6             # 연속 실패가 이 횟수가 되면 한 번 경고 (5분 간격이면 약 30분)
BOOKING_URL = "https://cgv.co.kr/cnm/movieBook/cinema"


def now_kst() -> datetime:
    return datetime.now(KST)


def log(msg: str) -> None:
    print(f"[{now_kst():%m-%d %H:%M:%S}] {msg}", flush=True)


def get_json(path: str, **params) -> list:
    url = f"{BASE}/{path}?" + urllib.parse.urlencode({"coCd": CO_CD, **params})
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as res:
        body = json.load(res)
    if body.get("statusCode") != 0:
        raise RuntimeError(f"{path}: {body.get('statusMessage')}")
    return body.get("data") or []


def open_dates() -> list:
    return [d["scnYmd"] for d in get_json("searchSiteScnscYmdListBySite", siteNo=SITE_NO)]


def schedules(ymd: str) -> list:
    return get_json("searchMovScnInfo", siteNo=SITE_NO, scnYmd=ymd, rtctlScopCd="08")


def is_target(s: dict) -> bool:
    hall = " ".join(str(s.get(k) or "") for k in
                    ("scnsNm", "expoScnsNm", "tcscnsGradNm", "movkndDsplNm")).lower()
    movie = " ".join(str(s.get(k) or "") for k in ("movNm", "prodNm", "expoProdNm")).lower()
    hall_ok = HALL_KEYWORD in hall or "아이맥스" in hall
    return hall_ok and (not MOVIE_KEYWORD or MOVIE_KEYWORD.lower() in movie)


def show_key(s: dict) -> str:
    return f"{s['scnYmd']}|{s['scnsNo']}|{s['scnSseq']}|{s.get('scnsrtTm')}"


def fmt_time(t) -> str:
    t = str(t or "")
    return f"{t[:2]}:{t[2:4]}" if len(t) >= 4 else t


def fmt_date(ymd: str) -> str:
    d = datetime.strptime(ymd, "%Y%m%d")
    return f"{d.month}/{d.day}({'월화수목금토일'[d.weekday()]})"


def notify(title: str, lines: list) -> None:
    text = title + "\n" + "\n".join(lines)
    print("=" * 50 + "\n" + text + "\n" + "=" * 50, flush=True)
    token, chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not (token and chat):
        log("텔레그램 미설정 → 콘솔 출력만 했어요")
        return
    data = urllib.parse.urlencode({"chat_id": chat, "text": text[:4000],
                                   "disable_web_page_preview": "true"}).encode()
    try:
        urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=15)
    except Exception as e:
        log(f"텔레그램 전송 실패: {e}")


# ───────────── 상태 파일 ─────────────
def load_state(path: Path):
    try:
        st = json.loads(path.read_text(encoding="utf-8"))
        return {"seen": set(st.get("seen", [])), "failures": int(st.get("failures", 0))}, True
    except (FileNotFoundError, ValueError):
        return {"seen": set(), "failures": 0}, False


def save_state(path: Path, st: dict) -> None:
    today = now_kst().strftime("%Y%m%d")
    seen = sorted(k for k in st["seen"] if k[:8] >= today)  # 지난 날짜는 정리
    path.write_text(json.dumps({"seen": seen, "failures": st["failures"],
                                "updated": now_kst().isoformat(timespec="seconds")},
                               ensure_ascii=False, indent=1), encoding="utf-8")


# ───────────── 1회 조회 ─────────────
def check_once(st: dict, has_baseline: bool) -> None:
    found = {}
    today = now_kst().strftime("%Y%m%d")
    dates = [d for d in open_dates() if d >= today]  # 지난 날짜는 무시 (상태 정리 기준과 일치)
    for ymd in dates:
        time.sleep(REQUEST_GAP_SEC)
        for s in schedules(ymd):
            if is_target(s):
                found[show_key(s)] = s

    new = sorted((found[k] for k in found if k not in st["seen"]),
                 key=lambda s: (s["scnYmd"], str(s.get("scnsrtTm"))))
    if not has_baseline:
        log(f"첫 실행: 기존 IMAX 회차 {len(found)}개를 기준으로 저장했어요 (이후 새 회차만 알림)")
        open_days = sorted({s["scnYmd"] for s in found.values()})
        notify("✅ CGV 용산 IMAX 알리미가 시작됐어요",
               [f"현재 열린 IMAX 날짜: {', '.join(fmt_date(d) for d in open_days) or '없음'}",
                "이제부터 새로 열리는 회차만 알려드릴게요."])
    elif new:
        by_day = {}
        for s in new:
            by_day.setdefault(s["scnYmd"], []).append(s)
        lines = []
        for ymd, items in by_day.items():
            movies = {}
            for s in items:
                movies.setdefault(s.get("movNm") or s.get("prodNm"), []).append(fmt_time(s.get("scnsrtTm")))
            for mv, times in movies.items():
                lines.append(f"• {fmt_date(ymd)} {mv}: {', '.join(times)}")
        lines.append(BOOKING_URL)
        notify(f"🎬 용산 IMAX 예매 오픈! 새 회차 {len(new)}개", lines[:40])
    else:
        log(f"변동 없음 (열린 날짜 {len(dates)}일, IMAX 회차 {len(found)}개)")

    st["seen"] |= set(found)


def run_once(state_path: Path) -> None:
    st, has_baseline = load_state(state_path)
    try:
        check_once(st, has_baseline)
        if st["failures"] >= FAIL_ALERT_AT:
            notify("✅ CGV 알리미 조회가 다시 정상화됐어요", [])
        st["failures"] = 0
    except Exception as e:
        st["failures"] += 1
        log(f"조회 실패 ({st['failures']}회 연속): {e}")
        if st["failures"] == FAIL_ALERT_AT:  # 딱 한 번만 경고
            notify("⚠️ CGV 알리미 조회가 계속 실패하고 있어요",
                   [str(e)[:300], "CGV API가 바뀌었거나 접속이 막혔을 수 있어요."])
    save_state(state_path, st)


def main():
    p = argparse.ArgumentParser(description="CGV 용산아이파크몰 IMAX 예매 오픈 알리미")
    p.add_argument("--once", action="store_true", help="한 번만 조회하고 종료 (GitHub Actions용)")
    p.add_argument("--state-file", default=str(Path(__file__).resolve().parent / ".cgv_state.json"))
    p.add_argument("--interval", type=int, default=300, help="반복 모드 조회 간격(초), 최소 120")
    p.add_argument("--test-notify", action="store_true", help="알림 채널만 테스트")
    a = p.parse_args()

    if a.test_notify:
        notify("🔔 CGV 알리미 테스트", ["알림이 정상적으로 도착했어요."])
        return

    state_path = Path(a.state_file)
    log(f"감시 대상: CGV {SITE_NM} IMAX / 영화='{MOVIE_KEYWORD or '전체'}'")
    while True:
        run_once(state_path)
        if a.once:
            break
        time.sleep(max(120, a.interval) + random.randint(0, 30))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
