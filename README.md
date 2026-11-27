# CGV 용산아이파크몰 IMAX 예매 오픈 알리미

GitHub Actions가 5분마다 CGV 공개 시간표를 조회해서, 용산아이파크몰 IMAX관에 새 회차가 열리면 텔레그램으로 알려줍니다. 조회만 하고 예매는 하지 않습니다.

## 설정

1. **텔레그램 봇 만들기**
   - 텔레그램에서 `@BotFather` → `/newbot` → 봇 토큰 복사
   - 만든 봇에게 아무 메시지나 보낸 뒤 `https://api.telegram.org/bot<토큰>/getUpdates` 를 열어 `"chat":{"id": ...}` 숫자 복사
2. **GitHub에 Public 저장소를 만들고** 이 폴더 내용을 그대로 올리기 (`.github/workflows/` 포함)
3. 저장소 **Settings → Secrets and variables → Actions → New repository secret**
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
4. **Actions 탭 → "CGV 용산 IMAX 오픈 알림" → Run workflow** 에서 `텔레그램 알림만 테스트` 를 체크하고 실행
   - "🔔 CGV 알리미 테스트" 메시지가 오면 성공. 이후 5분마다 자동 실행됩니다.

## 참고

- 특정 영화만 받으려면 `cgv_open_alert.py` 의 `MOVIE_KEYWORD = "듄"` 처럼 수정
- 30분 정도 조회가 계속 실패하면 경고 알림이 한 번 옵니다 (CGV API 변경 또는 차단 가능성)
- 끄려면 Actions 탭에서 워크플로 → `···` → Disable workflow
- Public 저장소는 60일 동안 커밋이 없으면 GitHub이 예약 실행을 자동으로 멈춥니다. 메일이 오면 다시 Enable 하면 됩니다.

## AWS Lambda (서울 리전) 버전 — 현재 운영 중

CGV는 해외 IP(GitHub Actions)를 차단해서, 실제 운영은 `lambda/lambda_function.py` 를 AWS Lambda 서울 리전에서 돌립니다.

### 구독자용 사용법 (텔레그램 봇)
- `/start` — 안내 + 극장 선택 버튼
- `/settings` — 알림 받을 극장 켜고 끄기 (용산아이파크몰, 압구정, 영등포타임스퀘어, 왕십리, 천호)
- `/status` — 구독 중인 극장 보기
- `/stop` — 알림 끄기

### 구성
- 함수: `cgv-test` (ap-northeast-2), Python 3.13, 제한 시간 3분
- 1분 조회: EventBridge 규칙 `cgv-imax-alert-every-5min`(이름만 5min, 실제 `rate(1 minute)`) — 구독자가 있는 극장만 조회, 구독자별로 자기 극장 알림만 발송
  - 매분 빠른 조회: 새 날짜 + 아직 IMAX가 없는 날짜만 확인 (몇 초)
  - 약 5분마다 전체 조회: 이미 IMAX가 있는 날짜에 추가된 회차까지 확인
  - 실행이 겹치면 건너뜀(`scan.lock`), 연속 실패 시 2·4·8…최대 30분 간격으로 자동 감속
- 봇 명령: Lambda Function URL(인증 NONE) → 텔레그램 웹훅. 텔레그램 비밀 토큰 헤더로 요청 검증(토큰에서 파생, 별도 설정 불필요)
- S3 버킷 `cgv-imax-alert-state-497597443603`: `cgv-imax-state.json`(본 회차/추적 극장/IMAX 있는 날짜), `scan.lock`(겹침 방지), `subscribers.json`(구독자별 극장)
- 새로 구독자가 생긴 극장은 기존 회차로 알림을 보내지 않고 기준선만 잡음
- 봇을 차단한 구독자는 자동 삭제, 조회 실패 경고는 관리자(`TELEGRAM_CHAT_ID`)에게만
- 환경변수: `STATE_BUCKET`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `MOVIE_KEYWORD`(선택)

### 관리자용 테스트 이벤트
- `{"test_notify": true}` — 관리자에게 테스트 알림
- `{"setup_webhook": "<Function URL>"}` — 웹훅/명령 메뉴 (재)등록. 봇 토큰을 바꾸면 다시 실행

### 끄기
- 조회 중지: Lambda → 구성 → 트리거에서 EventBridge 규칙 비활성화
- 봇 중지: Function URL 삭제

GitHub Actions 워크플로는 비활성화되어 있습니다.
