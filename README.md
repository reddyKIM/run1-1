# ARB Aggregator v3 — GitHub Actions Automation Scaffold

본 리포는 `arb_aggregator_v3.py`를 **GitHub Actions**에서 5분마다 자동 실행해
9개 CSV + 1개 JSON(설정)을 생성합니다. 결과는 `data/` 폴더에 저장되고,
**Workflow Artifacts**로도 업로드됩니다.

## 파일 구성
- `arb_aggregator_v3.py` — 주니가 제공한 집계기 (리포 루트에 업로드하세요)
- `requirements.txt` — 의존성
- `.github/workflows/arb_aggregator.yml` — 5분 주기 실행 워크플로
- `.env.example` — (선택) 텔레그램/디스코드 등 Webhook 사용시 참고
- `data/` — 결과물이 저장되는 폴더

## 빠른 시작 (요약)
1. 이 저장소를 GitHub에 생성 후, 아래 파일들을 그대로 업로드합니다.
2. 리포 루트에 **주니의 `arb_aggregator_v3.py` 원본**을 업로드합니다.
3. (선택) 리포 **Settings → Secrets and variables → Actions → New repository secret**
   - 예: `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID` 등
4. Actions 탭에서 워크플로가 동작하는지 확인합니다.

## 수동 실행 방법
- GitHub → Actions → **ARB Aggregator** → **Run workflow** → 즉시 수동 실행 가능

## 결과 확인
- `data/` 폴더 내 CSV/JSON (자동 커밋 설정 시)
- 또는 **Actions → 해당 실행(job) → Artifacts** 다운로드

---
⚠️ **주의**: 본 리포/워크플로는 연구용 참고이며, 투자 조언이 아닙니다.
모든 API 키/시크릿은 반드시 GitHub Secrets에 저장하세요.
(로컬/리포 파일에 직접 넣지 마세요)