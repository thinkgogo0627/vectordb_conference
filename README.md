# wiki-collector

위키피디아 실시간 편집 스트림(Wikimedia EventStreams `recentchange`)을 24시간 받아서,
본문까지 붙여 **Parquet 파일로 클라우드 저장소(R2/S3)에 쌓는** 수집기.
실험 파이프라인(Postgres → CDC → 벡터DB)은 여기 없음. 실험은 각자 이 레이크에서 내려받아 리플레이한다.

```
EventStreams(SSE) ──> 필터(위키·문서 네임스페이스·편집/생성/삭제)
                  ──> rev_id 50개씩 묶어 Action API 조회 (본문·sha1·태그)
                  ──> 5분마다 Parquet(zstd) ──> 로컬 outbox ──> R2 업로드
```

## 보장하는 것

| 상황 | 동작 |
|---|---|
| 서버가 연결을 끊음 (15분마다) | 마지막 이벤트 ID로 자동 재연결, 빈 구간 없음 |
| 컨테이너/VM 재시작 | 디스크에 저장된 재개 위치부터 다시 받음 (스트림 보관 기간 안이면) |
| R2 업로드 실패·네트워크 끊김 | 파일은 outbox에 남고, 성공할 때까지 30초마다 재시도 |
| 본문 API 실패 | 행은 저장하고 `content_status='failed'`로 표시 → 나중에 백필 |

**at-least-once**라서 재시작 직후엔 같은 이벤트가 두 번 들어갈 수 있음. 읽을 때 `event_id`로 중복 제거.

## 레이크 구조

```
s3://wiki-lake/raw/wiki_recentchange/source=kowiki/date=2026-11-08/hour=13/part-....parquet
```
날짜·시간은 **이벤트 시각(UTC)** 기준이라, 리플레이할 때 기간으로 바로 자를 수 있음.

주요 컬럼

| 컬럼 | 뜻 |
|---|---|
| `event_id` | 이벤트 고유 ID (중복 제거 키) |
| `event_dt` | 이벤트 시각 (UTC) |
| `op` | `create` / `update` / `delete` / `log_restore` 등 |
| `page_title`, `page_id` | 문서 |
| `rev_id`, `parent_rev_id` | 이번 버전 / 직전 버전 → 순서 꼬임 실험(W2)의 기준 |
| `sha1` | 본문 해시. 예전 버전과 같으면 되돌림(W4) |
| `tags` | `mw-rollback`, `mw-undo`, `mw-manual-revert` 등 되돌림 태그 |
| `content` | 본문(wikitext) |
| `content_status` | `ok` / `failed` / `missing`(삭제·숨김) / `none`(삭제 이벤트) |
| `raw_event` | 원본 이벤트 JSON 전체 |

## 1. 로컬에서 먼저 돌려보기 (MinIO)

```bash
cp .env.example .env
# .env 에서 S3 값 4줄을 'minio' 주석 블록 값으로 바꾸고 CONTACT 채우기
docker compose --profile local up --build
# 브라우저 http://localhost:9001 (minioadmin/minioadmin) → wiki-lake 버킷에 5분 뒤 파일 생김
```

## 2. R2 버킷 만들기 (Cloudflare)

1. Cloudflare 대시보드 → R2 → 버킷 `wiki-lake` 생성
2. R2 → API 토큰 관리 → 이 버킷에 **Object Read & Write** 권한 토큰 발급
3. 나온 Access Key / Secret / 계정 ID를 `.env`의 `S3_*`에 입력
4. 조원용으로는 **읽기 전용** 토큰을 따로 발급해서 공유

## 3. 24시간 VM에 올리기

무료 티어 VM(Oracle Cloud Always Free, GCP e2-micro 등) 하나면 충분함.

```bash
# VM 접속 후
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git
sudo usermod -aG docker $USER && newgrp docker
git clone <이 레포> && cd wiki-collector
cp .env.example .env && vi .env      # R2 키, CONTACT 입력
docker compose up -d --build         # collector만 뜸 (minio는 local 프로필이라 안 뜸)
docker compose logs -f collector     # 'flush: rows=...' 가 5분마다 찍히면 정상
```

상태 확인
```bash
docker compose ps                    # STATUS가 healthy 여야 함 (15분 넘게 flush 없으면 unhealthy)
```

## 4. 조원이 데이터 받기 (예: DuckDB)

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET r2 (TYPE R2, KEY_ID '<읽기전용키>', SECRET '<시크릿>', ACCOUNT_ID '<계정ID>');

SELECT op, count(*) FROM read_parquet('r2://wiki-lake/raw/wiki_recentchange/source=kowiki/date=2026-11-*/*/*.parquet')
GROUP BY op;

-- 중복 제거
SELECT * FROM read_parquet('...') QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingested_at) = 1;
```

## 설정 (.env)

| 변수 | 기본값 | 설명 |
|---|---|---|
| `WIKIS` | `kowiki` | 수집할 위키. 쉼표로 여러 개 |
| `NAMESPACES` | `0` | 0 = 일반 문서 |
| `FETCH_CONTENT` | `true` | 본문까지 받을지. false면 메타만 |
| `FLUSH_SECONDS` / `FLUSH_ROWS` | `300` / `5000` | Parquet 파일 쓰는 주기 |
| `CONTACT` | 없음 | **필수.** Wikimedia API 정책상 User-Agent에 연락처 |

## 테스트

```bash
pip install -r requirements.txt pytest "moto[s3]"
python -m pytest -q tests
```
네트워크 없이 SSE 파싱 → 필터 → 보강 → Parquet → 업로드 → 재개 위치까지 검증함.

## 알아둘 것

- 첫날 돌려보고 **하루 용량**을 꼭 확인. 본문 포함이라 R2 무료 용량(10GB)이 몇 주 버티는지 계산해야 함. 빠듯하면 `FETCH_CONTENT=false`로 메타만 받고 실험 때 필요한 리비전만 따로 받는 방식으로 전환
- `content_status='failed'` 행은 API가 일시적으로 실패한 것. 백필 스크립트는 아직 없음
- EventStreams 보관 기간(대략 1~4주)보다 오래 꺼져 있으면 그 구간은 못 되살림
