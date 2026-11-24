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

---

# 실험 파이프라인 (`pipeline/`) — 실험 0: 간극 존재 증명

설계 기준은 `docs/purpose/`. 수집기와 별개로, 부분집합을 Postgres에 리플레이하고 Debezium → Kafka → 워커 → FAISS로
반영하면서 편집 후 경과 시간별 stale 비율을 잰다.

```
subset.parquet ──(replayer, 원래 간격×배속)──> Postgres ──Debezium──> Kafka ──> worker(embed_ms 대기) ──> FAISS(Flat)
                                                  │                                                        ▲
                                                  └────────────── evaluate (Postgres 최신 vs FAISS top-1) ─┘
```

## 준비 (한 번)

```bash
python -m venv .venv
.venv/Scripts/pip install torch --index-url https://download.pytorch.org/whl/cpu
.venv/Scripts/pip install -r requirements-exp.txt
docker compose -f docker-compose.exp.yml up -d          # Postgres + Kafka + Debezium
.venv/Scripts/python -m pipeline.register_connector
```

## 데이터 (T-01, T-02)

```bash
# R2에서 부분집합 (.env에 S3_* 필요, .env.example 참고)
.venv/Scripts/python -m pipeline.make_subset --source r2 --date "2026-11-1*" --pages 200
# 또는 R2 접근 전 검증용 합성 데이터
.venv/Scripts/python -m pipeline.make_subset --source synthetic --pages 150 --revs 3 --window 240
# 청크 분할 + bge-m3 임베딩 사전 계산 (embed_ms 실측 포함)
.venv/Scripts/python -m pipeline.embed
```

## 실험 0 실행 (T-07)

```bash
.venv/Scripts/python -m pipeline.run_exp0 --run-id A --speed 1
.venv/Scripts/python -m pipeline.run_exp0 --run-id B --speed 1
.venv/Scripts/python -m pipeline.plot A B        # data/exp0/exp0_stale_curve.png + 재현성 수치
```

run마다 `data/exp0/runs/<run-id>/`에 `edits.parquet`(편집별 해소 시간), `judgments.parquet`(판정 로그),
`worker_log.jsonl`, `replay_log.parquet`, `latency.json`(구간별 지연), `meta.json`(실행 명령·설정)이 남는다.

| 모듈 | 작업 | 역할 |
|---|---|---|
| `make_subset.py` | T-01 | R2/로컬/합성 → `subset.parquet` |
| `embed.py` | T-02 | 청크 분할(문단 단위, 800자) + 임베딩 + embed_ms |
| `docker-compose.exp.yml`, `connector.json`, `register_connector.py` | T-03 | CDC 경로 |
| `replayer.py` | T-04 | 원래 간격 × 배속 재생, t0 기록 |
| `faiss_store.py`, `worker.py` | T-05 | 더 새 리비전일 때만 교체하는 Flat 인덱스 + Kafka 소비 + 검색 HTTP |
| `evaluate.py`, `check_t06.py` | T-06 | normal / stale / missing 판정 |
| `run_exp0.py`, `plot.py`, `latency.py` | T-07 | 한 번에 실행, 곡선, 구간별 지연 분해 |

주의: Docker Desktop의 VM 시계가 호스트와 수십 초 어긋날 수 있다. stale 곡선은 호스트 시계만 쓰고,
구간별 지연(`latency.py`)은 run 시작 때 잰 오프셋으로 보정한다.

## 실데이터·실험 1 (2026-11-18~)

임베딩 기준 장비는 GPU(RTX 3060 Laptop, fp16)다. GPU 작업은 WSL의 conda 환경에서 돌린다.

```bash
# 레이크(R2) → 로컬. --until 날짜(UTC)까지
.venv/Scripts/python -m pipeline.make_subset --source r2 --until 2026-11-17 --pages 0 --min-revs 1 --out data/lake/edits.parquet
# 전체 임베딩 (WSL, GPU). 중단돼도 --resume 으로 이어감
python -m pipeline.embed --subset data/lake/edits.parquet --out data/lake/emb --device cuda --fp16 --resume
# R2에 올리기 / 받기
.venv/Scripts/python -m pipeline.upload_r2 up data/lake/emb embeddings/bge-m3/until=2026-11-17
.venv/Scripts/python -m pipeline.upload_r2 down embeddings/bge-m3/until=2026-11-17 data/lake/emb

# 실험 1 입력: 6시간 구간 + 구간 직전 상태(base, 없으면 위키 API에서)
.venv/Scripts/python -m pipeline.make_window --start 2026-11-16T00:00:00 --hours 6 --out data/exp1/subset.parquet
python -m pipeline.embed --subset data/exp1/subset.parquet --out data/exp1/emb --device cuda --fp16   # WSL
# 조건표 전체 2회씩 → 정리
.venv/Scripts/python -m pipeline.run_exp1 --list
.venv/Scripts/python -m pipeline.run_exp1
.venv/Scripts/python -m pipeline.summarize_exp1     # docs/results/exp1/
```

임베딩 출력은 디렉터리 하나다. `revisions/`는 리비전별 청크 목록(위치 기준 chunk_id, sha1, 바뀜 여부, embed_ms)이고,
`vectors/`는 문서별로 처음 나온 텍스트의 벡터(float16)다. 직전 리비전에 같은 텍스트가 있으면 벡터를 다시 쓰고
그만큼은 embed_ms에 들어가지 않는다.

| 모듈 | 역할 |
|---|---|
| `make_window.py` | 실험 1 리플레이 구간 + base 리비전 |
| `embstore.py` | 임베딩 디렉터리 읽기 (옛 단일 파일 형식도 읽음) |
| `upload_r2.py` | 디렉터리 ↔ R2 |
| `run_exp1.py` | 조건표 (배속, 반영 주기, 워커 수, embed 대기 배율, Debezium poll 간격) 2회씩 |
| `summarize_exp1.py` | 조건별 곡선, 구간별 지연 막대, 요약 표, 재현성 |

주의: 이 저장소의 `.env`에 키 이름에 공백이 든 줄이 있으면 `docker compose`가 실패한다. 그럴 땐 컨테이너를
`docker start exp0-postgres-1 exp0-kafka-1 exp0-connect-1`로 직접 켠다.

## 실험 2 위키 (P3.1, 2026-11-18~)

> **진행 상태:** 처음(2026-11-18)에는 위키(P3.1)만 돌리고, 커머스(P3.2)를 빼먹었다는 사실을 적지 않았다.
> 노션 계획과 P3의 목표는 같은 세 방식을 위키와 커머스에 똑같이 적용해, 데이터 성격에 따라 맞는 방식이 다르다는 것을 보이는 것이다.
> 커머스는 2026-11-19에 돌렸다. 결과는 아래 [실험 2 커머스](#실험-2-커머스-p32-2026-11-19) 절에 있다.

HNSW 업데이트 처리 방식 3가지(꺼두기·지웠다 다시 넣기·재구축)를 hnswlib 하나로 비교한다. Windows에서는 pip `hnswlib`가
빌드되지 않아 같은 API의 `chroma-hnswlib`(0.7.6, wheel 제공)를 쓴다. CDC 경로(Docker)는 쓰지 않는다. 가로축이 시간이
아니라 누적 업데이트 수라서, 레이크 임베딩을 편집 시각 순으로 인덱스에 바로 반영한다.

```bash
.venv/Scripts/python -m pip install chroma-hnswlib==0.7.6
.venv/Scripts/python -m pipeline.exp2_data                       # 레이크 임베딩 → data/exp2/stream.npz (초기 청크 + 청크 연산)
sh data/exp2/run_all.sh                                          # 4조건 × 2회 (약 70분, 끝난 run은 건너뜀)
.venv/Scripts/python -m pipeline.run_exp2 --method rebuild --threshold 0.05 --rep a   # 한 조건만
.venv/Scripts/python -m pipeline.summarize_exp2                  # docs/results/exp2/
```

| 모듈 | 역할 |
|---|---|
| `exp2_data.py` | 문서별 첫 리비전 = 초기 인덱스, 이후 리비전 = 청크 upsert/delete 스트림 |
| `hnsw_store.py` | 처리 방식 3가지. 라벨은 (청크, 리비전)마다 새로 붙여 옛 리비전 혼입을 바로 판정 |
| `run_exp2.py` | 체크포인트(청크 연산 5,000건)마다 hot·cold·fixed 질의 100개씩, ef 10~160의 recall@10·지연·혼입률, 비용 |
| `summarize_exp2.py` | 누적 업데이트 대비 그래프, ef 트레이드오프, 요약 표, 재현성 |

`data/exp2/run_all.sh`는 `data/`(gitignore) 아래라 저장소에 없다. 내용은 조건 4개(`tombstone`, `reinsert`,
`rebuild --threshold 0.05`, `rebuild --threshold 0.1`)를 rep a, b 순으로 `run_exp2`에 넘기는 반복문이다.

### 실험 2 위키 결과 (레이크 11/17까지, 2026-11-18)

입력은 문서 38,952개다. 첫 리비전 청크 579,468개로 초기 인덱스를 만들고, 이후 편집 23,307건이 만든 청크 연산
89,317건(교체 81,125건, 삭제 8,192건)을 반영했다. 인덱스는 hnswlib(M=16, ef_construction=200)이다.
아래 표는 ef=40, 고정 질의 100개 기준이며, 1회차/2회차를 함께 적었다.

| 방식 | 끝날 때 꺼진 노드 | recall@10 시작 → 끝 | p95 지연 시작 → 끝 | 비용 |
|---|---|---|---|---|
| 꺼두기 | 12.2% (노드 +14%) | 0.972 → 0.971 / 0.971 | 0.43 → 0.46ms | 반영 124초 |
| 지웠다 다시 넣기 | 약 0% | 0.972 → 0.962 / 0.963 | 거의 같음 | 반영 590초 (4.7배) |
| 재구축 (꺼진 노드 5%마다) | 3.3% | 0.972 → 0.960 / 0.963 | 거의 같음 | 반영 119초 + 재구축 2회 425초 |
| 재구축 (10%마다) | 2.7% | 0.972 → 0.963 / 0.965 | 거의 같음 | 반영 118초 + 재구축 1회 약 223초 |

- 위키 10일치는 작은 수정 위주라 꺼진 노드가 12%를 넘지 않는다. 이 범위에서는 처리 방식과 상관없이 구조 간극(recall 저하)이 보이지 않았다.
  - 방식 사이 recall 차이는 0.01 이내다. 질의 1,000개 이웃 중 10개 수준이라 그래프마다의 우연과 구분하기 어렵다.
  - 구버전 혼입률은 모든 run에서 0이다.
- 차이는 비용에서 났다.
  - 꺼두기는 쓰기가 가장 싸다. 대신 노드가 14% 늘고 지연이 약 5% 늘었다.
  - 지웠다 다시 넣기는 쓰기가 4.7배 느리다. 지운 슬롯을 재사용하면서 주변 간선을 다시 계산하기 때문이다.
  - 재구축은 1회에 약 3.5분이 걸린다.
- 결론: 이 규모의 위키는 꺼두기로 충분하고, 재구축이나 지웠다 다시 넣기는 비용만 늘린다.
  - 꺼두기 recall이 떨어지는 지점이 없어서 재구축 기준은 정하지 못했다.
  - 5%·10%는 비용 비교용으로 고른 값이다.
- 재현성: 같은 조건 2회 실행에서 고정 질의 recall의 최대 차이는 꺼두기·재구축 0.001~0.004, 지웠다 다시 넣기 0.028이다.
- 후속: 수집 데이터가 더 쌓이면 같은 명령(`exp2_data` → `run_all.sh` → `summarize_exp2`)으로 다시 잰다.
  - 이번 입력은 레이크를 2026-11-17 15:29 UTC에 뽑은 것이라 그 시각까지의 편집만 들어 있다. 수집기는 그 뒤에도 정상 적재 중이다.
  - 꺼진 노드가 12%를 넘는 구간에서도 간극이 생기는지 확인하는 것이 목적이다.

### 노션 실험 2 계획과의 대조 (위키 부분, 2026-11-19)

기준은 노션 "VectorDB 실험/데이터 파이프라인"의 "실험 2: 데이터 구조(?) 간극" 절(2026-11-18 수정본)이다.

| 노션 계획 | 이번 위키 실행 | 판정 |
|---|---|---|
| 가로축은 누적 업데이트 수, HNSW 고정, 정답은 같은 시점 Flat 전수조사 | 그대로. 체크포인트(청크 연산 5,000건)마다 살아 있는 노드 전체를 전수조사 | 맞음 |
| 세 방식을 두 데이터에 똑같이 적용 | 처음엔 위키에만 적용. 커머스는 2026-11-19에 같은 하네스로 실행 (아래 절) | 맞음 (11-19부터) |
| 꺼두기: 옛 노드는 두고 결과에서만 거름 | `mark_deleted`. 옛 노드가 그래프에 남아 경유된다 | 맞음 |
| 지웠다 다시 넣기: 옛 노드를 지우고 새 노드를 넣음 (hnswlib) | `mark_deleted` 후 `replace_deleted=True`로 지운 슬롯에 새 벡터를 넣고 그 슬롯의 이웃을 다시 잇는다. 다른 노드에서 들어오던 간선은 남는다 | 부분적으로 맞음. hnswlib에는 간선까지 지우는 삭제가 없다 (D-07) |
| 재구축: 업데이트가 일정량 쌓이면 통째로 새로 만듦 | 꺼진 노드 5%·10%마다 살아 있는 노드만으로 새로 만듦 | 맞음 |
| 꺼두기: 꺼둔 노드가 쌓이면 recall·검색 속도가 얼마나 떨어지나 | 12%까지 recall 그대로, p95 약 5% 증가 | 답함 |
| 지웠다 다시 넣기: 그래프가 덜 망가지나, 쓰기 비용은 | 덜 망가지지 않음(recall 0.01 낮음, 잡음 수준), 쓰기 4.7배 | 답함 |
| 재구축: 언제가 적당한가 (꺼두기 recall 저하 곡선으로 기준 정하기) | 저하가 없어 기준을 못 정함. 5%·10%는 비용 비교용 | **못 답함** |
| 데이터: kowiki 수집분, 되돌림 표본이 부족하면 enwiki로 보완 | kowiki 레이크 10일치. 되돌림 표본 수는 세지 않았다 | 데이터는 맞음, 보완 판단은 안 함 |
| 임베딩 bge-m3 | bge-m3 (CPU 대신 GPU로 미리 계산) | 맞음 |
| 지표: recall@10, p95 (질의 100개), 구버전 혼입률, 비용 | 모두 냄. 질의 세트는 노션에 없어 hot·cold·fixed 100개씩으로 정함 (사용자 결정) | 맞음 |
| 예상: 위키는 언제 재구축할지가 중요 | 10일치에서는 꺼두기로 충분했다. 예상과 다름 | 확인 안 됨 |

- 구버전 혼입률은 8개 run 모두 0이고, 결과 10개를 못 채운 질의도 없다.
  - hnswlib은 `mark_deleted`된 노드를 결과에서 빼기 때문에, 세 방식 모두 구조상 0이 나온다.
  - 이 구현에서는 혼입률로 방식 차이가 드러나지 않는다. 차이는 recall과 지연, 비용으로만 보인다.
  - 커머스의 품절 혼입률도 같은 이유로 0이 나올 것이다.
- "cf) 실험 2에서 확인할 지표"의 IVF 비교는 본문(HNSW 고정)과 어긋나서 하지 않았다 (overview 미결).
- 가역 우회 간선(노션 "Plus")은 P3.3 범위라 하지 않았다.

## 실험 2 커머스 (P3.2, 2026-11-19)

Retailrocket(2015-05~09)에서 같은 상품이 품절됐다가 다시 판매되기를 반복하는 상황을 만든다.
여기에 위키와 같은 세 방식, 같은 hnswlib 설정, 같은 채점 코드(`run_exp2.score`)를 쓴다.
원본 CSV 4개는 `data/retail/`에 둔다 (Kaggle, gitignore).

```bash
.venv/Scripts/python -m pip install gensim==4.4.0
.venv/Scripts/python -m pipeline.exp2c_data                       # item2vec 학습 + 품절·판매 재개 스트림 + 질의 → data/exp2c/ (약 2분)
sh data/exp2c/run_all.sh                                          # 3조건 × 2회 (전체 약 3분, 끝난 run은 건너뜀)
.venv/Scripts/python -m pipeline.run_exp2c --method rebuild --threshold 0 --rep a   # 한 조건만 (0 = 매주 재구축)
.venv/Scripts/python -m pipeline.summarize_exp2 --dataset commerce   # docs/results/exp2c/
```

| 모듈 | 역할 |
|---|---|
| `exp2c_data.py` | 조회 세션(30분 끊김)으로 item2vec 학습, 주간 스냅샷 18개에서 품절·판매 재개 연산, 주별 조회 이벤트 질의 |
| `hnsw_store.py` `apply_flips` | 품절 = `mark_deleted`. 판매 재개: 꺼두기·재구축은 `unmark_deleted`, 지웠다 다시 넣기는 같은 벡터를 `replace_deleted`로 새로 넣음 |
| `run_exp2c.py` | 스냅샷마다 체크포인트. week 질의(그 주 조회 100개)와 fixed 질의(첫 주 100개 고정), ef 10~160 |

`data/exp2c/run_all.sh`는 `tombstone`, `reinsert`, `rebuild --threshold 0`을 rep a, b 순으로 돌리는 반복문이다.

### 입력 (사용자 결정 2026-11-19)

- **상품 벡터:** item2vec, gensim 4.4.0.
  - 설정: skip-gram, 64차원, window 20, negative 5, 10 epoch, `min_count=2`, 단일 스레드, 시드 42.
  - 조회 266만 건, 세션 176만 개에서 상품 161,045개의 벡터를 얻었다. 학습은 105초 걸렸다.
  - 품질 확인: 이웃 10개 중 같은 카테고리 비율이 28.6%다. 무작위로 고른 두 상품이 같은 카테고리일 확률(0.48%)의 약 60배다.
- **카탈로그:** 벡터가 있고 `available` 기록이 있는 상품 133,198개.
  - 기록이 없는 상품 27,847개는 뺐다.
  - 조회 5회 이상인 78,569개는 `views5`로 표시했다. 결과 중 이 상품의 비율(`v5_share`)을 같이 잰다.
- **상태:** `available`은 2015-05-10~09-13 일요일 03:00 UTC 주간 스냅샷 18개다.
  - 초기 인덱스는 첫 스냅샷에서 판매 중인 34,370개다.
  - 이후 상태 전환은 123,384건이다: 품절 61,307, 재입고 49,997, 신규 판매 12,080.
  - 카탈로그 중 품절 비율은 59%에서 74%로 올라간다.
  - 처음 나타난 상품은 그 주에 추가한다.
- **질의:** 조회 이벤트 하나를 추천 요청 하나로 본다.
  - 질의 벡터는 그 방문자가 최근 본 상품 최대 5개 벡터의 평균이다 (평균 2.3개).
  - 정답은 그 시점 판매 중인 상품 전체에 대한 Flat 전수조사다.

### 결과

ef=40, fixed 질의 기준이다. 각 칸은 1회차/2회차다.

| 방식 | 끝날 때 그래프 | recall@10 시작 → 끝 | p95 지연 시작 → 끝 | 쓰기 비용 |
|---|---|---|---|---|
| 꺼두기 | 노드 60,045개 중 41.5%가 꺼짐 | 0.967/0.960 → 0.971/0.969 | 0.035/0.045 → **0.094/0.106ms** (약 2.5배) | 반영 5.8/6.2초. 재입고 36,402번은 표시만 풂 |
| 지웠다 다시 넣기 | 노드 40,708개 중 13.7%가 꺼짐 | 0.967/0.960 → 0.942/0.957 | 0.037/0.035 → 0.046/0.040ms | 반영 30.8/31.5초 (꺼두기의 5.3배). 같은 벡터를 55,739번 다시 넣음 |
| 재구축 (매주) | 판매 중 35,140개만, 꺼진 노드 0 | 0.967/0.960 → 0.937/0.938 | 0.040/0.035 → 0.052/0.037ms | 반영 12.4/10.3초 + 재구축 17번 8.2/7.5초 (1회 약 0.45초) |

- **꺼진 노드가 많이 쌓인다.** 위키는 끝까지 12%였는데, 커머스는 41.5%까지 갔다.
- **꺼두기는 recall을 지키는 대신 지연이 늘었다.**
  - 꺼진 노드가 41%가 되자 같은 ef에서 p95 지연이 약 2.5배가 됐다. 꺼진 노드도 탐색 경로로 방문해서 거리 계산이 늘어나기 때문이다.
  - 그만큼 더 많이 탐색하므로 같은 ef에서 recall은 오히려 가장 높다.
  - 같은 지연끼리 비교하면 다른 방식보다 낫지 않다 (`fig4_ef_tradeoff_fixed.png`). 지연 0.06ms 아래에서는 오히려 가장 낮다.
- **recall 하락의 원인은 그래프 구조가 아니다.**
  - 매주 새로 만든 깨끗한 그래프(재구축)도 ef=40에서 0.967에서 0.937로 떨어졌다.
  - 이 하락은 판매 중 상품 집합이 바뀌면서 질의가 어려워진 효과이고, 그래프 구조 탓이 아니다.
  - 재구축을 기준선으로 보면 꺼두기와 지웠다 다시 넣기 모두 recall 손실(구조 간극)이 없다.
- **지웠다 다시 넣기는 쓰기만 많다.**
  - 재입고 때마다 같은 벡터를 다시 넣어 쓰기가 5배 넘게 들었다. 그런데 recall은 재구축과 비슷하고 지연도 비슷하다.
  - hnswlib에는 간선까지 지우는 삭제가 없다. 그래서 노션이 예상한 "경로가 끊겨 recall이 떨어지는" 현상은 재현되지 않았다.
- **이 규모에서는 매주 재구축이 싸다.** 판매 중 상품 3.5만 개, 64차원이라 재구축 1회에 0.45초다. 위키의 1회 3.5분과 비교된다.
- 품절 혼입률은 모든 run에서 0이고, 10개를 못 채운 질의도 없다. hnswlib가 꺼진 노드를 결과에서 빼기 때문이다.
- 결과 중 조회 5회 이상 상품의 비율은 72~82%로, 카탈로그 안 비율(59%)보다 높다.
- **재현성:** 같은 조건 2회 실행에서 fixed recall의 최대 차이는 0.010~0.023, p95의 최대 차이는 0.02~0.04ms다.
  - 지연이 수십 μs 단위라서, 파이썬 호출 오버헤드와 OS 잡음이 상대적으로 크다.
  - 꺼두기와 재구축의 끝 recall 차이(약 0.03)는 이 잡음보다 크지만, 지웠다 다시 넣기의 1·2회차 차이(0.015)는 잡음 수준이다.

### 위키와 비교

| | 위키 (P3.1) | 커머스 (P3.2) |
|---|---|---|
| 업데이트 성격 | 옛 벡터는 버려짐 | 같은 벡터가 꺼졌다 켜짐 |
| 끝날 때 꺼진 노드 (꺼두기) | 12% | 41.5% |
| 꺼두기의 지연 | +5% | +150% |
| 재구축 1회 | 약 3.5분 (58만 청크, 1024차원) | 약 0.45초 (3.5만 상품, 64차원) |
| 이번 데이터에서 맞는 방식 | 꺼두기 (재구축은 비싸고 이득 없음) | 지연 예산에 따라 다름. 지연이 중요하면 매주 재구축(싸다), 쓰기를 아끼려면 꺼두기 |

- 노션의 예상("위키는 언제 재구축할지가 중요, 커머스는 지우지 말고 꺼두는 게 낫다")은 반만 맞았다.
  - 커머스에서 꺼두기는 쓰기가 가장 싸고 recall도 지켰지만, 그 대가로 지연이 두 배 넘게 늘었다.
  - 위키에서는 재구축 시점이 중요해질 만큼 꺼진 노드가 쌓이지 않았다.
- 지웠다 다시 넣기는 두 데이터 모두에서 쓰기 비용만 크고 이득이 없었다.
