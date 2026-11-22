# 커머스 실시간 세션 추천 파이프라인: 입출력·워크플로 정의 (초안, 2026-11-23)

> `docs/purpose/`의 T-xx에 없는 확장 작업이다. 착수 범위는 팀장 결정에 따른다 (P3.3 착수 여부 결정과 같은 시점, 2026-12-07 예정).
> 실험 1(CDC 경로)과 실험 2 커머스(item2vec, hnswlib 처리 방식)의 코드와 결과를 재사용한다.

## 1. 목적과 범위

Retailrocket 이벤트를 원래 시간 순서대로 재생해서 다음 세 가지를 한 시스템에서 동작시킨다.

1. 방문자의 조회 이벤트가 들어오면 그 방문자의 세션을 갱신한다.
2. 상품 판매 상태가 바뀌면 CDC로 벡터 인덱스에 반영한다.
3. 요청이 오면 지금 판매 중인 상품만으로 그 방문자에게 맞는 상품 top-k를 돌려준다.

**주장할 수 있는 것**
- 이벤트가 들어온 뒤 세션과 추천에 반영되기까지의 지연(초 단위)
- 상품 상태 변경이 들어온 뒤 인덱스에 반영되기까지의 지연
- 재시작하거나 같은 이벤트가 다시 와도 상태가 같게 수렴한다는 것

**주장하지 않는 것 (비목표)**
- 품절이 분 단위로 반영된다는 주장. 원천 데이터의 판매 상태가 주 단위 스냅샷이라서 할 수 없다 (D-04, P5 제외).
  - 상태 변경은 스냅샷 시점에 한꺼번에 들어온다.
- 추천 품질(CTR, 매출)의 개선. item2vec은 실험 2에서 쓴 것을 그대로 쓰고 튜닝하지 않는다.
- 상용 벡터 DB와의 비교 (P4 제외).

## 2. 원천 데이터 (시스템 밖)

| 파일 | 내용 | 비고 |
|---|---|---|
| `data/retail/events.csv` | `timestamp`(ms), `visitorid`, `event`(view·addtocart·transaction), `itemid`, `transactionid` | 276만 건, 2015-05-03 ~ 09-18.<br>시각순 정렬이 안 되어 있다.<br>완전히 같은 행이 460개 있다. |
| `data/retail/item_properties_part{1,2}.csv` | `timestamp`, `itemid`, `property`, `value` | `available` 행으로 주간 스냅샷 18개(매주 일요일 03:00 UTC)를 만든다.<br>`categoryid` 행도 쓴다. |
| `data/retail/category_tree.csv` | `categoryid`, `parentid` | MVP에서는 쓰지 않는다. |
| `data/exp2c/vectors.f32.npy`, `stream.npz` | item2vec 64차원 벡터, 상품별 주간 상태 행렬 | `pipeline.exp2c_data`가 만든다.<br>여기서는 **벡터 버전 v1**로 쓴다. |

부하 규모 (원래 시간 기준):
- 이벤트는 평균 분당 14건(초당 0.23건)이다. 분 단위 최대는 87건, 초 단위 최대는 28건이다.
- 배속 ×1000이면 평균 초당 230건이 된다.

## 3. 구성 요소

| 프로세스 | 역할 | 새로 만듦 / 재사용 |
|---|---|---|
| **P1 리플레이어** `retail_replayer` | 이벤트와 스냅샷 변경을 하나의 재생 시계로 내보낸다.<br>이벤트는 Kafka로, 상태 변경은 Postgres로 간다.<br>추천 요청도 생성한다. | 새로 만든다. 실험 1 `replayer`의 재생 시계 방식을 따른다. |
| **P2 세션 처리기** `session_worker` | `retail.events`를 소비해 Redis의 세션 상태와 인기 집계를 갱신한다. | 새로 만든다. |
| **P3 인덱스·추천 서비스** `reco_service` | CDC를 소비해 hnswlib에 반영하고, 정책에 따라 재구축하고, `/recommend` HTTP를 제공한다. | 새로 만든다. 핵심은 `hnsw_store.HnswUpdater` 재사용이다. |
| P4 레이크 적재 `lake_sink` (확장) | 이벤트를 날짜별 Parquet로 R2/MinIO에 쌓는다. | 새로 만든다. wiki-collector와 같은 구조다. |
| P5 배치 재학습 `retrain` (확장) | 레이크로 item2vec을 다시 학습해 벡터 v2를 만든다. | `exp2c_data`의 학습 부분을 재사용한다. |
| 인프라 | Postgres 16, Kafka 3.8, Debezium 2.7, **Redis (새로 추가)** | `docker-compose.exp.yml`에 Redis만 더한다. |

## 4. 입출력 정의

### 4.1 Kafka 토픽

**`retail.events`** (P1 → P2, P4)
- 키: `visitorid`. 같은 방문자의 이벤트는 같은 파티션에 들어가 순서가 보장된다.
- 파티션: 4

```json
{"event_id": "a1b2c3d4e5f60718", "ts": 1431226800000, "visitorid": 257597, "event": "view",
 "itemid": 355908, "transactionid": null, "t0_host": 1763870000.123}
```
- `event_id`: `(ts, visitorid, event, itemid, transactionid)`의 해시 16자리다. 완전히 같은 행은 같은 id가 되어 중복 제거의 기준이 된다.
- `ts`: 원래 이벤트 시각(event time)이다.
- `t0_host`: 리플레이어가 보내기 직전의 호스트 시각(epoch s)이다. 반영 지연을 잴 때 쓴다 (실험 1과 같은 방식).

**`retail.public.products`** (Debezium → P3)
- Postgres `products` 테이블의 변경분이다. `topic.prefix`는 `retail`이다.
- 값은 변경 뒤 행 전체(`after`)다. 차이(delta)가 아니라 **상태**를 담고 있어서, 같은 메시지를 다시 적용해도 결과가 같다.

```json
{"op": "u", "after": {"itemid": 355908, "available": false, "categoryid": 1338,
                      "state_ts": 1431226800000, "t0_host": 1763870000.456}, "source": {"lsn": 123456}}
```

### 4.2 Postgres (정답 기준, GR-02)

```sql
CREATE TABLE products (
    itemid      BIGINT PRIMARY KEY,
    available   BOOLEAN NOT NULL,
    categoryid  BIGINT,
    state_ts    BIGINT NOT NULL,            -- 이 상태가 유효해진 스냅샷 시각 (원래 시간, ms)
    t0_host     DOUBLE PRECISION NOT NULL,  -- 리플레이어가 UPDATE 직전 찍은 호스트 시각
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
```
- 상품 범위: 실험 2 커머스 카탈로그와 같다 (조회 2회 이상이면서 판매 상태 기록이 있는 13.3만 개).
- 스냅샷이 바뀔 때는 **상태가 바뀐 행만** UPDATE하고, 처음 나타난 상품은 INSERT한다.

### 4.3 Redis (온라인 세션 상태)

| 키 | 타입 | 내용 | 쓰는 쪽 / 읽는 쪽 |
|---|---|---|---|
| `sess:{visitorid}:items` | LIST | 최근 본 상품 id, 최신순, 최대 20개 | P2 / P3 |
| `sess:{visitorid}:meta` | HASH | `start_ts`, `last_ts`, `last_event_id`, `n_events`, `t0_host`(마지막 이벤트) | P2 / P3 |
| `pop:{yyyymmdd}` | ZSET | 그날(원래 시간) 상품별 조회 수 | P2 / P3 |
| `wm:events` | STRING | P2가 처리한 이벤트 시각의 최댓값(워터마크) | P2 / P3 |

- **세션 벡터 대신 상품 id를 저장한다.** 평균 벡터는 질의할 때 P3이 자기 인덱스와 같은 버전의 벡터로 계산한다.
  - 그래서 벡터 v1과 v2가 섞여 세션과 인덱스가 어긋나는 문제가 구조적으로 생기지 않는다.
- TTL은 메모리 회수용일 뿐이다. 세션 만료 판단은 이벤트 시각으로 한다 (5절).

### 4.4 추천 API (P3, HTTP)

`GET /recommend?visitor_id=257597&k=10`

```json
{"visitor_id": 257597, "items": [{"itemid": 1234, "score": 0.83}],
 "source": "session", "session_items_used": 5, "watermark_ts": 1431226800000,
 "vector_version": "v1", "index_epoch": 3, "latency_ms": 1.2}
```
- `source`
  - `session`: 세션 기반 추천
  - `popular`: 세션이 없거나 벡터가 있는 상품이 없을 때, 그날 인기 상품 중 판매 중인 것
- 결과에는 **판매 중인 상품만** 들어간다. 이번 세션에서 이미 본 상품은 뺀다.
- `index_epoch`: 재구축할 때마다 1씩 올라간다.

그 밖의 엔드포인트:
- `GET /health`: 준비 여부, 인덱스 노드 수, 꺼진 노드 비율
- `GET /metrics`: 7절의 지표 (Prometheus 텍스트 형식)
- `GET /reconcile`: 인덱스에 살아 있는 상품 집합과 Postgres `available=true` 집합의 차이

### 4.5 벡터 아티팩트

```
data/reco/vectors/v1/vectors.f32.npy   # (n, 64), 행 = items.parquet 순서
data/reco/vectors/v1/items.parquet     # itemid, row
data/reco/vectors/v1/meta.json         # 학습 구간, 파라미터, 만든 시각
```
v1은 `data/exp2c/`에서 변환한다. P5가 v2 이후 버전을 만든다.

## 5. 시간 모델

- **재생 시계:** `sim_ts = start_ts + (host_now − host_start) × speed`
  - P1 하나가 이 시계로 이벤트(Kafka)와 스냅샷 변경(Postgres)을 모두 내보낸다. 두 흐름이 같은 시계를 쓰므로 순서가 결정적이다.
- **기본 구간:** 스냅샷 0(2015-05-10 03:00 UTC)부터 시작한다. 그 시점의 판매 상태가 초기 상태다.
  - 데모 기본값은 1주를 ×1000으로 돌리는 것이다(약 10분). 스냅샷 경계가 한 번 들어간다.
- **세션 규칙 (실험 2 커머스와 같다):**
  - 이벤트 시각 기준으로 30분 동안 이벤트가 없으면 새 세션이다.
  - 질의 벡터는 그 세션에서 최근 본 상품 중 벡터가 있는 최대 5개의 평균이다.
  - MVP에서는 조회(view)만 세션에 넣는다. addtocart는 미결이다 (9절).
- **세션 만료:** P3은 `wm:events − last_ts > 30분`이면 세션이 끝났다고 보고 `popular`로 응답한다. 벽시계가 아니라 워터마크로 판단하므로 배속과 상관없이 같은 결과가 나온다.

## 6. 워크플로

### W1. 부트스트랩 (run 시작 시 한 번)
1. 인프라를 띄운다: Postgres, Kafka, Debezium, Redis.
2. 초기화: `products`를 비우고 스냅샷 0 상태를 INSERT한다.
   - 이때는 Debezium 커넥터를 등록하기 전이거나 `snapshot.mode=no_data`이므로 초기 적재는 CDC로 흐르지 않는다.
   - Redis를 비우고, Kafka 토픽을 다시 만든다.
3. 커넥터를 등록한다 (`retail.public.products`).
4. P3을 시작한다.
   - 벡터 v1을 읽고, Postgres `available=true` 상품으로 hnswlib 인덱스를 만든다 (M=16, efc=200).
   - CDC 소비를 시작한다.
   - 준비가 되면 `/health.ready=true`가 된다.
5. P2를 시작한다.
6. P3이 준비되면 P1을 시작한다.

출력: 준비된 인덱스(epoch 0), 빈 세션 저장소, run 메타(`data/reco/runs/<run_id>/meta.json`)

### W2. 이벤트 → 세션 (스트리밍)
1. P1이 이벤트를 `sim_ts` 순서대로 `retail.events`에 보낸다.
   - 원본이 정렬되어 있지 않으므로 먼저 `(ts, event_id)` 순으로 정렬한다.
2. P2가 이벤트를 하나씩 소비하면서 방문자별로 다음을 처리한다.
   - `(ts, event_id) ≤ (last_ts, last_event_id)`이면 건너뛴다. 이것으로 중복과 재처리를 막는다.
   - `ts − last_ts > 30분`이면 세션을 초기화한다.
   - view이면 `items`에 LPUSH하고 LTRIM으로 20개만 남기고, `pop:{날짜}`를 1 올린다.
   - `meta`와 `wm:events`를 갱신한다.
   - 이 갱신들은 Redis MULTI 한 번으로 묶는다.
3. Redis에 쓴 다음에 Kafka 오프셋을 커밋한다. 적어도 한 번(at-least-once) 처리되고, 단계 2의 건너뛰기 규칙 때문에 결과는 한 번 처리한 것과 같다.

출력: Redis 세션 상태, 지표 `event_to_session_ms`(쓴 시각 − `t0_host`)

### W3. 상품 상태 변경 → 인덱스 (CDC)
1. `sim_ts`가 스냅샷 k의 시각을 지나면, P1이 스냅샷 k에서 상태가 바뀐 행들을 `products`에 반영한다.
   - 한 트랜잭션 안에서 UPDATE와 INSERT를 한다. 행마다 `t0_host`가 들어간다.
2. Debezium이 변경을 `retail.public.products`로 보낸다.
3. P3이 `after.available`에 따라 인덱스에 반영한다.
   - true: 벡터가 없으면 건너뛰고 `no_vector`를 센다. 이미 살아 있으면 무시한다. 꺼져 있던 노드면 `unmark`하고, 처음이면 새로 삽입한다.
   - false: 살아 있으면 `mark_deleted`한다.
   - 실험 2의 `apply_flips`와 같은 규칙이다. 처리 방식은 설정값이고 기본은 꺼두기다.
4. 메시지가 상태를 담고 있어서 같은 메시지를 다시 적용해도 결과가 같다.
5. 반영이 끝난 뒤 오프셋을 커밋한다.

출력: 갱신된 인덱스, 지표 `cdc_to_index_ms`(반영 시각 − `t0_host`)

### W4. 추천 요청 (서빙)
1. 요청을 만드는 방식: P1이 조회 이벤트의 일부(기본 10%)를 골라, 그 이벤트를 보내고 `d`ms 뒤(기본 200ms)에 그 방문자로 `/recommend`를 호출한다.
   - 응답과 함께 방금 본 상품이 세션에 반영됐는지(`read_your_write`)를 기록한다.
2. P3이 요청을 처리한다.
   - Redis에서 `items`와 `meta`를 읽는다.
   - 세션이 살아 있는지 확인한다(5절 만료 규칙).
   - 벡터가 있는 최근 5개 상품의 평균으로 질의 벡터를 만든다.
   - hnswlib에 `k + 세션 상품 수`개를 요청한다(ef=40). 세션에서 본 상품을 빼고 상위 k개를 돌려준다.
3. 세션이 없으면 `pop:{오늘}`의 상위 상품 중 인덱스에 살아 있는 것으로 채운다.
   - 부족하면 전날 인기 상품까지 본다.

출력: 4.4의 응답, 요청 로그(`requests.jsonl`)

### W5. 인덱스 유지 (정책 재구축)
1. W3 반영이 끝날 때마다 트리거를 검사한다.
   - `dead_ratio ≥ θ` (기본 0.1)
   - 최근 1분 검색 p95가 예산을 넘음
   - 기준값은 실험 2 커머스 결과에서 정한다. 미결이다.
2. 백그라운드 스레드가 지금 살아 있는 상품만으로 새 인덱스를 만든다.
   - 그동안 들어온 CDC 반영은 기존 인덱스에도 적용하고, 버퍼에도 쌓는다.
3. 새 인덱스가 다 만들어지면 버퍼를 새 인덱스에 적용한 뒤, 참조를 한 번에 바꾼다 (`index_epoch += 1`).
4. 그 사이에도 검색은 계속 기존 인덱스로 처리한다.

출력: 지표 `rebuilds`, `rebuild_s`, `swap_pause_ms`, `dead_ratio`

### W6. 배치 경로 (확장)
1. P4가 `retail.events`를 소비해 `lake/retail/events/date=YYYY-MM-DD/*.parquet`에 쌓는다.
2. P5가 (재생 시계 기준으로) 매일 또는 매주 레이크 전체로 item2vec을 다시 학습해 `vectors/v{n+1}`을 만든다.
3. P3이 새 버전을 감지한다. 새 벡터와 현재 판매 상태로 인덱스를 만들고, W5와 같은 방식으로 교체한다.
   - 세션에는 상품 id만 있으므로 교체 즉시 새 버전의 벡터로 질의 벡터를 계산한다.
   - 응답에 `vector_version`이 실린다.

### W7. 장애와 재시작
| 멈춘 것 | 다시 켰을 때 | 수렴 근거 |
|---|---|---|
| P2 | 마지막으로 커밋한 오프셋부터 다시 소비한다. | `(ts, event_id)` 건너뛰기 규칙으로 중복 반영이 없다. |
| P3 | Postgres `available=true`로 인덱스를 다시 만든 뒤, 커밋한 오프셋부터 CDC를 소비한다. | 메시지가 상태를 담고 있어 다시 적용해도 결과가 같다. Postgres가 정답이다 (GR-02). |
| Debezium | 복제 슬롯의 위치부터 다시 보낸다. | P3 수렴 규칙과 같다. |
| Redis | 세션을 잃는다. 해당 세션은 `popular`로 응답한다. | 세션은 30분짜리라 복구하지 않는다. 필요하면 P2 오프셋을 30분 전으로 되돌려 다시 만든다. |

검증: run이 끝나면 `/reconcile`로 인덱스에 살아 있는 상품 집합이 Postgres와 같은지 확인한다. 실험 2의 `live_ok`와 같은 검사다.

## 7. 지표

| 지표 | 정의 | 어디서 |
|---|---|---|
| `event_to_session_ms` p50/p95 | P2가 Redis에 쓴 시각 − 이벤트의 `t0_host` | P2 |
| `cdc_to_index_ms` p50/p95 | P3이 반영을 마친 시각 − 행의 `t0_host` | P3 |
| `consumer_lag` | 토픽·그룹별 지연 메시지 수 | Kafka |
| `api_ms`, `search_ms` p50/p95 | 요청 전체 시간, hnswlib 검색 시간 | P3 |
| `read_your_write` | 요청 직전에 본 상품이 세션에 들어가 있던 비율 | P1 요청 로그 |
| `oos_exposure` | 응답 상품 중 그 순간 Postgres에서 `available=false`인 비율. CDC 반영 지연 동안만 0보다 커야 한다. | 사후 대조 |
| `source_popular_ratio` | `popular`로 응답한 비율 | P3 |
| `dead_ratio`, `nodes`, `rebuilds`, `rebuild_s`, `swap_pause_ms` | 인덱스 상태 | P3 |
| `reconcile_diff` | 인덱스와 Postgres의 판매 중 상품 집합 차이 수 (끝날 때 0) | P3 |

모든 프로세스는 run 디렉터리에 JSONL로 남긴다. 정리 스크립트가 이를 모아 그림과 표를 만든다.

## 8. 단계

| 단계 | 내용 | 완료 기준 |
|---|---|---|
| S1 (MVP) | W1~W4. P1·P2·P3, Redis 추가, 꺼두기만 적용 | 1주 구간 ×1000 run이 끝까지 돈다.<br>`reconcile_diff = 0`<br>`read_your_write`, `event_to_session_ms`, `cdc_to_index_ms`, `api_ms`가 나온다. |
| S2 | W5 (정책 재구축과 더블 버퍼 교체), W7 (재시작 검증) | 재구축 중에도 요청 실패가 0이다.<br>P2와 P3을 강제로 죽였다 다시 켜도 `reconcile_diff = 0`이고 세션이 일치한다. |
| S3 (확장) | W6 (레이크, 재학습, 벡터 버전 교체), 대시보드 | 버전을 바꾸는 동안 응답의 `vector_version`이 섞이지 않는다. |

## 9. 미결 (결정 필요)

- [ ] Redis 도입 여부. 대안은 P3 프로세스 안의 딕셔너리(재시작 시 소실)나 Postgres 테이블(지연 증가)이다.
- [ ] 세션 처리기 구현. 기본안은 파이썬 Kafka 컨슈머다. 대안은 Bytewax나 Flink다.
- [ ] addtocart를 세션 상품에 넣을지, 가중치를 줄지. 기본안은 view만이다 (실험 2와 같음).
- [ ] 요청 생성 비율과 지연(`10%`, `200ms`), 데모 구간과 배속(1주, ×1000)
- [ ] 재구축 트리거 θ와 p95 예산
- [ ] 벡터가 없는 신규 상품의 처리. 기본안은 인덱스에서 빼는 것이다. 대안은 카테고리 평균 벡터로 넣는 것이다 (S3).
