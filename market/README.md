# market/: 업비트 + 미국 주식 실시간 파이프라인

설계는 [docs/design/market-reco.md](../docs/design/market-reco.md)에 있다. 지금 구현된 것은 ① 수집과 ② 1분봉 정규화다.

## 수집기

| 모듈 | 원천 | Kafka 토픽 (키 = 종목) |
|---|---|---|
| `upbit_collector` | 업비트 WebSocket, 원화 마켓 전 종목 체결 (키 불필요) | `raw.upbit.trade` |
| `alpaca_collector` | Alpaca IEX WebSocket. 1분봉·1분봉 정정·거래 상태는 전 종목(`*`), 체결은 `--trades`로 최대 30종목 | `raw.alpaca.bar`, `raw.alpaca.bar_update`, `raw.alpaca.status`, `raw.alpaca.trade`, `raw.alpaca.trade_fix` |
| (공통) | 연결, 끊김, 구독 응답, 오류 | `raw.collector.events` |

- 메시지 값은 `{"src", "recv_ts"(호스트 epoch ms), "data"(원천 메시지 그대로)}`이다. 가공은 다음 단계(② 1분봉 정규화)에서 한다.
- 끊기면 1초부터 두 배씩, 최대 60초까지 기다렸다 다시 연결한다. 데이터를 받은 연결이 끊기면 1초부터 다시 센다.
- 인증 실패(401·402·404·409)는 재시도하지 않고 멈춘다.
- Alpaca 키는 `.env`에서 이름에 `ALPACA`(또는 `APCA`)와 `KEY`/`ID`·`SECRET`이 들어간 항목을 찾는다. 키 하나당 연결은 1개라, 수집기도 한 번에 하나만 띄운다.

```bash
# Kafka (저장소 .env의 키 이름에 공백이 있어 compose가 읽지 못하므로 빈 env 파일을 준다)
docker compose --env-file <빈 파일> -f docker-compose.exp.yml up -d --wait kafka

python -m market.upbit_collector
python -m market.alpaca_collector --trades AAPL,NVDA,TSLA,COIN,MSTR,SPY,QQQ
python -m market.peek                    # 토픽별 건수, 종목 수, 수신 지연, 예시

# Kafka 없이 확인: data/market/raw/*.jsonl
python -m market.upbit_collector --sink jsonl --seconds 60
python -m market.alpaca_collector --test --sink jsonl --seconds 90     # 테스트 스트림 (FAKEPACA, 24시간)
```

`peek`의 수신 지연(수신 − 원천 시각)에는 호스트 시계 오차가 그대로 섞인다. 1분봉은 봉 확정 시각(시작 + 60초)을 기준으로 잰다.

## 1분봉 정규화 (`bar_builder`)

Kafka `raw.*`에서 읽어 Postgres `bars_1m`·`symbol_status`([schema.sql](schema.sql))에 쓴다.

| 원천 | 처리 |
|---|---|
| 업비트 체결 | 체결 시각 기준 1분봉으로 묶는다(`bars.UpbitMinuteBars`).<br>· 워터마크가 봉 끝 + 5초를 지나면 확정한다.<br>· 늦게 온 체결은 확정한 봉을 다시 계산한다(10분까지).<br>· 같은 체결(`sequential_id`)은 한 번만 센다. |
| Alpaca 1분봉·정정 | 형식만 바꿔 그대로 쓴다. 정정이 오면 같은 행의 값과 `revision`이 바뀐다. |
| Alpaca 거래 상태 | 종목마다 최신 상태 한 행 |

**재처리 안전성**
- 오프셋을 커밋하지 않는다. 시작할 때 소스마다 "DB의 마지막 봉 − 3분" 지점으로 되감아 읽는다. DB가 비어 있으면 처음부터 읽는다.
- 쓰기는 값이 실제로 바뀔 때만, 그리고 수신 시각이 저장된 값 이후일 때만 반영한다.
- 그래서 같은 데이터를 몇 번 다시 읽어도 결과가 같다.

**일부만 본 봉은 쓰지 않는다**
- 업비트 수집기는 (재)연결할 때마다 `raw.upbit.trade`에 세션 시작 표지를 넣는다. 토픽이 1파티션이라 체결과 순서가 섞이지 않는다.
- bar_builder는 이 표지를 보면 끊기기 전에 열려 있던 봉을 버리고, 다시 연결된 뒤 첫 분도 버린다.
- 가장 최근 메시지가 10초보다 오래됐으면 워터마크를 멈춘다. 수집기가 멈출 때 열려 있던 분을 확정하지 않기 위해서다.

```bash
docker compose --env-file <빈 파일> -f docker-compose.exp.yml up -d --wait postgres kafka
python -m market.bar_builder
python -m market.verify_upbit --since 2026-11-24T16:35:00Z --until 2026-11-24T16:38:00Z   # 업비트 공식 1분봉과 대조
```

접속 주소는 `MARKET_PG_DSN`, `MARKET_KAFKA` 환경변수로 바꾼다 (기본: 로컬 docker-compose.exp.yml).

## 상태 벡터와 기능 1 "지금 비슷한 종목" (`features`, `backfill`, `cdc`, `vector_worker`)

| 모듈 | 하는 일 |
|---|---|
| `features.py` | 1분봉 → 60분 창의 5분 묶음 12개 → 30차원 벡터 (가격 12, 거래량 12, 요약 6). 거래 부족·정규장 밖·종목 목록 밖이면 벡터를 만들지 않는다 (설계 4절) |
| `backfill.py` | 과거 1분봉으로 종목별 평소 값(`symbol_ref`: 평소 5분 변동폭, 시간대별 평소 거래량, 5분 묶음 채움 비율)과 오늘의 종목 목록(채움 80% 이상)을 만들고, 목록 종목의 1분봉을 `bars_1m`에 넣는다 (이미 있는 행은 그대로 둔다) |
| `pairs.py` | 같은 기초자산 쌍(`symbol_pair`): 미국 정규장 5분 수익률 상관 \|corr\| ≥ 0.95. 기능 1이 결과에서 접는다 (BTC ↔ IBIT, NVDA ↔ NVDL). 손 보정은 `same_underlying_manual.json` |
| `cdc.py` | Debezium 커넥터 `market-bars` (`bars_1m` → `market.public.bars_1m`, poll 50ms) |
| `vector_worker.py` | CDC를 받아 종목별 최근 80분 1분봉을 유지하고, 소스별로 분이 넘어가면 전 종목 벡터를 다시 계산해 현재 상태 인덱스(Flat)를 바꾼다. 지난 분의 정정이 오면 그 종목만 다시 계산한다. HTTP `/similar`, `/status` |
| `similar.py` | 조회 명령 |

```bash
python -m market.backfill upbit --days 7 --top 150     # 업비트: 24시간 거래대금 상위 150종목, 7일
python -m market.backfill alpaca --days 20             # 미국: 상장 전 종목, 직전 20거래일 IEX (약 1시간)
python -m market.backfill upbit-gap --hours 2          # 실시간 수집 시작 전 빈 구간 메우기 (평소 값은 그대로)
python -m market.pairs build                           # 같은 기초자산 쌍 (벡터 워커 시작 전, 과거 데이터를 넣은 뒤)
python -m market.pairs show KRW-BTC                    # 한 종목의 짝과 상관
python -m market.cdc register                          # 과거 데이터를 다 넣은 뒤에 등록 (넣는 중에 등록하면 수백만 행이 CDC로 흐른다)
python -m market.vector_worker
python -m market.similar KRW-BTC                       # 지금 BTC와 비슷하게 움직이는 종목 top-10 (같은 기초자산은 접음)
python -m market.similar KRW-BTC --no-fold             # 접지 않고 그대로
python -m market.similar NVDA --scope crypto
python -m market.similar                               # 상태: 인덱스 종목 수, 제외 이유별 수, 지연
```

미국 주식 벡터는 창이 정규장 안에 있어야 해서 장 시작 60분 뒤(한국 시간 겨울 00:30)부터 나온다.
