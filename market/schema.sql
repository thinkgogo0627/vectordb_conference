-- ② 1분봉 정규화의 출력 = 원본 DB (설계 3절 ③). market.bar_builder가 시작할 때 만든다.

-- 두 소스의 1분봉을 같은 형식으로. 체결이 없는 분은 행이 없다 (Alpaca와 같은 규칙).
CREATE TABLE IF NOT EXISTS bars_1m (
    source       TEXT        NOT NULL,             -- 'upbit' | 'alpaca'
    symbol       TEXT        NOT NULL,             -- 'KRW-BTC' | 'AAPL'
    minute_ts    TIMESTAMPTZ NOT NULL,             -- 봉 시작 시각 (UTC). 봉 확정 = minute_ts + 1분
    asset_class  TEXT        NOT NULL,             -- 'crypto' | 'us_equity'
    o DOUBLE PRECISION NOT NULL, h DOUBLE PRECISION NOT NULL,
    l DOUBLE PRECISION NOT NULL, c DOUBLE PRECISION NOT NULL,
    v DOUBLE PRECISION NOT NULL,                   -- 거래량 (업비트: 코인 수량, Alpaca: IEX 주식 수)
    n INTEGER NOT NULL,                            -- 체결 수
    vw DOUBLE PRECISION,                           -- 거래량 가중 평균가
    revision     INTEGER     NOT NULL DEFAULT 0,   -- 값이 실제로 바뀐 횟수 (늦은 체결, 1분봉 정정)
    recv_ts      BIGINT      NOT NULL,             -- 이 값을 만든 마지막 원천 메시지의 수신 시각 (호스트 ms)
    built_ts     BIGINT      NOT NULL,             -- bar_builder가 쓴 시각 (호스트 ms, recv_ts와 같은 시계)
    PRIMARY KEY (source, symbol, minute_ts)
);
CREATE INDEX IF NOT EXISTS bars_1m_minute ON bars_1m (minute_ts);

-- 종목별 평소 값과 오늘의 종목 목록 (market.backfill이 만든다. 설계 4절)
CREATE TABLE IF NOT EXISTS symbol_ref (
    source       TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    asset_class  TEXT NOT NULL,
    asof         TIMESTAMPTZ NOT NULL,           -- 이 시각 이전 데이터로 계산
    n_days       INTEGER NOT NULL,
    sigma5       DOUBLE PRECISION NOT NULL,      -- 평소 5분 로그수익률 크기 (1.4826 × MAD)
    vref         DOUBLE PRECISION[] NOT NULL,    -- 슬롯별 평소 5분 거래량 (미국 30분 슬롯 13개, 코인 UTC 시간 24개)
    fill5        DOUBLE PRECISION NOT NULL,      -- 5분 묶음 중 체결이 있던 비율
    in_universe  BOOLEAN NOT NULL,               -- fill5 >= 0.8
    PRIMARY KEY (source, symbol)
);

-- 거래 정지·재개 (Alpaca statuses). 종목마다 최신 상태 한 행. 이력은 CDC로 남는다.
CREATE TABLE IF NOT EXISTS symbol_status (
    source       TEXT   NOT NULL,
    symbol       TEXT   NOT NULL,
    status_code  TEXT   NOT NULL,                  -- 예: 'H' 정지, 'T' 거래 중
    status_msg   TEXT,
    reason_code  TEXT,
    reason_msg   TEXT,
    status_ts    TIMESTAMPTZ NOT NULL,             -- 원천 시각
    recv_ts      BIGINT NOT NULL,
    built_ts     BIGINT NOT NULL,
    PRIMARY KEY (source, symbol)
);
