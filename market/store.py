"""Postgres 쓰기: bars_1m·symbol_status upsert. 같은 값을 다시 써도 결과가 같다 (재처리 안전).

- 값이 실제로 바뀔 때만 revision을 올린다. Kafka를 되감아 같은 메시지를 다시 처리해도 revision이 늘지 않는다.
- 수신 시각(recv_ts)이 저장된 값보다 이른 메시지로는 덮어쓰지 않는다. 1분봉과 1분봉 정정은 다른 토픽이라
  되감아 읽을 때 순서가 뒤바뀔 수 있는데, 이 조건 때문에 정정본이 원본으로 되돌아가지 않는다.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg

DSN = os.environ.get("MARKET_PG_DSN", "host=localhost port=5432 user=exp password=exp dbname=exp")
SCHEMA = Path(__file__).with_name("schema.sql")


@dataclass
class Bar:
    source: str
    symbol: str
    minute_ms: int          # 봉 시작 (UTC epoch ms)
    asset_class: str
    o: float
    h: float
    l: float
    c: float
    v: float
    n: int
    vw: float | None
    recv_ts: int


UPSERT_BAR = """
INSERT INTO bars_1m (source, symbol, minute_ts, asset_class, o, h, l, c, v, n, vw, recv_ts, built_ts)
VALUES (%s, %s, to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (source, symbol, minute_ts) DO UPDATE SET
    o = EXCLUDED.o, h = EXCLUDED.h, l = EXCLUDED.l, c = EXCLUDED.c, v = EXCLUDED.v, n = EXCLUDED.n,
    vw = EXCLUDED.vw, recv_ts = EXCLUDED.recv_ts, built_ts = EXCLUDED.built_ts,
    revision = bars_1m.revision + 1
WHERE EXCLUDED.recv_ts >= bars_1m.recv_ts      -- 더 오래된 메시지로 덮어쓰지 않는다 (되감기·토픽 간 순서 뒤바뀜)
  AND (bars_1m.o, bars_1m.h, bars_1m.l, bars_1m.c, bars_1m.v, bars_1m.n)
      IS DISTINCT FROM (EXCLUDED.o, EXCLUDED.h, EXCLUDED.l, EXCLUDED.c, EXCLUDED.v, EXCLUDED.n)
RETURNING (xmax = 0)
"""

UPSERT_STATUS = """
INSERT INTO symbol_status (source, symbol, status_code, status_msg, reason_code, reason_msg, status_ts, recv_ts, built_ts)
VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s / 1000.0), %s, %s)
ON CONFLICT (source, symbol) DO UPDATE SET
    status_code = EXCLUDED.status_code, status_msg = EXCLUDED.status_msg, reason_code = EXCLUDED.reason_code,
    reason_msg = EXCLUDED.reason_msg, status_ts = EXCLUDED.status_ts, recv_ts = EXCLUDED.recv_ts,
    built_ts = EXCLUDED.built_ts
WHERE symbol_status.status_ts <= EXCLUDED.status_ts
"""


class Store:
    def __init__(self, dsn: str = DSN):
        self.conn = psycopg.connect(dsn, autocommit=False)
        with self.conn.cursor() as cur:
            cur.execute(SCHEMA.read_text(encoding="utf-8"))
        self.conn.commit()

    def write_bars(self, bars: list[Bar]) -> dict[str, int]:
        """한 트랜잭션. 반환: 새 행·값이 바뀐 행·그대로인 행 수."""
        if not bars:
            return {"inserted": 0, "updated": 0, "unchanged": 0}
        built = int(time.time() * 1000)
        ins = upd = 0
        with self.conn.cursor() as cur:
            cur.executemany(UPSERT_BAR, [(b.source, b.symbol, b.minute_ms, b.asset_class, b.o, b.h, b.l, b.c, b.v,
                                          b.n, b.vw, b.recv_ts, built) for b in bars], returning=True)
            while True:
                row = cur.fetchone()
                if row is not None:
                    ins += bool(row[0])
                    upd += not row[0]
                if not cur.nextset():
                    break
        self.conn.commit()
        return {"inserted": ins, "updated": upd, "unchanged": len(bars) - ins - upd}

    def write_statuses(self, rows: list[tuple]) -> None:
        """rows: (source, symbol, code, msg, reason_code, reason_msg, status_ms, recv_ts)"""
        if not rows:
            return
        built = int(time.time() * 1000)
        with self.conn.cursor() as cur:
            cur.executemany(UPSERT_STATUS, [(*r, built) for r in rows])
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
