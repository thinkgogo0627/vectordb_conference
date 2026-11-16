"""구간별 지연 분해 (GR-03 해석용, 실험 1 P2의 기초).

워커 로그의 타임스탬프 출처가 둘이다.
  호스트 시계: t0_host(리플레이어), consumed_at, applied_at(워커)
  컨테이너(Docker VM) 시계: source.ts_ms(Postgres 커밋), ts_ms(Debezium 처리), Kafka 메시지 timestamp
Docker Desktop의 VM 시계는 호스트와 수십 초 어긋날 수 있어서, run 시작 때 잰 오프셋(meta.json clock_offset_s,
= VM − host)을 빼서 호스트 시계로 맞춘다.

    python -m pipeline.latency A
"""
from __future__ import annotations

import argparse
import json
import time

import numpy as np
import psycopg

from . import config as C


def measure_clock_offset(n: int = 15) -> dict:
    """VM(Postgres) 시계 − 호스트 시계 (초). 왕복이 가장 짧은 표본을 쓴다."""
    best = None
    with psycopg.connect(C.PG_DSN, autocommit=True) as conn, conn.cursor() as cur:
        for _ in range(n):
            t_send = time.time()
            cur.execute("SELECT extract(epoch FROM clock_timestamp())")
            pg = float(cur.fetchone()[0])
            t_recv = time.time()
            rtt = t_recv - t_send
            off = pg - (t_send + t_recv) / 2
            if best is None or rtt < best["rtt_s"]:
                best = {"clock_offset_s": off, "rtt_s": rtt}
    return best


def decompose(run_id: str) -> dict:
    run_dir = C.RUNS_DIR / run_id
    meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))
    off = meta.get("clock_offset_s", 0.0)
    rows = [json.loads(l) for l in open(run_dir / "worker_log.jsonl", encoding="utf-8")]
    rows = [r for r in rows if r.get("src_ts_ms") and r.get("kafka_ts_ms") and r["kafka_ts_ms"] > 0]
    t0 = np.array([r["t0_host"] for r in rows])
    con = np.array([r["consumed_at"] for r in rows])
    app = np.array([r["applied_at"] for r in rows])

    # 실측: VM 시계가 호스트와 맞는 값과 off 만큼 어긋난 값 사이를 오간다 (같은 행 안에서도 필드마다 다름).
    # off(수십 초)는 Kafka 적재까지의 지연(수 초 이하)보다 훨씬 커서, t0에 더 가까운 쪽을 고르면 모호하지 않다.
    n_shifted = {}

    def fix(key: str) -> np.ndarray:
        raw = np.array([r[key] / 1000 for r in rows])
        # Postgres 커밋·Debezium·Kafka 적재는 t0 직후(수 초 안)에 일어난다. 워커 대기열이 길어도 마찬가지.
        use_off = np.abs(raw - off - t0) < np.abs(raw - t0)
        n_shifted[key] = int(use_off.sum())
        return np.where(use_off, raw - off, raw)

    src, dbz, kaf = fix("src_ts_ms"), fix("dbz_ts_ms"), fix("kafka_ts_ms")
    stages = {
        "insert_to_pg_commit": src - t0,
        "pg_commit_to_debezium": dbz - src,
        "debezium_to_kafka": kaf - dbz,
        "kafka_to_consumed": con - kaf,
        "consumed_to_applied(embed_wait)": app - con,
        "total_t0_to_applied": app - t0,
    }
    out = {"run_id": run_id, "n": len(rows), "clock_offset_s": off, "rows_clock_corrected": n_shifted}
    for k, v in stages.items():
        out[k] = {"p50_ms": float(np.median(v) * 1000), "p90_ms": float(np.percentile(v, 90) * 1000),
                  "max_ms": float(v.max() * 1000)}
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_id", nargs="?")
    ap.add_argument("--offset-only", action="store_true")
    a = ap.parse_args()
    if a.offset_only or not a.run_id:
        print(json.dumps(measure_clock_offset(), indent=2))
        return
    d = decompose(a.run_id)
    print(f"run={d['run_id']} n={d['n']} clock_offset_s={d['clock_offset_s']:.3f}")
    for k, v in d.items():
        if isinstance(v, dict) and "p50_ms" in v:
            print(f"  {k:34s} p50={v['p50_ms']:8.0f}ms  p90={v['p90_ms']:8.0f}ms  max={v['max_ms']:8.0f}ms")
    (C.RUNS_DIR / a.run_id / "latency.json").write_text(json.dumps(d, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
