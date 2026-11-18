"""구간별 지연 분해 (GR-03 해석용, 실험 1 P2).

타임스탬프 출처가 둘이다.
  호스트 시계: t0_host·commit_host(리플레이어), consumed_at·embed_start·embed_done·applied_at(워커)
  Docker VM 시계: source.ts_ms(Postgres 커밋), ts_ms(Debezium 처리), Kafka 메시지 timestamp(Connect 프로듀서)
VM 시계는 호스트와 어긋나고 튀기도 한다(실측: 50초, 이후 ±1초 수준). 그래서 서로 다른 시계를 빼지 않는다.
  insert_to_pg_commit   = commit_host - t0_host            (호스트끼리)
  pg_commit_to_debezium = dbz_ts - src_ts                   (VM끼리)
  debezium_to_kafka     = kafka_ts - dbz_ts                 (VM끼리)
  kafka_to_consumed     = (consumed - t0) - 위 세 구간       (나머지)
  queue_or_batch_wait / embed_wait / faiss_apply           (호스트끼리, 실험 1 워커)
시계가 튀는 순간에 걸친 이벤트는 VM 구간 값이 틀어지므로 중앙값 위주로 본다.

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
    import pyarrow.parquet as pq

    run_dir = C.RUNS_DIR / run_id
    meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))
    commit = {r["rev_id"]: r["commit_host"] for r in
              pq.read_table(run_dir / "replay_log.parquet", columns=["rev_id", "commit_host"]).to_pylist()}
    rows = [json.loads(l) for l in open(run_dir / "worker_log.jsonl", encoding="utf-8")]
    rows = [r for r in rows if r.get("src_ts_ms") and r.get("kafka_ts_ms") and r["kafka_ts_ms"] > 0
            and r.get("result") != "no_embedding" and r["rev_id"] in commit]
    t0 = np.array([r["t0_host"] for r in rows])
    cm = np.array([commit[r["rev_id"]] for r in rows])
    con = np.array([r["consumed_at"] for r in rows])
    app = np.array([r["applied_at"] for r in rows])
    src = np.array([r["src_ts_ms"] / 1000 for r in rows])
    dbz = np.array([r["dbz_ts_ms"] / 1000 for r in rows])
    kaf = np.array([r["kafka_ts_ms"] / 1000 for r in rows])
    ins, s_dbz, s_kaf = cm - t0, dbz - src, kaf - dbz
    stages = {
        "insert_to_pg_commit": ins,
        "pg_commit_to_debezium": s_dbz,
        "debezium_to_kafka": s_kaf,
        "kafka_to_consumed": (con - t0) - ins - s_dbz - s_kaf,
    }
    if all("embed_start" in r for r in rows):   # 실험 1 워커: 대기열·묶음 대기 / 임베딩 / FAISS 반영
        es = np.array([r["embed_start"] for r in rows])
        ed = np.array([r["embed_done"] for r in rows])
        stages["queue_or_batch_wait"] = es - con
        stages["embed_wait"] = ed - es
        stages["faiss_apply"] = app - ed
    else:
        stages["consumed_to_applied(embed_wait)"] = app - con
    stages["total_t0_to_applied"] = app - t0
    out = {"run_id": run_id, "n": len(rows), "vm_clock_offset_at_start_s": meta.get("clock_offset_s"),
           "method": "same-clock differences only"}
    for k, v in stages.items():
        out[k] = {"p50_ms": float(np.median(v) * 1000), "p90_ms": float(np.percentile(v, 90) * 1000),
                  "mean_ms": float(np.mean(v) * 1000), "max_ms": float(v.max() * 1000)}
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
    print(f"run={d['run_id']} n={d['n']} vm_offset_at_start={d['vm_clock_offset_at_start_s']}")
    for k, v in d.items():
        if isinstance(v, dict) and "p50_ms" in v:
            print(f"  {k:34s} p50={v['p50_ms']:8.0f}ms  p90={v['p90_ms']:8.0f}ms  max={v['max_ms']:8.0f}ms")
    (C.RUNS_DIR / a.run_id / "latency.json").write_text(json.dumps(d, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
