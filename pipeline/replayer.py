"""T-04: 부분집합을 원래 시간 간격 × (1/배속) 으로 Postgres에 재생한다.

INSERT 직전 호스트 시각을 t0_host 로 같이 넣는다. 평가(T-06)는 이 값을 편집 시각으로 쓴다.
리플레이 로그(run 디렉터리 replay_log.parquet)에 예정 시각·실제 시각을 남겨 간격 오차를 검증한다.

    python -m pipeline.replayer --run-id A --speed 1
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from . import config as C


def _parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--speed", type=float, default=1.0, help="배속. 2면 원래 간격의 1/2로 재생")
    ap.add_argument("--subset", default=str(C.SUBSET_PATH))
    ap.add_argument("--limit", type=int, default=0, help="앞에서부터 N건만 (0=전부)")
    a = ap.parse_args()

    rows = pq.read_table(a.subset).to_pylist()
    rows.sort(key=lambda r: (r["event_dt"], r["rev_id"]))
    if a.limit:
        rows = rows[:a.limit]
    dts = [_parse_dt(r["event_dt"]) for r in rows]
    sched = [(d - dts[0]).total_seconds() / a.speed for d in dts]

    run_dir = C.RUNS_DIR / a.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    log: list[dict] = []
    sql = ("INSERT INTO wiki_revisions (run_id, page_id, rev_id, parent_rev_id, page_title, event_dt, content, t0_host) "
           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id, replayed_at")
    print(f"[replayer] run={a.run_id} rows={len(rows)} speed={a.speed} span={sched[-1]:.1f}s", flush=True)
    with psycopg.connect(C.PG_DSN, autocommit=True) as conn, conn.cursor() as cur:
        start = time.perf_counter()
        for i, r in enumerate(rows):
            target = start + sched[i]
            while True:
                now = time.perf_counter()
                if now >= target:
                    break
                time.sleep(min(0.01, target - now))
            t0_host = time.time()
            actual = time.perf_counter() - start
            cur.execute(sql, (a.run_id, r["page_id"], r["rev_id"], r["parent_rev_id"], r["page_title"],
                              dts[i], r["content"], t0_host))
            rid, replayed_at = cur.fetchone()
            commit_host = time.time()
            log.append({"seq": i, "db_id": rid, "page_id": r["page_id"], "rev_id": r["rev_id"],
                        "event_dt": r["event_dt"], "scheduled_s": sched[i], "actual_s": actual,
                        "t0_host": t0_host, "commit_host": commit_host,
                        "pg_replayed_at": replayed_at.isoformat()})
            if (i + 1) % 100 == 0:
                print(f"[replayer] {i + 1}/{len(rows)} at {actual:.1f}s", flush=True)

    # 간격 오차: 연속 리비전 사이 실제 간격 - 예정 간격
    errs = [abs((log[i]["actual_s"] - log[i - 1]["actual_s"]) - (sched[i] - sched[i - 1])) * 1000
            for i in range(1, len(log))]
    max_err = max(errs) if errs else 0.0
    pq.write_table(pa.Table.from_pylist(log), run_dir / "replay_log.parquet")
    print(f"[replayer] done rows={len(log)} max_gap_error_ms={max_err:.1f} "
          f"mean_insert_ms={1000 * sum(l['commit_host'] - l['t0_host'] for l in log) / len(log):.1f}", flush=True)


if __name__ == "__main__":
    main()
