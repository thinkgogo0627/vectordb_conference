"""T-06 완료 기준 검증: 워커를 일부러 멈추면 stale, 반영 후 normal 로 판정되는지 실제 경로로 확인한다.

  1. 워커 시작 → 문서 N개의 첫 리비전 INSERT → FAISS 반영 대기
  2. 워커 일시정지(/pause) → 같은 문서의 둘째 리비전 INSERT → 3초간 판정: 전부 stale 이어야 함
  3. 워커 재개(/resume) → 전부 normal 이 되어야 함

    python -m pipeline.check_t06 [--embeddings data/exp0/embeddings.parquet]
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

import psycopg
import pyarrow.parquet as pq
import requests

from . import config as C
from .evaluate import Evaluator
from .register_connector import register

SQL = ("INSERT INTO wiki_revisions (run_id, page_id, rev_id, parent_rev_id, page_title, event_dt, content, t0_host) "
       "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--embeddings", default=str(C.EMBED_PATH))
    ap.add_argument("--pages", type=int, default=10)
    a = ap.parse_args()
    run_id = "t06-" + datetime.now(timezone.utc).strftime("%H%M%S")
    rows = sorted(pq.read_table(C.SUBSET_PATH).to_pylist(), key=lambda r: r["rev_id"])
    by_page: dict[int, list[dict]] = {}
    for r in rows:
        by_page.setdefault(r["page_id"], []).append(r)
    pages = [p for p, rs in by_page.items() if len(rs) >= 2][:a.pages]
    first = [by_page[p][0] for p in pages]
    second = [by_page[p][1] for p in pages]

    register()
    (C.RUNS_DIR / run_id).mkdir(parents=True, exist_ok=True)
    url = f"http://127.0.0.1:{C.WORKER_PORT}"
    worker = subprocess.Popen([sys.executable, "-m", "pipeline.worker", "--run-id", run_id, "--embeddings", a.embeddings],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=dict(os.environ, PYTHONUTF8="1"))
    ok = True
    try:
        for _ in range(120):
            try:
                if requests.get(f"{url}/health", timeout=2).json().get("assigned"):
                    break
            except requests.RequestException:
                pass
            time.sleep(0.5)
        else:
            raise SystemExit("worker not ready")
        time.sleep(1)

        def insert(batch):
            with psycopg.connect(C.PG_DSN, autocommit=True) as conn:
                for r in batch:
                    conn.execute(SQL, (run_id, r["page_id"], r["rev_id"], r["parent_rev_id"], r["page_title"],
                                       datetime.fromisoformat(r["event_dt"].replace("Z", "+00:00")), r["content"], time.time()))

        insert(first)
        for _ in range(600):   # 첫 리비전은 청크 전부를 임베딩하므로 embed_ms 대기가 길다
            if requests.get(f"{url}/health", timeout=2).json()["applied"] >= len(first):
                break
            time.sleep(0.2)
        else:
            raise SystemExit("first revisions never applied")
        print(f"[1] 첫 리비전 {len(first)}건 반영됨")

        requests.post(f"{url}/pause", timeout=5)
        insert(second)
        ev = Evaluator(run_id, a.embeddings, url, poll_ms=100, max_wait=60)
        with psycopg.connect(C.PG_DSN, autocommit=True) as conn, conn.cursor() as cur:
            t_end = time.time() + 3
            while time.time() < t_end:
                ev.fetch_new(cur)
                ev.check_pending()
                time.sleep(0.1)
            paused_status = {j["status"] for j in ev.judgments}
            n_pending = len(ev.pending)
            print(f"[2] 워커 정지 중 판정: {sorted(paused_status)} (판정 {len(ev.judgments)}회, 미해소 편집 {n_pending}건)")
            ok &= paused_status == {"stale"} and n_pending == 2 * len(pages)

            n_before = len(ev.judgments)
            requests.post(f"{url}/resume", timeout=5)
            t_end = time.time() + 30
            while ev.pending and time.time() < t_end:
                ev.check_pending()
                time.sleep(0.1)
            final = {e["final_status"] for e in ev.edits}
            print(f"[3] 워커 재개 후 최종: {sorted(final)} (미해소 {len(ev.pending)}건, 추가 판정 {len(ev.judgments) - n_before}회)")
            ok &= final == {"normal"} and not ev.pending
    finally:
        worker.terminate()
    print("T-06 검증:", "충족" if ok else "미충족")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
