"""T-06: 평가 하네스 (stale 판정). D-06 / GR-02.

리플레이 중 Postgres를 폴링해 새 편집을 받고, 편집마다 "이번 리비전에서 바뀐 청크"를 Postgres 최신 리비전(L)의
임베딩으로 FAISS에 top-1 질의한다.
  top-1 == 그 청크 & rev == L   → normal
  그 청크가 인덱스에 L보다 옛 rev로 있음 → stale
  그 청크가 인덱스에 없음        → missing (누락)
  그 청크는 L로 있는데 top-1이 다른 청크 → other (동일 벡터 충돌. 곡선에서는 해소로 본다)
  L에서 그 청크가 사라진 경우: 인덱스에서도 없으면 normal, 옛 벡터가 남아 있으면 stale
편집 상태 = 청크 중 stale이 하나라도 있으면 stale, 아니면 missing, 아니면 normal.
normal이 되면 그 편집은 해소(sync_elapsed = t0_host 기준 경과 초). max_wait 초 넘으면 timeout.

비용 절감 (판정 결과는 같다): 먼저 /chunk_revs로 청크가 인덱스에 어느 리비전으로 있는지만 본다.
없으면 missing, L보다 옛것이면 top-1과 무관하게 stale이므로 검색하지 않는다. L 이상일 때만 top-1 검색으로 확인한다.
판정 로그(judgments)는 편집의 상태가 바뀔 때만 남긴다.
base 리비전(is_base, 리플레이 직전 상태)은 최신 리비전 계산에만 쓰고 편집으로 세지 않는다.

    python -m pipeline.evaluate --run-id A --expected 450
"""
from __future__ import annotations

import argparse
import json
import time

import numpy as np
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from . import config as C
from .embstore import load_revisions


def judge_chunk(top1_id: int, top1_rev: int, want_rev: int, qc: int, latest: int) -> str:
    if want_rev == -1:
        return "missing"
    if top1_id == qc:
        return "normal" if top1_rev >= latest else "stale"
    return "stale" if want_rev < latest else "other"


def judge_edit(chunk_status: list[str]) -> str:
    if any(s == "stale" for s in chunk_status):
        return "stale"
    if any(s == "missing" for s in chunk_status):
        return "missing"
    return "normal"


class Evaluator:
    def __init__(self, run_id: str, embeddings: str, worker_url: str, poll_ms: int, max_wait: float):
        self.run_id = run_id
        self.worker_url = worker_url
        self.poll_s = poll_ms / 1000.0
        self.max_wait = max_wait
        # 벡터는 워커가 갖고 있다. 평가기는 (page, rev)별 청크 목록과 바뀐 청크만 안다.
        self.http = requests.Session()
        self.chunks: dict[tuple[int, int], set[int]] = {}
        self.changed: dict[tuple[int, int], list[int]] = {}
        for r in load_revisions(embeddings):
            key = (r["page_id"], r["rev_id"])
            self.chunks.setdefault(key, set()).add(r["chunk_id"])
            if r["changed"]:
                self.changed.setdefault(key, []).append(r["chunk_id"])
        self.latest: dict[int, int] = {}
        self.pending: list[dict] = []
        self.edits: list[dict] = []
        self.judgments: list[dict] = []
        self.last_id = 0
        self.n_search = 0
        self.n_polls = 0

    def fetch_new(self, cur) -> int:
        cur.execute("SELECT id, page_id, rev_id, t0_host, coalesce(is_base, false) FROM wiki_revisions "
                    "WHERE run_id=%s AND id>%s ORDER BY id", (self.run_id, self.last_id))
        n = 0
        for db_id, page_id, rev_id, t0, is_base in cur.fetchall():
            self.last_id = db_id
            self.latest[page_id] = max(self.latest.get(page_id, -1), rev_id)
            if is_base:
                continue
            e = {"db_id": db_id, "page_id": page_id, "rev_id": rev_id, "t0_host": t0,
                 "n_chunks_changed": len(self.changed.get((page_id, rev_id), [])),
                 "n_checks": 0, "first_check_elapsed": None, "first_status": None, "last_status": None,
                 "last_stale_elapsed": None, "sync_elapsed": None, "final_status": None}
            if e["n_chunks_changed"] == 0:
                e["final_status"] = "nochange"
                self.edits.append(e)
            else:
                self.pending.append(e)
            n += 1
        return n

    def check_pending(self) -> None:
        if not self.pending:
            return
        self.n_polls += 1
        meta = []   # (edit, qc, L, gone)
        for e in self.pending:
            p, r = e["page_id"], e["rev_id"]
            L = self.latest[p]
            for qc in self.changed[(p, r)]:
                gone = qc not in self.chunks.get((p, L), ())   # 최신 리비전에서 이 청크가 사라짐
                meta.append((e, qc, L, gone))
        t_check = time.time()
        wrevs = self.http.post(f"{self.worker_url}/chunk_revs", json={"ids": [m[1] for m in meta]},
                               timeout=30).json()["revs"]
        status = [None] * len(meta)
        to_search = []
        for i, ((e, qc, L, gone), wrev) in enumerate(zip(meta, wrevs)):
            if gone:
                status[i] = "normal" if wrev == -1 else "stale"
            elif wrev == -1:
                status[i] = "missing"
            elif wrev < L:
                status[i] = "stale"
            else:
                to_search.append(i)
        if to_search:
            keys = [[meta[i][0]["page_id"], meta[i][2], meta[i][1]] for i in to_search]
            resp = self.http.post(f"{self.worker_url}/search",
                                  json={"keys": keys, "k": 1, "want_ids": [meta[i][1] for i in to_search]},
                                  timeout=30).json()
            self.n_search += len(to_search)
            for i, ids, revs, wrev in zip(to_search, resp["ids"], resp["revs"], resp["want_revs"]):
                _e, qc, L, _g = meta[i]
                status[i] = judge_chunk(ids[0], revs[0], wrev, qc, L)
        per_edit: dict[int, list[str]] = {}
        for (e, _qc, _L, _g), s in zip(meta, status):
            per_edit.setdefault(e["db_id"], []).append(s)
        still = []
        for e in self.pending:
            st = judge_edit(per_edit[e["db_id"]])
            elapsed = t_check - e["t0_host"]
            e["n_checks"] += 1
            if e["first_check_elapsed"] is None:
                e["first_check_elapsed"] = elapsed
                e["first_status"] = st
            if st != e["last_status"]:
                self.judgments.append({"db_id": e["db_id"], "page_id": e["page_id"], "rev_id": e["rev_id"],
                                       "t_check": t_check, "elapsed": elapsed, "status": st})
                e["last_status"] = st
            if st == "normal":
                e["sync_elapsed"] = elapsed
                e["final_status"] = "normal"
                self.edits.append(e)
            elif elapsed > self.max_wait:
                e["final_status"] = f"timeout_{st}"
                self.edits.append(e)
            else:
                e["last_stale_elapsed"] = elapsed
                still.append(e)
        self.pending = still

    def run(self, expected: int, run_dir) -> None:
        seen = 0
        t_last_report = 0.0
        with psycopg.connect(C.PG_DSN, autocommit=True) as conn, conn.cursor() as cur:
            while True:
                seen += self.fetch_new(cur)
                self.check_pending()
                if seen >= expected and not self.pending:
                    break
                now = time.time()
                if now - t_last_report > 10:
                    t_last_report = now
                    print(f"[evaluate] seen={seen}/{expected} pending={len(self.pending)} resolved={len(self.edits)}",
                          flush=True)
                time.sleep(self.poll_s)
        pq.write_table(pa.Table.from_pylist(self.edits), run_dir / "edits.parquet")
        pq.write_table(pa.Table.from_pylist(self.judgments), run_dir / "judgments.parquet")
        synced = [e["sync_elapsed"] for e in self.edits if e["final_status"] == "normal"]
        counts: dict[str, int] = {}
        for e in self.edits:
            counts[e["final_status"]] = counts.get(e["final_status"], 0) + 1
        print(f"[evaluate] done edits={len(self.edits)} status={counts} polls={self.n_polls} searches={self.n_search} "
              f"sync_s median={np.median(synced) if synced else float('nan'):.2f} "
              f"p90={np.percentile(synced, 90) if synced else float('nan'):.2f} "
              f"max={max(synced) if synced else float('nan'):.2f}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--expected", type=int, required=True, help="이 run에서 들어올 편집 수 (base 제외)")
    ap.add_argument("--embeddings", default=str(C.EMBED_PATH))
    ap.add_argument("--worker-url", default=f"http://127.0.0.1:{C.WORKER_PORT}")
    ap.add_argument("--poll-ms", type=int, default=50)
    ap.add_argument("--max-wait", type=float, default=120.0)
    a = ap.parse_args()
    run_dir = C.RUNS_DIR / a.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    ev = Evaluator(a.run_id, a.embeddings, a.worker_url, a.poll_ms, a.max_wait)
    ev.run(a.expected, run_dir)


if __name__ == "__main__":
    main()
