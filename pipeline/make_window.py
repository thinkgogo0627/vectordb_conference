"""실험 1 리플레이 입력: 레이크에서 시간 구간 하나를 그대로 자르고, 각 문서의 '구간 직전 상태'(base)를 붙인다.

- 구간 안의 편집(create/update, 본문 있음)은 원래 시각 그대로 리플레이 대상 (is_base=False)
- 문서마다 구간 안 첫 편집의 parent_rev_id 리비전이 base (is_base=True). 리플레이 전에 Postgres와 FAISS에
  미리 넣어 두고 재지 않는다. 그래야 첫 편집도 "옛 버전이 검색되는" 정상적인 stale로 판정된다.
  base 본문은 레이크에 있으면 그것을, 없으면 MediaWiki Action API에서 받는다 (수집기와 같은 방식).
- 새 문서(create)는 base 없음 → 반영 전까지 '누락'으로 판정된다.

    python -m pipeline.make_window --start 2026-11-16T00:00:00 --hours 6 --out data/exp1/subset.parquet
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .make_subset import OUT_SCHEMA

SCHEMA = OUT_SCHEMA.append(pa.field("is_base", pa.bool_()))
# Wikimedia API 정책상 식별 가능한 User-Agent를 쓴다. 개인 연락처는 넣지 않는다.
USER_AGENT = "ybigta-vectordb-gap-experiment/0.1 (research replay; python-requests)"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default="data/lake/edits.parquet")
    ap.add_argument("--start", required=True, help="UTC, 예: 2026-11-16T00:00:00")
    ap.add_argument("--hours", type=float, default=6)
    ap.add_argument("--domain", default="ko.wikipedia.org")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute(f"CREATE TABLE lake AS SELECT * FROM read_parquet('{a.lake}')")
    end = con.execute(f"SELECT strftime(strptime('{a.start}', '%Y-%m-%dT%H:%M:%S') + INTERVAL ({a.hours} * 3600) SECOND,"
                      f" '%Y-%m-%dT%H:%M:%S')").fetchone()[0]
    win = con.execute(f"SELECT * FROM lake WHERE event_dt >= '{a.start}' AND event_dt < '{end}' "
                      f"ORDER BY event_dt, rev_id").to_arrow_table().to_pylist()
    first: dict[int, dict] = {}
    for r in win:
        first.setdefault(r["page_id"], r)

    bases: list[dict] = []
    need_api: dict[int, dict] = {}
    stats = {"window_edits": len(win), "pages": len(first), "creates": 0, "base_from_lake": 0,
             "base_from_api": 0, "base_missing": 0}
    for pid, r in first.items():
        parent = r["parent_rev_id"]
        if r["op"] == "create" or not parent:
            stats["creates"] += 1
            continue
        hit = con.execute("SELECT * FROM lake WHERE rev_id = ?", [parent]).to_arrow_table().to_pylist()
        if hit:
            b = dict(hit[0])
            b.update(op="base", is_base=True)
            bases.append(b)
            stats["base_from_lake"] += 1
        else:
            need_api[parent] = r

    if need_api:
        from collector.enrich import Enricher

        enr = Enricher(USER_AGENT, fetch_content=True)
        ids = list(need_api)
        for i in range(0, len(ids), 50):
            got = enr.fetch(a.domain, ids[i:i + 50])
            for rid in ids[i:i + 50]:
                info = got.get(rid) or {}
                child = need_api[rid]
                if info.get("content_status") != "ok" or info.get("content") is None:
                    stats["base_missing"] += 1
                    continue
                bases.append({"page_id": child["page_id"], "rev_id": rid, "parent_rev_id": None,
                              "page_title": child["page_title"],
                              "event_dt": (info.get("rev_timestamp") or "").replace("Z", ".000Z"),
                              "content": info["content"], "op": "base", "is_bot": None, "is_base": True})
                stats["base_from_api"] += 1
            time.sleep(1.0)  # API 예의상 간격
            print(f"  api {min(i + 50, len(ids))}/{len(ids)}", flush=True)

    rows = [dict(r, is_base=False) for r in win] + bases
    rows.sort(key=lambda r: (not r["is_base"], r["event_dt"] or "", r["rev_id"]))
    t = pa.Table.from_pylist([{k: r.get(k) for k in SCHEMA.names} for r in rows], schema=SCHEMA)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(t, a.out, compression="zstd")
    stats.update(start=a.start, end=end, hours=a.hours, bases=len(bases), out=a.out)
    Path(a.out).with_suffix(".meta.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(stats, ensure_ascii=False))


if __name__ == "__main__":
    main()
