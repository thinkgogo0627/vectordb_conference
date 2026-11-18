"""사전 계산 임베딩 읽기. 두 형식을 받는다.

- 디렉터리 (pipeline.embed 현재 형식): revisions/*.parquet + vectors/*.parquet + meta.json
- 파일 하나 (실험 0 합성 데이터 때의 옛 형식): 행마다 vector(float32 list)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


def _is_dir(path) -> bool:
    return Path(path).is_dir()


def load_revisions(path) -> list[dict]:
    """리비전×청크 행: page_id, rev_id, chunk_id, changed, embed_ms, is_base"""
    if _is_dir(path):
        cols = ["page_id", "rev_id", "chunk_id", "changed", "embed_ms", "is_base", "sha1"]
        return pq.read_table(Path(path) / "revisions", columns=cols).to_pylist()
    rows = pq.read_table(path, columns=["page_id", "rev_id", "chunk_id", "changed", "embed_ms"]).to_pylist()
    for r in rows:
        r["is_base"] = False
    return rows


def load_by_rev(path):
    """returns dim, {(page_id, rev_id): {"embed_ms", "is_base", "chunks": [(chunk_id, vec float32)]}}"""
    by_rev: dict[tuple[int, int], dict] = {}
    if _is_dir(path):
        vt = pq.read_table(Path(path) / "vectors")
        dim = vt.schema.field("vector").type.byte_width // 2
        raw = b"".join(vt.column("vector").to_pylist())
        mat = np.frombuffer(raw, dtype="<f2").reshape(-1, dim).astype(np.float32)
        key = {(p, s): i for i, (p, s) in enumerate(zip(vt.column("page_id").to_pylist(),
                                                       vt.column("sha1").to_pylist()))}
        for r in load_revisions(path):
            d = by_rev.setdefault((r["page_id"], r["rev_id"]),
                                  {"embed_ms": r["embed_ms"], "is_base": r["is_base"], "chunks": []})
            d["chunks"].append((r["chunk_id"], mat[key[(r["page_id"], r["sha1"])]]))
        return dim, by_rev
    t = pq.read_table(path)
    dim = t.schema.field("vector").type.list_size
    cols = {k: t.column(k).to_pylist() for k in ("page_id", "rev_id", "chunk_id", "embed_ms")}
    vecs = np.asarray(t.column("vector").combine_chunks().flatten().to_numpy(zero_copy_only=False),
                      dtype=np.float32).reshape(-1, dim)
    for i in range(t.num_rows):
        d = by_rev.setdefault((cols["page_id"][i], cols["rev_id"][i]),
                              {"embed_ms": cols["embed_ms"][i], "is_base": False, "chunks": []})
        d["chunks"].append((cols["chunk_id"][i], vecs[i]))
    return dim, by_rev
