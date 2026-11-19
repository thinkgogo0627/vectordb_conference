"""실험 2 위키(P3.1) 입력: 레이크 임베딩을 '초기 인덱스 + 청크 업데이트 스트림'으로 바꾼다.

- 초기 인덱스: 문서마다 레이크에서 처음 보이는 리비전의 청크 전부
- 업데이트 스트림: 그 뒤 리비전을 편집 시각 순으로. 청크 자리(chunk_id)마다 현재 sha1과 다르면 upsert,
  새 리비전에서 사라진 자리면 delete. 같은 sha1이면 아무 일도 없다.
- 벡터: data/lake/emb/vectors (fp16) → fp32로 바꾸고 다시 L2 정규화

결과는 data/exp2/에 캐시한다 (stream.npz, vectors.f32.npy).

    python -m pipeline.exp2_data            # 캐시 생성 + 요약 출력
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

LAKE_EMB = Path("data/lake/emb")
LAKE_EDITS = Path("data/lake/edits.parquet")
OUT = Path("data/exp2")


def _load_vectors(emb: Path) -> tuple[np.ndarray, pd.DataFrame]:
    parts, keys = [], []
    for f in sorted((emb / "vectors").glob("*.parquet")):
        t = pq.read_table(f)
        col = t.column("vector").combine_chunks()
        dim = col.type.byte_width // 2
        buf = col.buffers()[1]
        arr = np.frombuffer(buf, dtype="<f2", count=len(col) * dim, offset=col.offset * dim * 2)
        parts.append(arr.reshape(-1, dim))
        keys.append(t.select(["page_id", "sha1"]).to_pandas())
    v = np.concatenate(parts).astype(np.float32)
    v /= np.linalg.norm(v, axis=1, keepdims=True)
    k = pd.concat(keys, ignore_index=True)
    k["vrow"] = np.arange(len(k), dtype=np.int64)
    return v, k


def build(emb: Path = LAKE_EMB, edits: Path = LAKE_EDITS, out: Path = OUT) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    vecs, key = _load_vectors(emb)
    rows = pq.read_table(emb / "revisions", columns=["page_id", "rev_id", "chunk_idx", "chunk_id", "sha1"]).to_pandas()
    rows = rows.merge(key, on=["page_id", "sha1"], how="left")
    if rows.vrow.isna().any():
        raise SystemExit(f"vector missing for {rows.vrow.isna().sum()} chunk rows")
    ts = pq.read_table(edits, columns=["page_id", "rev_id", "event_dt"]).to_pandas().drop_duplicates(["page_id", "rev_id"])
    ts["event_dt"] = pd.to_datetime(ts.event_dt, utc=True)
    revs = rows[["page_id", "rev_id"]].drop_duplicates().merge(ts, on=["page_id", "rev_id"], how="left")
    revs = revs.sort_values(["page_id", "rev_id"])
    revs["k"] = revs.groupby("page_id").cumcount()
    first = revs[revs.k == 0][["page_id", "rev_id"]]
    later = revs[revs.k > 0].sort_values(["event_dt", "rev_id"]).reset_index(drop=True)

    init = rows.merge(first, on=["page_id", "rev_id"]).sort_values("chunk_id")
    cur_sha = dict(zip(init.chunk_id.to_numpy(), init.sha1.to_numpy()))
    cur_n = init.groupby("page_id").chunk_idx.max().add(1).to_dict()

    by_rev = {k: g for k, g in rows.groupby(["page_id", "rev_id"], sort=False)}
    op_edit, op_chunk, op_vrow = [], [], []
    for ei, (p, r) in enumerate(zip(later.page_id.to_numpy(), later.rev_id.to_numpy())):
        g = by_rev[(p, r)]
        for cid, sha, vrow in zip(g.chunk_id.to_numpy(), g.sha1.to_numpy(), g.vrow.to_numpy()):
            if cur_sha.get(cid) != sha:
                cur_sha[cid] = sha
                op_edit.append(ei); op_chunk.append(cid); op_vrow.append(vrow)
        n_new = len(g)
        for idx in range(n_new, cur_n.get(p, 0)):          # 줄어든 자리 = delete
            cid = (int(p) << 16) | idx
            if cid in cur_sha:
                del cur_sha[cid]
                op_edit.append(ei); op_chunk.append(cid); op_vrow.append(-1)
        cur_n[p] = n_new

    np.save(out / "vectors.f32.npy", vecs)
    np.savez(out / "stream.npz",
             init_chunk=init.chunk_id.to_numpy(np.int64), init_vrow=init.vrow.to_numpy(np.int64),
             op_edit=np.array(op_edit, np.int32), op_chunk=np.array(op_chunk, np.int64),
             op_vrow=np.array(op_vrow, np.int64),
             edit_page=later.page_id.to_numpy(np.int64), edit_rev=later.rev_id.to_numpy(np.int64),
             edit_ts=(later.event_dt.dt.tz_localize(None).to_numpy().astype("datetime64[ms]").astype(np.int64)))
    op_vrow_a = np.array(op_vrow)
    summary = {"vectors": int(len(vecs)), "dim": int(vecs.shape[1]), "pages": int(len(first)),
               "init_chunks": int(len(init)), "edits": int(len(later)),
               "ops": int(len(op_vrow)), "upserts": int((op_vrow_a >= 0).sum()), "deletes": int((op_vrow_a < 0).sum()),
               "edits_with_ops": int(len(set(op_edit))),
               "first_edit": str(later.event_dt.min()), "last_edit": str(later.event_dt.max())}
    (out / "stream.meta.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


def load(out: Path = OUT) -> tuple[np.ndarray, dict]:
    if not (out / "stream.npz").exists():
        build(out=out)
    vecs = np.load(out / "vectors.f32.npy", mmap_mode="r")
    s = dict(np.load(out / "stream.npz"))
    return vecs, s


if __name__ == "__main__":
    print(json.dumps(build(), indent=2, ensure_ascii=False))
