"""T-02: 청크 분할 + 임베딩 사전 계산 → data/exp0/embeddings.parquet

청크 규칙: 본문을 빈 줄 기준 문단으로 나누고, 문단을 앞에서부터 CHUNK_CHARS(기본 800자) 이하로
묶는다. 한 문단이 그보다 길면 CHUNK_CHARS 단위로 자른다. chunk_idx는 0부터 순서대로.
청크 키 = chunk_id(page_id, chunk_idx) + rev_id.

embed_ms: 리비전마다 "직전 리비전과 달라진 청크"만 실제로 인코딩한 시간(ms). 워커(T-05)가 이 시간만큼
기다린 뒤 반영한다 (D-03). 안 바뀐 청크는 직전 리비전 벡터를 그대로 쓴다 (changed=false, 시간 0).

    python -m pipeline.embed [--model BAAI/bge-m3]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from . import config as C


def chunk_text(content: str, max_chars: int = C.CHUNK_CHARS) -> list[str]:
    paras = [p.strip() for p in content.replace("\r\n", "\n").split("\n\n")]
    paras = [p for p in paras if p]
    chunks: list[str] = []
    cur = ""
    for p in paras:
        if len(p) > max_chars:
            if cur:
                chunks.append(cur)
                cur = ""
            chunks.extend(p[i:i + max_chars] for i in range(0, len(p), max_chars))
            continue
        if cur and len(cur) + 2 + len(p) > max_chars:
            chunks.append(cur)
            cur = p
        else:
            cur = f"{cur}\n\n{p}" if cur else p
    if cur:
        chunks.append(cur)
    return chunks or [""]


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def load_model(name: str):
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(name, device="cpu")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=C.EMBED_MODEL)
    ap.add_argument("--subset", default=str(C.SUBSET_PATH))
    ap.add_argument("--out", default=str(C.EMBED_PATH))
    ap.add_argument("--chunk-chars", type=int, default=C.CHUNK_CHARS)
    a = ap.parse_args()

    subset = pq.read_table(a.subset).to_pylist()
    model = load_model(a.model)
    dim = int(model.get_sentence_embedding_dimension())
    # 예열 (첫 호출의 지연이 embed_ms에 섞이지 않게)
    model.encode(["warm up"], normalize_embeddings=True)

    prev: dict[int, dict[int, tuple[str, np.ndarray]]] = {}  # page_id -> chunk_idx -> (sha1, vec)
    rows: list[dict] = []
    t_all = time.perf_counter()
    for i, r in enumerate(subset):
        page_id, rev_id = int(r["page_id"]), int(r["rev_id"])
        texts = chunk_text(r["content"] or "", a.chunk_chars)
        shas = [_sha1(t) for t in texts]
        old = prev.get(page_id, {})
        changed_idx = [k for k, s in enumerate(shas) if old.get(k, (None,))[0] != s]
        embed_ms = 0.0
        new_vecs: dict[int, np.ndarray] = {}
        if changed_idx:
            t0 = time.perf_counter()
            vecs = model.encode([texts[k] for k in changed_idx], normalize_embeddings=True,
                                batch_size=16, convert_to_numpy=True)
            embed_ms = (time.perf_counter() - t0) * 1000.0
            new_vecs = {k: np.asarray(v, dtype=np.float32) for k, v in zip(changed_idx, vecs)}
        cur: dict[int, tuple[str, np.ndarray]] = {}
        for k, s in enumerate(shas):
            vec = new_vecs[k] if k in new_vecs else old[k][1]
            cur[k] = (s, vec)
            rows.append({
                "page_id": page_id, "rev_id": rev_id, "chunk_idx": k,
                "chunk_id": C.chunk_id(page_id, k), "sha1": s,
                "changed": k in new_vecs, "embed_ms": embed_ms, "vector": vec.tolist(),
            })
        prev[page_id] = cur
        if (i + 1) % 50 == 0:
            print(f"  {i + 1}/{len(subset)} revs, {len(rows)} chunks, {time.perf_counter() - t_all:.0f}s")

    schema = pa.schema([
        ("page_id", pa.int64()), ("rev_id", pa.int64()), ("chunk_idx", pa.int32()),
        ("chunk_id", pa.int64()), ("sha1", pa.string()), ("changed", pa.bool_()),
        ("embed_ms", pa.float64()), ("vector", pa.list_(pa.float32(), dim)),
    ])
    table = pa.Table.from_pylist(rows, schema=schema)
    C.DATA_DIR.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, a.out, compression="zstd")
    meta = {"model": a.model, "dim": dim, "chunk_chars": a.chunk_chars,
            "revs": len(subset), "chunks": len(rows), "changed_chunks": int(sum(r["changed"] for r in rows)),
            "embed_ms_per_rev": {
                "mean": float(np.mean([r["embed_ms"] for r in rows if r["chunk_idx"] == 0])),
                "p50": float(np.median([r["embed_ms"] for r in rows if r["chunk_idx"] == 0])),
                "max": float(max(r["embed_ms"] for r in rows)),
            }}
    Path(a.out).with_suffix(".meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print("embeddings:", json.dumps(meta, ensure_ascii=False))


if __name__ == "__main__":
    main()
