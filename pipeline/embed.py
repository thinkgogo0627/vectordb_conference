"""T-02: 청크 분할 + bge-m3 임베딩 사전 계산.

청크 규칙: 본문을 빈 줄 기준 문단으로 나누고, 문단을 앞에서부터 CHUNK_CHARS(기본 800자) 이하로
묶는다. 한 문단이 그보다 길면 CHUNK_CHARS 단위로 자른다. chunk_idx는 0부터 순서대로 (위치 기준 키).
청크 키 = chunk_id(page_id, chunk_idx) + rev_id.

벡터 재사용: 같은 문서 직전 리비전에 똑같은 텍스트(sha1)의 청크가 있으면 위치가 달라도 그 벡터를 쓴다.
embed_ms = 그 리비전에서 새로 생긴 텍스트만 실제로 인코딩한 시간(ms). 워커(T-05)가 이만큼 기다린다 (D-03).
2026-11-18부터 기준 장비는 GPU (RTX 3060 Laptop, fp16)다 (사용자 결정).

출력은 디렉터리 하나:
  revisions.parquet   page_id, rev_id, chunk_idx, chunk_id, sha1, changed(위치 기준으로 직전과 다름),
                      new_text(이 리비전에서 인코딩함), n_new, embed_ms, is_base
  vectors/part-*.parquet  page_id, sha1, vector(fixed_size_binary, float16 little-endian)
  meta.json

    python -m pipeline.embed --subset data/exp1/subset.parquet --out data/exp1/emb --device cuda --fp16
    python -m pipeline.embed --subset data/lake/edits.parquet --out data/lake/emb --device cuda --fp16 --resume
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import config as C

REV_SCHEMA = pa.schema([
    ("page_id", pa.int64()), ("rev_id", pa.int64()), ("chunk_idx", pa.int32()), ("chunk_id", pa.int64()),
    ("sha1", pa.string()), ("changed", pa.bool_()), ("new_text", pa.bool_()),
    ("n_new", pa.int32()), ("embed_ms", pa.float64()), ("is_base", pa.bool_()),
])


def vec_schema(dim: int) -> pa.Schema:
    return pa.schema([("page_id", pa.int64()), ("sha1", pa.string()), ("vector", pa.binary(dim * 2))])


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


class Encoder:
    """bge-m3 dense = [CLS] 은닉 상태를 L2 정규화 (sentence-transformers 설정과 같음)."""

    def __init__(self, name: str, device: str, fp16: bool, batch_size: int = 16):
        import torch
        from transformers import AutoModel, AutoTokenizer

        self.torch = torch
        self.device = device
        self.bs = batch_size
        self.tok = AutoTokenizer.from_pretrained(name)
        dtype = torch.float16 if fp16 else torch.float32
        self.model = AutoModel.from_pretrained(name, dtype=dtype).to(device).eval()
        self.dim = int(self.model.config.hidden_size)

    def encode(self, texts: list[str]) -> np.ndarray:
        torch = self.torch
        outs = []
        with torch.inference_mode():
            for i in range(0, len(texts), self.bs):
                enc = self.tok(texts[i:i + self.bs], padding=True, truncation=True, max_length=8192,
                               return_tensors="pt").to(self.device)
                h = self.model(**enc).last_hidden_state[:, 0]
                outs.append(torch.nn.functional.normalize(h.float(), dim=-1).cpu())
            if self.device == "cuda":
                torch.cuda.synchronize()
        return torch.cat(outs).numpy().astype(np.float32)


class _PartWriter:
    def __init__(self, out_dir: Path, schema: pa.Schema, prefix: str, start_idx: int, rows_per_part: int):
        self.dir, self.schema, self.prefix = out_dir, schema, prefix
        self.idx, self.rows_per_part = start_idx, rows_per_part
        self.buf: list[dict] = []

    def add(self, row: dict) -> None:
        self.buf.append(row)

    def flush(self, force: bool = False) -> bool:
        if not self.buf or (not force and len(self.buf) < self.rows_per_part):
            return False
        pq.write_table(pa.Table.from_pylist(self.buf, schema=self.schema),
                       self.dir / f"{self.prefix}-{self.idx:05d}.parquet", compression="zstd")
        self.idx += 1
        self.buf = []
        return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=C.EMBED_MODEL)
    ap.add_argument("--subset", default=str(C.SUBSET_PATH))
    ap.add_argument("--out", required=True, help="출력 디렉터리")
    ap.add_argument("--chunk-chars", type=int, default=C.CHUNK_CHARS)
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--fp16", action="store_true")
    ap.add_argument("--batch-size", type=int, default=16)
    ap.add_argument("--rows-per-part", type=int, default=50_000)
    ap.add_argument("--resume", action="store_true", help="progress.json 이후 문서부터 이어서")
    a = ap.parse_args()

    out = Path(a.out)
    progress_p = out / "progress.json"
    progress = {"last_page_id": None, "vec_part": 0, "rev_part": 0, "revs": 0, "vectors": 0, "elapsed_s": 0.0}
    if a.resume and progress_p.exists():
        progress = json.loads(progress_p.read_text(encoding="utf-8"))
        print(f"[embed] resume after page {progress['last_page_id']}", flush=True)
    else:
        if out.exists():
            shutil.rmtree(out)
    (out / "vectors").mkdir(parents=True, exist_ok=True)
    (out / "revisions").mkdir(parents=True, exist_ok=True)

    table = pq.read_table(a.subset)
    if "is_base" not in table.column_names:
        table = table.append_column("is_base", pa.array([False] * table.num_rows))
    table = table.sort_by([("page_id", "ascending"), ("rev_id", "ascending")])
    if progress["last_page_id"] is not None:
        table = table.filter(pc.greater(table["page_id"], progress["last_page_id"]))
    n_total = table.num_rows

    enc = Encoder(a.model, a.device, a.fp16, a.batch_size)
    enc.encode(["warm up"] * 4)  # 예열 (첫 호출 지연이 embed_ms에 섞이지 않게)
    dim = enc.dim
    vw = _PartWriter(out / "vectors", vec_schema(dim), "part", progress["vec_part"], a.rows_per_part)
    rw = _PartWriter(out / "revisions", REV_SCHEMA, "part", progress["rev_part"], a.rows_per_part * 4)

    t_start = time.perf_counter()
    prev_page = None
    prev_pos: list[str] = []
    prev_vecs: dict[str, bool] = {}   # 직전 리비전에 있던 sha1 (벡터는 이미 저장됨)
    page_seen: set[str] = set()        # 이 문서에서 이미 저장한 sha1
    done_rows = 0

    elapsed_before = progress["elapsed_s"]   # 이전 실행(--resume)까지의 누적

    def checkpoint(last_page):
        rw.flush(force=True)
        vw.flush(force=True)
        progress.update(last_page_id=last_page, vec_part=vw.idx, rev_part=rw.idx,
                        elapsed_s=elapsed_before + (time.perf_counter() - t_start))
        progress_p.write_text(json.dumps(progress), encoding="utf-8")

    for batch in table.to_batches(max_chunksize=2000):
        for r in batch.to_pylist():
            page_id, rev_id = int(r["page_id"]), int(r["rev_id"])
            if page_id != prev_page:
                if prev_page is not None and (vw.flush() or rw.flush()):
                    # 파트를 쓴 직후 = 문서 경계. 여기까지를 재개 지점으로 남긴다.
                    checkpoint(prev_page)
                prev_page, prev_pos, prev_vecs, page_seen = page_id, [], {}, set()
            texts = chunk_text(r["content"] or "", a.chunk_chars)
            shas = [_sha1(t) for t in texts]
            new_idx = []
            seen_now = set()
            for k, s in enumerate(shas):
                if s not in prev_vecs and s not in page_seen and s not in seen_now:
                    new_idx.append(k)
                    seen_now.add(s)
            embed_ms = 0.0
            if new_idx:
                t0 = time.perf_counter()
                vecs = enc.encode([texts[k] for k in new_idx])
                embed_ms = (time.perf_counter() - t0) * 1000.0
                for k, v in zip(new_idx, vecs):
                    vw.add({"page_id": page_id, "sha1": shas[k], "vector": v.astype("<f2").tobytes()})
                    page_seen.add(shas[k])
                    progress["vectors"] += 1
            new_set = set(new_idx)
            for k, s in enumerate(shas):
                rw.add({"page_id": page_id, "rev_id": rev_id, "chunk_idx": k, "chunk_id": C.chunk_id(page_id, k),
                        "sha1": s, "changed": k >= len(prev_pos) or prev_pos[k] != s, "new_text": k in new_set,
                        "n_new": len(new_idx), "embed_ms": embed_ms, "is_base": bool(r["is_base"])})
            prev_pos = shas
            prev_vecs = {s: True for s in shas}
            progress["revs"] += 1
            done_rows += 1
            if done_rows % 2000 == 0:
                el = time.perf_counter() - t_start
                print(f"[embed] {done_rows}/{n_total} revs, vectors {progress['vectors']}, {el:.0f}s, "
                      f"eta {el / done_rows * (n_total - done_rows) / 60:.0f}min", flush=True)
    checkpoint(prev_page)

    # 요약 (리비전 단위)
    rev = pq.read_table(out / "revisions", columns=["page_id", "rev_id", "n_new", "embed_ms", "chunk_idx", "is_base"])
    first = rev.filter(pc.equal(rev["chunk_idx"], 0)).to_pydict()
    ems = np.array(first["embed_ms"])
    nnew = np.array(first["n_new"])
    meta = {
        "model": a.model, "dim": dim, "chunk_chars": a.chunk_chars, "chunk_rule": "pack-paragraphs-v1",
        "vector_reuse": "same page, same sha1 as any chunk of previous revision", "vector_dtype": "float16",
        "device": a.device, "fp16": a.fp16, "batch_size": a.batch_size, "host": platform.node(),
        "gpu": __import__("torch").cuda.get_device_name(0) if a.device == "cuda" else None,
        "subset": str(a.subset), "revs": int(len(ems)), "vectors": int(progress["vectors"]),
        "rev_rows": int(rev.num_rows), "elapsed_s": round(progress["elapsed_s"], 1),
        "n_new_per_rev": {"mean": float(nnew.mean()), "p50": float(np.median(nnew)), "max": int(nnew.max())},
        "embed_ms_per_rev": {"mean": float(ems.mean()), "p50": float(np.median(ems)),
                             "p90": float(np.percentile(ems, 90)), "max": float(ems.max())},
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    progress_p.unlink(missing_ok=True)
    print("[embed] done", json.dumps(meta, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
