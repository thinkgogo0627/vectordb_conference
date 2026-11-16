"""T-05: 버전을 아는 FAISS 저장소 (Flat + IndexIDMap2).

한 chunk_id 에는 벡터가 하나만 있다. 더 새 리비전일 때만 교체한다 (옛 이벤트가 늦게 와도 덮어쓰지 않음).
교체 = 그 문서의 기존 청크 전부 remove_ids → 새 리비전 청크 add_with_ids.
"""
from __future__ import annotations

import threading

import faiss
import numpy as np


class VersionedFlatIndex:
    def __init__(self, dim: int):
        self.dim = dim
        self.index = faiss.IndexIDMap2(faiss.IndexFlatIP(dim))
        self.page_rev: dict[int, int] = {}          # page_id -> 반영된 rev_id
        self.page_chunks: dict[int, list[int]] = {}  # page_id -> chunk_id 목록
        self.chunk_rev: dict[int, int] = {}          # chunk_id -> rev_id
        self.lock = threading.Lock()
        self.n_applied = 0
        self.n_skipped_old = 0

    def apply(self, page_id: int, rev_id: int, chunks: list[tuple[int, np.ndarray]]) -> str:
        """returns 'applied' | 'skipped_old'"""
        with self.lock:
            cur = self.page_rev.get(page_id)
            if cur is not None and cur >= rev_id:
                self.n_skipped_old += 1
                return "skipped_old"
            old_ids = self.page_chunks.get(page_id, [])
            if old_ids:
                self.index.remove_ids(np.asarray(old_ids, dtype=np.int64))
                for cid in old_ids:
                    self.chunk_rev.pop(cid, None)
            if chunks:
                ids = np.asarray([c for c, _ in chunks], dtype=np.int64)
                vecs = np.stack([v for _, v in chunks]).astype(np.float32)
                self.index.add_with_ids(vecs, ids)
            self.page_rev[page_id] = rev_id
            self.page_chunks[page_id] = [c for c, _ in chunks]
            for c, _ in chunks:
                self.chunk_rev[c] = rev_id
            self.n_applied += 1
            return "applied"

    def search(self, vecs: np.ndarray, k: int = 1) -> tuple[np.ndarray, np.ndarray]:
        with self.lock:
            if self.index.ntotal == 0:
                n = vecs.shape[0]
                return np.full((n, k), -1, dtype=np.int64), np.full((n, k), -1.0, dtype=np.float32)
            scores, ids = self.index.search(np.ascontiguousarray(vecs, dtype=np.float32), k)
            return ids, scores

    def state(self) -> dict:
        with self.lock:
            return {"ntotal": int(self.index.ntotal), "applied": self.n_applied,
                    "skipped_old": self.n_skipped_old,
                    "page_rev": dict(self.page_rev), "chunk_rev": dict(self.chunk_rev)}
