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

    def apply_many(self, items: list[tuple[int, int, list[tuple[int, np.ndarray]]]]) -> list[str]:
        """묶음 반영: 여러 문서를 remove_ids 한 번, add_with_ids 한 번으로 교체한다.
        같은 문서가 여러 번 있으면 가장 새 리비전만 반영하고 나머지는 'coalesced'."""
        with self.lock:
            latest: dict[int, int] = {}
            for i, (page_id, rev_id, _c) in enumerate(items):
                if page_id not in latest or rev_id > items[latest[page_id]][1]:
                    latest[page_id] = i
            results = ["coalesced"] * len(items)
            remove: list[int] = []
            add_ids: list[int] = []
            add_vecs: list[np.ndarray] = []
            for page_id, i in latest.items():
                _p, rev_id, chunks = items[i]
                cur = self.page_rev.get(page_id)
                if cur is not None and cur >= rev_id:
                    self.n_skipped_old += 1
                    results[i] = "skipped_old"
                    continue
                for cid in self.page_chunks.get(page_id, []):
                    remove.append(cid)
                    self.chunk_rev.pop(cid, None)
                for c, v in chunks:
                    add_ids.append(c)
                    add_vecs.append(v)
                    self.chunk_rev[c] = rev_id
                self.page_rev[page_id] = rev_id
                self.page_chunks[page_id] = [c for c, _ in chunks]
                self.n_applied += 1
                results[i] = "applied"
            if remove:
                self.index.remove_ids(np.asarray(remove, dtype=np.int64))
            if add_ids:
                self.index.add_with_ids(np.stack(add_vecs).astype(np.float32), np.asarray(add_ids, dtype=np.int64))
            return results

    def chunk_revs(self, ids) -> list[int]:
        with self.lock:
            return [self.chunk_rev.get(int(i), -1) for i in ids]

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
