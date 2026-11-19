"""실험 2 (P3): HNSW 업데이트 처리 방식 3가지. 세 방식 모두 hnswlib(chroma-hnswlib 0.7.6, 같은 API)로 통일한다.

라벨은 (chunk_id, 리비전)마다 새로 붙인다. 그래야 검색 결과의 라벨이 옛 리비전인지 바로 알 수 있다 (구버전 혼입률).

- tombstone  꺼두기: 옛 노드는 mark_deleted만 하고 그래프에 남긴다. 새 벡터는 새 노드로 추가. 검색 결과에서만 걸러진다.
- reinsert   지웠다 다시 넣기: 옛 노드를 mark_deleted한 뒤 새 벡터를 replace_deleted=True로 넣는다.
             hnswlib은 지운 자리(내부 슬롯)를 재사용하고 그 슬롯의 이웃 간선을 다시 계산한다(updatePoint).
             주의: 이웃이 아니었던 노드에서 들어오는 간선은 그대로 남는다. 간선을 '전부' 지우는 삭제는 아니다 (D-07 확인 사항).
- rebuild    재구축: 평소엔 꺼두기, 꺼진 노드 비율이 threshold에 닿으면 살아 있는 벡터만으로 인덱스를 새로 만든다.

커머스(P3.2)는 apply_flips로 상품 상태 전환을 반영한다. 상품 벡터는 고정이고 같은 상품이 꺼졌다 켜지기를 반복한다.
- 품절: 세 방식 모두 mark_deleted (hnswlib에는 노드·간선을 실제로 지우는 연산이 없다)
- 판매 재개: tombstone·rebuild는 그래프에 남은 그 노드를 unmark_deleted (쓰기 없음).
  reinsert는 같은 벡터를 새 라벨로 replace_deleted=True 삽입 (아무 꺼진 슬롯이나 재사용, 이웃 간선 재계산).
  그래프에 없는 상품(신규, 또는 재구축 때 빠진 품절 상품)은 세 방식 모두 새로 넣는다.
"""
from __future__ import annotations

import time

import hnswlib
import numpy as np


class HnswUpdater:
    def __init__(self, method: str, vecs: np.ndarray, dim: int, max_elements: int, M: int = 16,
                 ef_construction: int = 200, build_threads: int = 16, rebuild_threshold: float | None = None):
        assert method in ("tombstone", "reinsert", "rebuild")
        self.method, self.vecs, self.dim = method, vecs, dim
        self.M, self.efc, self.build_threads = M, ef_construction, build_threads
        self.max_elements = max_elements
        self.rebuild_threshold = rebuild_threshold
        self.index: hnswlib.Index | None = None
        self.lab_chunk = np.full(max_elements, -1, np.int64)   # 라벨 → chunk_id
        self.lab_vrow = np.full(max_elements, -1, np.int64)    # 라벨 → 벡터 행
        self.lab_live = np.zeros(max_elements, bool)           # 라벨이 현재 살아 있는(최신) 노드인가
        self.chunk_lab: dict[int, int] = {}
        self.next_label = 0
        self.n_nodes = 0          # 그래프에 들어 있는 노드 수 (꺼진 노드 포함)
        self.n_dead = 0           # 그래프에 남아 있는 꺼진 노드 수
        self.cost = {"adds": 0, "marks": 0, "replaces": 0, "unmarks": 0, "apply_s": 0.0, "rebuilds": 0,
                     "rebuild_s": 0.0}
        self.off_lab: dict[int, int] = {}   # 커머스: 품절로 꺼졌지만 그래프에 남은 상품 → 그 라벨 (tombstone·rebuild)

    # ---------- 초기 인덱스 ----------
    def _new_index(self, capacity: int) -> hnswlib.Index:
        ix = hnswlib.Index(space="ip", dim=self.dim)
        ix.init_index(max_elements=capacity, M=self.M, ef_construction=self.efc,
                      allow_replace_deleted=(self.method == "reinsert"))
        return ix

    def build_initial(self, chunks: np.ndarray, vrows: np.ndarray, path: str | None = None) -> float:
        """초기 인덱스를 만든다. path가 주어지면 저장된 그래프를 읽는다 (같은 rep의 세 방식이 같은 그래프에서 출발)."""
        n = len(chunks)
        labels = np.arange(n, dtype=np.int64)
        self.lab_chunk[:n], self.lab_vrow[:n], self.lab_live[:n] = chunks, vrows, True
        self.chunk_lab = dict(zip(chunks.tolist(), labels.tolist()))
        self.next_label = self.n_nodes = n
        t = time.perf_counter()
        if path:
            ix = hnswlib.Index(space="ip", dim=self.dim)
            ix.load_index(path, max_elements=self.max_elements, allow_replace_deleted=(self.method == "reinsert"))
        else:
            ix = self._new_index(self.max_elements)
            ix.set_num_threads(self.build_threads)
            for s in range(0, n, 100_000):
                ix.add_items(np.ascontiguousarray(self.vecs[vrows[s:s + 100_000]]), labels[s:s + 100_000],
                             num_threads=self.build_threads)
        ix.set_num_threads(1)
        self.index = ix
        return time.perf_counter() - t

    # ---------- 업데이트 ----------
    def apply_edit(self, chunk_ids: np.ndarray, vrows: np.ndarray) -> None:
        """한 편집의 청크 연산들. vrow >= 0 → upsert, -1 → delete."""
        t = time.perf_counter()
        ix = self.index
        for cid, vrow in zip(chunk_ids.tolist(), vrows.tolist()):
            old = self.chunk_lab.pop(cid, None)
            if old is not None:
                ix.mark_deleted(old)
                self.lab_live[old] = False
                self.cost["marks"] += 1
                self.n_dead += 1
            if vrow < 0:
                continue
            lab = self.next_label
            self.next_label += 1
            self.lab_chunk[lab], self.lab_vrow[lab], self.lab_live[lab] = cid, vrow, True
            self.chunk_lab[cid] = lab
            self._insert(lab, vrow)
        self.cost["apply_s"] += time.perf_counter() - t
        self._maybe_rebuild()

    def _insert(self, lab: int, vrow: int) -> None:
        v = self.vecs[vrow][None, :]
        if self.method == "reinsert" and self.n_dead > 0:
            self.index.add_items(v, [lab], num_threads=1, replace_deleted=True)
            self.cost["replaces"] += 1
            self.n_dead -= 1        # 꺼진 슬롯 하나를 새 노드가 차지
        else:
            self.index.add_items(v, [lab], num_threads=1)
            self.cost["adds"] += 1
            self.n_nodes += 1

    def _maybe_rebuild(self) -> None:
        if self.method == "rebuild" and self.n_dead > 0 and self.dead_ratio() >= self.rebuild_threshold:
            self.rebuild()

    def apply_flips(self, items: np.ndarray, vrows: np.ndarray, on: np.ndarray) -> None:
        """커머스: 한 스냅샷의 상품 상태 전환들. on=True 판매 재개(재입고·신규), False 품절."""
        t = time.perf_counter()
        ix = self.index
        for it, vrow, o in zip(items.tolist(), vrows.tolist(), on.tolist()):
            if not o:
                lab = self.chunk_lab.pop(it, None)
                if lab is None:
                    continue
                ix.mark_deleted(lab)
                self.lab_live[lab] = False
                self.cost["marks"] += 1
                self.n_dead += 1
                if self.method != "reinsert":
                    self.off_lab[it] = lab
                continue
            if it in self.chunk_lab:
                continue
            dead = self.off_lab.pop(it, None)
            if dead is not None:                 # tombstone·rebuild: 표시만 푼다
                ix.unmark_deleted(dead)
                self.lab_live[dead] = True
                self.chunk_lab[it] = dead
                self.cost["unmarks"] += 1
                self.n_dead -= 1
                continue
            lab = self.next_label
            self.next_label += 1
            self.lab_chunk[lab], self.lab_vrow[lab], self.lab_live[lab] = it, vrow, True
            self.chunk_lab[it] = lab
            self._insert(lab, vrow)
        self.cost["apply_s"] += time.perf_counter() - t
        self._maybe_rebuild()

    def rebuild(self) -> float:
        t = time.perf_counter()
        live = np.fromiter(self.chunk_lab.values(), np.int64)
        live.sort()
        self.index = None          # 옛 그래프 메모리를 먼저 놓는다 (인덱스 하나가 수 GB)
        ix = self._new_index(self.max_elements)
        ix.set_num_threads(self.build_threads)
        for s in range(0, len(live), 100_000):
            labs = live[s:s + 100_000]
            ix.add_items(np.ascontiguousarray(self.vecs[self.lab_vrow[labs]]), labs, num_threads=self.build_threads)
        ix.set_num_threads(1)
        self.index = ix
        self.n_nodes, self.n_dead = len(live), 0
        self.off_lab.clear()       # 꺼진 노드는 새 그래프에 없다 → 판매 재개 땐 새로 넣는다
        dt = time.perf_counter() - t
        self.cost["rebuilds"] += 1
        self.cost["rebuild_s"] += dt
        return dt

    # ---------- 상태·검색 ----------
    def dead_ratio(self) -> float:
        return self.n_dead / max(1, self.n_nodes)

    def live_labels(self) -> np.ndarray:
        return np.fromiter(self.chunk_lab.values(), np.int64)

    def search(self, q: np.ndarray, k: int, ef: int, repeat: int = 5) -> tuple[np.ndarray, np.ndarray]:
        """질의 하나씩, 단일 스레드로 repeat번 재서 질의별 지연(ms) 중앙값을 돌려준다 (OS 잡음 완화)."""
        self.index.set_ef(max(ef, k))
        labs = np.empty((len(q), k), np.int64)
        ms = np.empty((repeat, len(q)))
        for rep in range(repeat):
            self._search_once(q, k, labs, ms[rep])
        return labs, np.median(ms, axis=0)

    def _search_once(self, q, k, labs, ms) -> None:
        for i in range(len(q)):
            t = time.perf_counter()
            try:
                l, _ = self.index.knn_query(q[i:i + 1], k=k, num_threads=1)
                labs[i] = l[0]
            except RuntimeError:   # 꺼진 노드에 막혀 k개를 못 채움 → 못 채운 자리는 -1
                l, _ = self._knn_partial(q[i:i + 1], k)
                labs[i] = -1
                labs[i, :len(l)] = l
            ms[i] = (time.perf_counter() - t) * 1000

    def _knn_partial(self, q: np.ndarray, k: int):
        for kk in range(k - 1, 0, -1):
            try:
                l, d = self.index.knn_query(q, k=kk, num_threads=1)
                return l[0], d[0]
            except RuntimeError:
                continue
        return np.array([], np.int64), np.array([])
