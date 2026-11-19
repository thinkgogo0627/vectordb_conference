"""실험 2 처리 방식 3가지: 업데이트 뒤 검색 결과에 옛 리비전이 나오지 않고, 방식별 노드 수가 설계대로인지."""
import numpy as np
import pytest

from pipeline.hnsw_store import HnswUpdater


def _stream(seed=0, n0=400, n_ops=600, dim=16):
    rng = np.random.default_rng(seed)
    vecs = rng.standard_normal((n0 + n_ops, dim)).astype(np.float32)
    vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
    chunks = np.arange(n0, dtype=np.int64)
    ops = []
    for i in range(n_ops):
        cid = int(rng.integers(0, n0 + 50))          # 일부는 새 청크 자리
        ops.append((cid, -1 if rng.random() < 0.1 else n0 + i))
    return vecs, chunks, ops


@pytest.mark.parametrize("method,thr", [("tombstone", None), ("reinsert", None), ("rebuild", 0.2)])
def test_methods_keep_only_latest(method, thr):
    vecs, chunks, ops = _stream()
    upd = HnswUpdater(method, vecs, 16, len(vecs) + 8, M=8, ef_construction=50, build_threads=1,
                      rebuild_threshold=thr)
    upd.build_initial(chunks, np.arange(len(chunks)))
    latest = {int(c): int(c) for c in chunks}
    for cid, vr in ops:
        upd.apply_edit(np.array([cid]), np.array([vr]))
        if vr < 0:
            latest.pop(cid, None)
        else:
            latest[cid] = vr
    assert {int(upd.lab_chunk[l]): int(upd.lab_vrow[l]) for l in upd.live_labels()} == latest
    labs, _ = upd.search(vecs[list(latest.values())[:50]], 5, 50)
    got = labs[labs >= 0]
    assert upd.lab_live[got].all()                      # 옛 리비전은 결과에 안 나온다
    marks = upd.cost["marks"]
    if method == "tombstone":
        assert upd.n_dead == marks and upd.n_nodes == len(chunks) + upd.cost["adds"]
    if method == "reinsert":
        assert upd.n_nodes - upd.n_dead == len(latest) and upd.cost["replaces"] > 0
    if method == "rebuild":
        assert upd.cost["rebuilds"] >= 1 and upd.dead_ratio() < thr


@pytest.mark.parametrize("method,thr", [("tombstone", None), ("reinsert", None), ("rebuild", 0.0)])
def test_commerce_flips(method, thr):
    """커머스: 같은 상품이 꺼졌다 켜지기를 반복해도 살아 있는 상품 = 판매 중 상품, 결과에 품절 상품 없음."""
    rng = np.random.default_rng(1)
    n, dim = 500, 16
    vecs = rng.standard_normal((n, dim)).astype(np.float32)
    vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
    state = rng.random(n) < 0.5                         # 첫 스냅샷 판매 중
    upd = HnswUpdater(method, vecs, dim, 4 * n, M=8, ef_construction=50, build_threads=1, rebuild_threshold=thr)
    init = np.flatnonzero(state)
    upd.build_initial(init, init)
    for _ in range(6):                                  # 스냅샷 6주
        flip = rng.random(n) < 0.3
        new = state ^ flip
        off, on = np.flatnonzero(state & ~new), np.flatnonzero(~state & new)
        items = np.r_[off, on]
        upd.apply_flips(items, items, np.r_[np.zeros(len(off), bool), np.ones(len(on), bool)])
        state = new
        assert set(upd.chunk_lab) == set(np.flatnonzero(state).tolist())
        assert all(upd.lab_vrow[l] == c for c, l in upd.chunk_lab.items())
    labs, _ = upd.search(vecs[np.flatnonzero(state)[:50]], 5, 50)
    assert upd.lab_live[labs[labs >= 0]].all()          # 품절 상품은 결과에 안 나온다
    if method == "tombstone":
        assert upd.cost["unmarks"] > 0 and upd.n_nodes == len(init) + upd.cost["adds"]
    if method == "reinsert":
        assert upd.cost["unmarks"] == 0 and upd.cost["replaces"] > 0
        assert upd.n_nodes - upd.n_dead == len(upd.chunk_lab)
    if method == "rebuild":
        assert upd.cost["rebuilds"] == 6 and upd.n_dead == 0
