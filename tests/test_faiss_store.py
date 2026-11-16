"""T-05 완료 기준: 이벤트 순서를 섞은 입력에서 옛 버전이 새 버전을 덮어쓰지 않는다.
T-06 판정 규칙(normal / stale / missing)과 T-02 청크 규칙도 여기서 확인한다."""
import random

import numpy as np

from pipeline.config import chunk_id, split_chunk_id
from pipeline.embed import chunk_text
from pipeline.evaluate import judge_chunk, judge_edit
from pipeline.faiss_store import VersionedFlatIndex


def _vec(seed: int, dim: int = 8) -> np.ndarray:
    v = np.random.default_rng(seed).standard_normal(dim).astype(np.float32)
    return v / np.linalg.norm(v)


def test_shuffled_events_never_downgrade():
    rng = random.Random(1)
    events = []
    for page in range(1, 21):
        for rev in range(1, 6):
            chunks = [(chunk_id(page, k), _vec(page * 100 + rev * 10 + k)) for k in range(rng.randint(1, 3))]
            events.append((page, rev, chunks))
    rng.shuffle(events)
    store = VersionedFlatIndex(8)
    for page, rev, chunks in events:
        store.apply(page, rev, chunks)
    st = store.state()
    assert all(st["page_rev"][p] == 5 for p in range(1, 21))
    assert all(r == 5 for r in st["chunk_rev"].values())
    # 인덱스에 남은 벡터는 rev 5의 청크 수와 정확히 같다
    n5 = sum(len(c) for p, r, c in events if r == 5)
    assert st["ntotal"] == n5
    assert st["skipped_old"] > 0


def test_replace_removes_dropped_chunks_and_search_returns_latest():
    store = VersionedFlatIndex(8)
    v_old = [(chunk_id(7, 0), _vec(1)), (chunk_id(7, 1), _vec(2)), (chunk_id(7, 2), _vec(3))]
    store.apply(7, 10, v_old)
    v_new = [(chunk_id(7, 0), _vec(1)), (chunk_id(7, 1), _vec(22))]  # 청크 2 삭제, 청크 1 변경
    store.apply(7, 11, v_new)
    st = store.state()
    assert st["ntotal"] == 2 and chunk_id(7, 2) not in st["chunk_rev"]
    ids, _ = store.search(_vec(22)[None, :], 1)
    assert ids[0][0] == chunk_id(7, 1) and st["chunk_rev"][chunk_id(7, 1)] == 11
    assert split_chunk_id(chunk_id(7, 1)) == (7, 1)


def test_judge_rules():
    qc = chunk_id(1, 0)
    assert judge_chunk(qc, 5, 5, qc, 5) == "normal"
    assert judge_chunk(qc, 4, 4, qc, 5) == "stale"
    assert judge_chunk(-1, -1, -1, qc, 5) == "missing"
    assert judge_chunk(chunk_id(2, 0), 9, 4, qc, 5) == "stale"   # 다른 청크가 top-1인데 내 청크는 옛 rev
    assert judge_chunk(chunk_id(2, 0), 9, 5, qc, 5) == "other"
    assert judge_edit(["normal", "stale"]) == "stale"
    assert judge_edit(["normal", "missing"]) == "missing"
    assert judge_edit(["normal", "other"]) == "normal"


def test_chunk_text_packs_paragraphs():
    text = "\n\n".join(["a" * 300, "b" * 300, "c" * 300, "d" * 1000])
    chunks = chunk_text(text, max_chars=800)
    assert chunks[0] == "a" * 300 + "\n\n" + "b" * 300
    assert chunks[1] == "c" * 300
    assert chunks[2] == "d" * 800 and chunks[3] == "d" * 200
    assert chunk_text("", 800) == [""]
