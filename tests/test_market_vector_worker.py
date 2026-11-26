"""벡터 워커: CDC 반영 → 재계산 → 유사 검색, 정정 처리, 오래된 메시지 무시 (DB 없이)."""
from datetime import datetime, timezone

import numpy as np

from market.features import MIN, Ref
from market.vector_worker import Worker

T0 = 1_795_444_200_000          # 분 경계


def after(sym, m, price, recv, vol=20.0):
    return {"source": "upbit", "symbol": sym, "minute_ts": datetime.fromtimestamp(m / 1000, timezone.utc).isoformat(),
            "o": price, "h": price, "l": price, "c": price, "v": vol, "recv_ts": recv, "built_ts": recv + 10}


def make_worker():
    w = Worker(store=object())
    for s in ("KRW-A", "KRW-B", "KRW-C"):
        w.refs[("upbit", s)] = Ref(0.001, np.full(24, 100.0), 0.95, True)
        w.cls[("upbit", s)] = "crypto"
    return w


def feed(w, sym, slope, recv0=1):
    for i in range(60):
        w.on_change(after(sym, T0 + i * MIN, 100 * np.exp(slope * (i + 1) / 60), recv0 + i))


def test_similar_ranks_same_direction_first_and_corrections_recompute():
    w = make_worker()
    feed(w, "KRW-A", 0.004)      # 상승
    feed(w, "KRW-B", 0.0035)     # 비슷한 상승
    feed(w, "KRW-C", -0.004)     # 하락
    w.last_msg_wall["upbit"] = 0                     # 메시지가 멈춘 지 오래 → 확정
    w.tick()
    assert w.computed_t["upbit"] == T0 + 60 * MIN and len(w.keys) == 3
    r = w.similar("KRW-A", 2, "all", False)
    assert [x["symbol"] for x in r["results"]] == ["KRW-B", "KRW-C"]

    before = w.index[("upbit", "KRW-C")][0].copy()
    w.on_change(after("KRW-C", T0 + 59 * MIN, 50.0, recv=10_000))   # 지난 분의 정정 (늦게 받은 값)
    assert ("upbit", "KRW-C") in w.dirty
    w.tick()
    assert not w.dirty and not np.allclose(before, w.index[("upbit", "KRW-C")][0])
    assert w.stats["corrections"] == 1

    w.on_change(after("KRW-C", T0 + 59 * MIN, 100.0, recv=5))        # 더 일찍 받은 값은 무시
    assert w.stats["older_ignored"] == 1 and w.bars[("upbit", "KRW-C")][T0 + 59 * MIN][3] == 50.0


def test_not_in_universe_and_sparse_symbols_excluded():
    w = make_worker()
    w.refs[("upbit", "KRW-B")] = Ref(0.001, np.full(24, 100.0), 0.5, False)
    feed(w, "KRW-A", 0.004)
    feed(w, "KRW-B", 0.004)
    for i in range(0, 60, 3):                                        # 3분에 한 번만 체결 → 묶음은 채워짐
        w.on_change(after("KRW-C", T0 + i * MIN, 100.0, i + 1))
    w.last_msg_wall["upbit"] = 0
    w.tick()
    assert ("upbit", "KRW-B") not in w.index and w.stats["skipped_not_universe"] == 60
    assert w.similar("KRW-B", 3, "all", False)["error"] == "벡터 없음"
