"""같은 기초자산 묶음: 상관 계산과 기능 1 결과 접기 (DB 없이)."""
import numpy as np

from market.features import MIN, Ref
from market.pairs import MANUAL, corr_matrix, decide, load_manual, manual_pairs, returns_grid, same_pairs
from market.vector_worker import Worker
from tests.test_market_vector_worker import T0, after

B = 300_000


def test_leveraged_and_inverse_are_same_independent_is_not():
    rng = np.random.default_rng(0)
    base = rng.normal(0, 0.002, 600)
    R = np.column_stack([base,
                         2 * base + rng.normal(0, 0.0003, 600),     # 2배 레버리지 (잡음 조금)
                         -base + rng.normal(0, 0.0003, 600),        # 인버스
                         rng.normal(0, 0.002, 600)])                # 무관
    R[:300, 3] = np.nan                                             # 겹친 시점이 적어도 그 구간으로 잰다
    c, n = corr_matrix(R, min_n=100)
    assert c[0, 1] > 0.98 and c[0, 2] < -0.98 and abs(c[0, 3]) < 0.2
    assert n[0, 3] == 300 and np.isnan(c[0, 0])
    keys = [("s", "A"), ("s", "A2X"), ("s", "AINV"), ("s", "Z")]
    got = {(p[1], p[3]) for p in same_pairs(keys, c, n, thr=0.95)}
    assert got == {("A", "A2X"), ("A2X", "A"), ("A", "AINV"), ("AINV", "A"), ("A2X", "AINV"), ("AINV", "A2X")}


def test_returns_grid_fills_inside_session_only():
    s1, s2 = (0, 4 * B), (10 * B, 13 * B)
    rows = [("s", "A", 0, 100.0), ("s", "A", 2 * B, 110.0),               # 1번째 묶음 비어 있음 → 직전 값
            ("s", "A", 10 * B, 50.0), ("s", "A", 11 * B, 55.0)]
    keys, R = returns_grid(rows, [s1, s2], B)
    r = R[:, 0]
    assert np.isnan(r[0]) and r[1] == 0 and np.isclose(r[2], np.log(1.1)) and r[3] == 0
    assert np.isnan(r[4])                                                  # 다음 장 첫 묶음: 밤사이 수익률은 쓰지 않는다
    assert np.isclose(r[5], np.log(1.1)) and r[6] == 0


def _worker():
    w = Worker(store=object())
    for s in ("KRW-A", "KRW-A2X", "KRW-B", "KRW-B2", "KRW-C"):
        w.refs[("upbit", s)] = Ref(0.001, np.full(24, 100.0), 0.95, True)
        w.cls[("upbit", s)] = "crypto"
    slopes = {"KRW-A": 0.004, "KRW-A2X": 0.0041, "KRW-B": 0.0034, "KRW-B2": 0.0033, "KRW-C": -0.004}
    for s, sl in slopes.items():
        for i in range(60):
            w.on_change(after(s, T0 + i * MIN, 100 * np.exp(sl * (i + 1) / 60), i + 1))
    w.last_msg_wall["upbit"] = 0
    w.tick()
    u = lambda s: ("upbit", s)
    w.pairs = {u("KRW-A"): {u("KRW-A2X"): 0.99}, u("KRW-A2X"): {u("KRW-A"): 0.99},     # pairs.load: same인 쌍만
               u("KRW-B"): {u("KRW-B2"): None}, u("KRW-B2"): {u("KRW-B"): None}}      # 상품군 목록 (상관 없음)
    return w


def test_similar_folds_same_underlying():
    w = _worker()
    raw = w.similar("KRW-A", 3, "all", False, fold=False)
    assert [x["symbol"] for x in raw["results"]] == ["KRW-A2X", "KRW-B", "KRW-B2"]

    r = w.similar("KRW-A", 3, "all", False)
    assert [x["symbol"] for x in r["same_underlying"]] == ["KRW-A2X"] and r["same_underlying"][0]["corr"] == 0.99
    assert [x["symbol"] for x in r["results"]] == ["KRW-B", "KRW-C"]          # 결과가 모자라면 그대로 둔다
    assert r["results"][0]["same_underlying"] == ["KRW-B2"]                    # 결과끼리 같은 기초자산 → 접음
    assert r["results"][1]["same_underlying"] == []


def test_manual_groups_decide_and_correlation_only_auto_above_098():
    src = {"KRW-BTC": "upbit", "IBIT": "alpaca", "FBTC": "alpaca", "QQQ": "alpaca", "SPY": "alpaca", "VOO": "alpaca"}
    mp, missing = manual_pairs([{"name": "btc", "symbols": ["KRW-BTC", "IBIT", "FBTC", "NOPE"]}], src)
    btc, ibit, fbtc = ("upbit", "KRW-BTC"), ("alpaca", "IBIT"), ("alpaca", "FBTC")
    assert missing == ["NOPE"] and len(mp) == 6 and (btc, fbtc) in mp and (fbtc, btc) in mp
    corr = [("alpaca", "FBTC", "upbit", "KRW-BTC", 0.936, 700),                 # 목록 → 상관이 낮아도 같음
            ("alpaca", "QQQ", "alpaca", "SPY", 0.950, 700),                     # 목록 없음, 0.98 미만 → 다름
            ("alpaca", "SPY", "alpaca", "VOO", 0.991, 700)]                     # 0.98 이상 → 자동
    d = decide(corr, mp, not_same={("VOO", "SPY")})
    assert d[(fbtc, btc)] == (0.936, 700, True, True)
    assert d[(btc, ibit)] == (None, 0, True, True)
    assert d[(("alpaca", "QQQ"), ("alpaca", "SPY"))][3] is False
    assert d[(("alpaca", "SPY"), ("alpaca", "VOO"))][3] is True
    d2 = decide(corr, mp, not_same={("SPY", "VOO")})                           # not_same이 자동을 이긴다
    assert d2[(("alpaca", "SPY"), ("alpaca", "VOO"))][3] is False


def test_manual_file_is_valid():
    m = load_manual(MANUAL)
    syms = [s for g in m["groups"] for s in g["symbols"]]
    assert len(syms) == len(set(syms)), "한 종목이 두 상품군에 들어 있다"
    assert "KRW-BTC" in syms and "IBIT" in syms
