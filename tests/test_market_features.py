"""⑤ 상태 벡터 (30차원, 5분 묶음)."""
import numpy as np

from market.features import DIM, MIN, NB, Ref, slot_of, state_vector, us_session_ms

T = 1_795_444_200_000 + 120 * MIN       # 2026-11-23 16:30 UTC = 11:30 ET (정규장 안)
REF = Ref(sigma5=0.001, vref=np.full(24, 100.0), fill5=0.95, in_universe=True)
REF_US = Ref(sigma5=0.001, vref=np.full(13, 100.0), fill5=0.95, in_universe=True)


def flat_bars(t, price=100.0, vol=100.0, skip=()):
    return {t - (60 - i) * MIN: (price, price, price, price, vol / 5) for i in range(60) if i not in skip}


def test_flat_market_vector():
    v, info = state_vector(flat_bars(T), T, REF, "crypto")
    assert v.shape == (DIM,) and info["filled"] == NB
    assert np.allclose(v[:NB], 0)                                # 가격 변화 없음
    assert np.allclose(v[NB:2 * NB], 0.5 * np.log(2) / np.log(11))   # 평소 거래량과 같음 → ln(1+1)


def test_big_move_is_far_from_small_move_same_shape():
    def ramp(total):
        b = {}
        for i in range(60):
            p = 100 * np.exp(total * (i + 1) / 60)
            b[T - (60 - i) * MIN] = (p, p, p, p, 20.0)
        return b
    small, _ = state_vector(ramp(0.001), T, REF, "crypto")     # 평소 60분 변동폭(0.0035)보다 작은 상승
    big, _ = state_vector(ramp(0.02), T, REF, "crypto")        # 평소의 5배 넘는 상승 → 잘림
    flat, _ = state_vector(flat_bars(T, vol=100.0), T, REF, "crypto")
    assert np.linalg.norm(big - flat) > 3 * np.linalg.norm(small - flat)   # 크기가 지워지지 않는다
    assert big[NB - 1] == 1.0                                   # +5σ에서 잘림


def test_sparse_window_rejected_and_gaps_carry_forward():
    # 묶음 3개를 통째로 비우면 9/12 → 거절
    skip = set(range(10, 25))
    v, info = state_vector(flat_bars(T, skip=skip), T, REF, "crypto")
    assert v is None and info["reason"] == "too_sparse" and info["filled"] == 9
    # 묶음 2개만 비우면 10/12 → 허용, 빈 묶음 거래량 0
    skip = set(range(10, 20))
    v, info = state_vector(flat_bars(T, skip=skip), T, REF, "crypto")
    assert v is not None and info["filled"] == 10
    assert v[NB + 2] == 0 and v[NB + 3] == 0


def test_us_session_and_universe():
    o, c = us_session_ms(T)
    assert (c - o) == 390 * MIN
    v, info = state_vector(flat_bars(o + 30 * MIN), o + 30 * MIN, REF_US, "us_equity")   # 창이 장 시작 전을 걸침
    assert v is None and info["reason"] == "outside_session"
    v, _ = state_vector(flat_bars(o + 60 * MIN), o + 60 * MIN, REF_US, "us_equity")
    assert v is not None
    out = Ref(0.001, np.full(13, 100.0), 0.5, False)
    assert state_vector(flat_bars(T), T, out, "us_equity")[1]["reason"] == "not_in_universe"
    assert slot_of("us_equity", o) == 0 and slot_of("us_equity", o + 389 * MIN) == 12
