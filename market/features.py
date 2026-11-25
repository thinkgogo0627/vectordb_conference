"""⑤ 상태 벡터 (설계 4절, 30차원): 1분봉 → 60분 창의 5분 묶음 12개 → 가격 12 + 거래량 12 + 요약 6.

- 묶음 경계는 창 끝 t에서 거꾸로 센다: 묶음 k = [t-60+5k, t-55+5k) 분.
- 가격·거래량은 그 종목의 '평소' 값(Ref, 직전 20일)으로 나눈다. 창 자기 자신의 변동성으로 나누지 않는다.
- 거리는 L2. 블록 길이를 맞추지 않는다 (움직임의 크기가 남아야 희귀도가 의미 있다).
- 유효 조건: 그날 종목 목록에 있고, 12개 묶음 중 10개 이상에 체결이 있고, 미국 주식은 창이 정규장 안에 있다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dtime, timezone
from zoneinfo import ZoneInfo

import numpy as np

MIN = 60_000
WIN_MIN, BUCKET_MIN = 60, 5
NB = WIN_MIN // BUCKET_MIN            # 12
DIM = NB + NB + 6                     # 30
W_PRICE, W_VOL, W_SUM = 1.0, 0.5, 0.5
MIN_FILLED = 10                       # 12개 묶음 중
P_CLIP, VR_CAP, VOLR_CLIP, RANGE_CLIP = 5.0, np.log(11.0), 3.0, 5.0
NY = ZoneInfo("America/New_York")
US_OPEN, US_CLOSE = dtime(9, 30), dtime(16, 0)


@dataclass
class Ref:
    """그 종목의 평소 값 (직전 기간). sigma5 = 평소 5분 로그수익률 크기(강건 추정), vref = 슬롯별 평소 5분 거래량."""
    sigma5: float
    vref: np.ndarray                   # 미국: 30분 슬롯 13개 (09:30부터), 코인: UTC 시간 24개
    fill5: float
    in_universe: bool

    @property
    def sigma60(self) -> float:
        return self.sigma5 * np.sqrt(NB)


def us_session_ms(minute_ms: int) -> tuple[int, int]:
    """그 날짜(뉴욕 기준)의 정규장 시작·끝 (UTC ms)."""
    d = datetime.fromtimestamp(minute_ms / 1000, NY).date()
    o = datetime.combine(d, US_OPEN, NY).astimezone(timezone.utc)
    c = datetime.combine(d, US_CLOSE, NY).astimezone(timezone.utc)
    return int(o.timestamp() * 1000), int(c.timestamp() * 1000)


def slot_of(asset_class: str, minute_ms: int) -> int:
    if asset_class == "us_equity":
        o, _ = us_session_ms(minute_ms)
        return int(min(12, max(0, (minute_ms - o) // (30 * MIN))))
    return int((minute_ms // (60 * MIN)) % 24)


def _clip_scale(x, lo, hi, scale):
    return np.clip(x, lo, hi) / scale


def state_vector(bars: dict[int, tuple], t_ms: int, ref: Ref | None, asset_class: str
                 ) -> tuple[np.ndarray | None, dict]:
    """bars: {분 시작 ms: (o, h, l, c, v)}. t_ms: 창 끝 (분 경계, 이 분은 포함하지 않음).
    반환: (30차원 벡터 또는 None, 정보 {filled, reason, ...})."""
    start = t_ms - WIN_MIN * MIN
    if ref is None or not ref.in_universe:
        return None, {"reason": "not_in_universe"}
    if asset_class == "us_equity":
        o, c = us_session_ms(start)
        if start < o or t_ms > c:
            return None, {"reason": "outside_session"}
    mins = sorted(m for m in bars if start <= m < t_ms)
    if not mins:
        return None, {"reason": "no_bars", "filled": 0}
    closes = np.empty(NB)
    vols = np.zeros(NB)
    has = np.zeros(NB, bool)
    hi, lo = -np.inf, np.inf
    c0 = float(bars[mins[0]][0])                     # 창 안 첫 1분봉의 시가
    last = c0
    j = 0
    for k in range(NB):
        b_end = start + (k + 1) * BUCKET_MIN * MIN
        while j < len(mins) and mins[j] < b_end:
            o_, h_, l_, c_, v_ = bars[mins[j]]
            has[k] = True
            last = float(c_)
            vols[k] += float(v_)
            hi, lo = max(hi, float(h_)), min(lo, float(l_))
            j += 1
        closes[k] = last                              # 빈 묶음은 직전 가격 (변화 0)
    filled = int(has.sum())
    info = {"filled": filled, "c0": c0, "close": float(closes[-1])}
    if filled < MIN_FILLED:
        return None, {**info, "reason": "too_sparse"}
    s60 = max(ref.sigma60, 1e-12)
    path = np.log(closes / c0) / s60
    slots = [slot_of(asset_class, start + k * BUCKET_MIN * MIN) for k in range(NB)]
    vr = np.log1p(vols / np.maximum(ref.vref[slots], 1e-12))
    rets = np.diff(np.log(np.concatenate([[c0], closes])))
    win_vol = float(np.std(rets))
    summary = np.array([
        _clip_scale(path[-1], -P_CLIP, P_CLIP, P_CLIP),
        _clip_scale(np.log(max(win_vol, 1e-12) / max(ref.sigma5, 1e-12)), -VOLR_CLIP, VOLR_CLIP, VOLR_CLIP),
        _clip_scale(np.log(hi / lo) / s60 if lo > 0 else 0.0, 0, RANGE_CLIP, RANGE_CLIP),
        float(np.argmax(closes)) / (NB - 1),
        float(np.argmin(closes)) / (NB - 1),
        filled / NB,
    ])
    vec = np.concatenate([W_PRICE * _clip_scale(path, -P_CLIP, P_CLIP, P_CLIP),
                          W_VOL * _clip_scale(vr, 0, VR_CAP, VR_CAP),
                          W_SUM * summary]).astype(np.float32)
    return vec, {**info, "reason": "ok"}
