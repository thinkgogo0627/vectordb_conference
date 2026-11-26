"""⑨ 과거 데이터 채우기 (작게) + 종목별 평소 값(symbol_ref)과 오늘의 종목 목록.

    python -m market.backfill alpaca --days 20      # 미국: 직전 20거래일 IEX 1분봉 (정규장만)
    python -m market.backfill upbit --days 7 --top 150

- 평소 값: sigma5 = 5분 로그수익률의 1.4826 × MAD, vref = 슬롯별 평균 5분 거래량, fill5 = 체결 있는 5분 묶음 비율.
- 종목 목록(in_universe): fill5 >= 0.8. 목록에 든 종목의 1분봉만 bars_1m에 넣는다 (이미 있는 행은 건드리지 않는다).
- Alpaca는 실시간과 같은 feed=iex로 받는다 (거래량 기준을 맞추기 위해).
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np

from . import config as C
from .features import BUCKET_MIN, MIN, NY, US_CLOSE, US_OPEN, slot_of, us_session_ms
from .store import Bar, Store

FILL_MIN = 0.8
B = BUCKET_MIN * MIN


def robust_sigma(r: np.ndarray) -> float:
    r = r[np.isfinite(r)]
    if len(r) < 10:
        return float("nan")
    s = 1.4826 * float(np.median(np.abs(r - np.median(r))))
    return s if s > 0 else float(np.std(r))


def ref_from_buckets(closes_by_seg: list[np.ndarray], has: np.ndarray, vols: np.ndarray, slots: np.ndarray,
                     n_slots: int) -> tuple[float, list[float], float]:
    """세그먼트(미국: 하루 정규장, 코인: 전체)별 5분 종가 → sigma5, 슬롯별 평균 거래량, 채움 비율."""
    parts = [np.diff(np.log(c)) for c in closes_by_seg if len(c) > 1]
    rets = np.concatenate(parts) if parts else np.array([])
    sigma5 = robust_sigma(rets)
    overall = float(vols.mean()) if len(vols) else 0.0
    vref = []
    for s in range(n_slots):
        m = slots == s
        vref.append(max(float(vols[m].mean()) if m.any() else overall, overall * 0.1, 1e-9))
    return sigma5, vref, float(has.mean()) if len(has) else 0.0


def buckets_for(minute_bars: dict[int, tuple], seg_start: int, seg_end: int):
    """[seg_start, seg_end)를 시계에 맞춘 5분 묶음으로. 빈 묶음 종가는 직전 값으로 채운다 (첫 체결 전은 버린다)."""
    n = (seg_end - seg_start) // B
    closes, has, vols, starts = [], [], [], []
    last = None
    mins = sorted(m for m in minute_bars if seg_start <= m < seg_end)
    j = 0
    for k in range(n):
        b0, b1 = seg_start + k * B, seg_start + (k + 1) * B
        h, v = False, 0.0
        while j < len(mins) and mins[j] < b1:
            _, _, _, c, vol = minute_bars[mins[j]]
            last, h, v = c, True, v + vol
            j += 1
        has.append(h)
        vols.append(v)
        starts.append(b0)
        closes.append(last if last is not None else np.nan)
    return np.array(closes, float), np.array(has), np.array(vols), np.array(starts)


# ---------------- Alpaca ----------------

def _alpaca_get(path: str, params: dict, key: str, secret: str, host="https://data.alpaca.markets") -> dict:
    url = f"{host}{path}?{urllib.parse.urlencode(params)}"
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            raise
    raise RuntimeError("Alpaca 요청 반복 실패")


def alpaca(days: int, store: Store) -> None:
    key, secret = C.alpaca_keys()
    assets = _alpaca_get("/v2/assets", {"status": "active", "asset_class": "us_equity"}, key, secret,
                         host="https://paper-api.alpaca.markets")
    syms = sorted(a["symbol"] for a in assets if a.get("exchange") in {"NYSE", "NASDAQ", "ARCA", "AMEX", "BATS"}
                  and a.get("tradable"))
    today = datetime.now(NY).date()
    start_d = today - timedelta(days=int(days * 7 / 5) + 4)
    start = datetime.combine(start_d, US_OPEN, NY).astimezone(timezone.utc)
    end = datetime.combine(today, US_OPEN, NY).astimezone(timezone.utc)        # 오늘 데이터는 쓰지 않는다
    print(f"[backfill alpaca] 종목 {len(syms)}, {start:%Y-%m-%d} ~ {end:%Y-%m-%d} (UTC)", flush=True)
    # 거래일: SPY 봉이 있는 날 중 마지막 days일
    spy = _alpaca_get("/v2/stocks/bars", {"symbols": "SPY", "timeframe": "1Day", "start": start.isoformat(),
                                          "end": end.isoformat(), "feed": "iex", "limit": 1000}, key, secret)
    tdays = sorted({b["t"][:10] for b in spy["bars"].get("SPY", [])})[-days:]
    first_day = tdays[0]
    sessions = [us_session_ms(int(datetime.fromisoformat(d + "T15:00:00+00:00").timestamp() * 1000)) for d in tdays]
    t_first = sessions[0][0]
    calls = kept_rows = 0
    refs = []
    t0 = time.time()
    for i in range(0, len(syms), 200):
        batch = syms[i:i + 200]
        per = defaultdict(dict)
        token = None
        while True:
            q = {"symbols": ",".join(batch), "timeframe": "1Min", "start": datetime.fromtimestamp(t_first / 1000, timezone.utc).isoformat(),
                 "end": end.isoformat(), "limit": 10000, "feed": "iex", "adjustment": "raw"}
            if token:
                q["page_token"] = token
            d = _alpaca_get("/v2/stocks/bars", q, key, secret)
            calls += 1
            for s, bars in (d.get("bars") or {}).items():
                for b in bars:
                    m = int(datetime.fromisoformat(b["t"].replace("Z", "+00:00")).timestamp() * 1000)
                    per[s][m] = (b["o"], b["h"], b["l"], b["c"], b["v"], b.get("n", 0), b.get("vw"))
            token = d.get("next_page_token")
            time.sleep(0.32)                     # 분당 200회 아래
            if not token:
                break
        out = []
        for s in batch:
            mb = {m: v[:5] for m, v in per.get(s, {}).items()}
            segs, H, V, S = [], [], [], []
            for o, c in sessions:
                cl, h, v, st = buckets_for(mb, o, c)
                segs.append(cl[np.isfinite(cl)])
                H.append(h), V.append(v), S.append([slot_of("us_equity", x) for x in st])
            H, V, S = np.concatenate(H), np.concatenate(V), np.concatenate(S)
            sigma5, vref, fill5 = ref_from_buckets(segs, H, V, S, 13)
            inu = bool(fill5 >= FILL_MIN and np.isfinite(sigma5) and sigma5 > 0)
            refs.append(("alpaca", s, "us_equity", int(end.timestamp() * 1000), len(tdays),
                         sigma5 if np.isfinite(sigma5) else 0.0, vref, fill5, inu))
            if inu:
                for m, (o_, h_, l_, c_, v_, n_, vw_) in per[s].items():
                    if any(so <= m < sc for so, sc in sessions):
                        out.append(Bar("alpaca", s, m, "us_equity", o_, h_, l_, c_, v_, int(n_), vw_, 0))
        kept_rows += store.copy_bars(out)
        store.write_refs(refs[-len(batch):])
        print(f"[backfill alpaca] {i + len(batch)}/{len(syms)} 종목, 요청 {calls}, 넣은 봉 {kept_rows}, "
              f"목록 {sum(r[-1] for r in refs)}, {time.time() - t0:.0f}s", flush=True)
    print(f"[backfill alpaca] 끝: 거래일 {len(tdays)} ({first_day}~{tdays[-1]}), 종목 목록 {sum(r[-1] for r in refs)}개",
          flush=True)


# ---------------- 업비트 ----------------

def _upbit_get(path: str, params: dict):
    url = f"https://api.upbit.com{path}?{urllib.parse.urlencode(params)}"
    for attempt in range(8):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(1.0 + attempt)
                continue
            raise
    raise RuntimeError("업비트 요청 반복 실패")


def upbit(days: int, top: int, store: Store) -> None:
    markets = [m["market"] for m in _upbit_get("/v1/market/all", {"is_details": "false"}) if m["market"].startswith("KRW-")]
    tick = _upbit_get("/v1/ticker", {"markets": ",".join(markets)})
    cands = [t["market"] for t in sorted(tick, key=lambda t: -t["acc_trade_price_24h"])][:top]
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    end_ms = int(now.timestamp() * 1000) // (60 * MIN) * (60 * MIN)            # 직전 정시까지
    start_ms = end_ms - days * 24 * 60 * MIN
    print(f"[backfill upbit] 후보 {len(cands)}개 (24시간 거래대금 상위), {days}일", flush=True)
    refs, kept, calls, t0 = [], 0, 0, time.time()
    for idx, mk in enumerate(cands):
        mb, raw = {}, {}
        to = datetime.fromtimestamp(end_ms / 1000, timezone.utc)
        while True:
            rows = _upbit_get("/v1/candles/minutes/1", {"market": mk, "count": 200, "to": to.strftime("%Y-%m-%dT%H:%M:%SZ")})
            calls += 1
            time.sleep(0.11)
            if not rows:
                break
            for r in rows:
                m = int(datetime.fromisoformat(r["candle_date_time_utc"]).replace(tzinfo=timezone.utc).timestamp() * 1000)
                if start_ms <= m < end_ms:
                    v = float(r["candle_acc_trade_volume"])
                    raw[m] = (float(r["opening_price"]), float(r["high_price"]), float(r["low_price"]),
                              float(r["trade_price"]), v, (float(r["candle_acc_trade_price"]) / v) if v > 0 else None)
            oldest = min(int(datetime.fromisoformat(r["candle_date_time_utc"]).replace(tzinfo=timezone.utc).timestamp() * 1000)
                         for r in rows)
            if oldest <= start_ms:
                break
            to = datetime.fromtimestamp(oldest / 1000, timezone.utc)
        mb = {m: v[:5] for m, v in raw.items()}
        cl, h, v, st = buckets_for(mb, start_ms, end_ms)
        slots = np.array([slot_of("crypto", x) for x in st])
        sigma5, vref, fill5 = ref_from_buckets([cl[np.isfinite(cl)]], h, v, slots, 24)
        inu = bool(fill5 >= FILL_MIN and np.isfinite(sigma5) and sigma5 > 0)
        refs.append(("upbit", mk, "crypto", end_ms, days, sigma5 if np.isfinite(sigma5) else 0.0, vref, fill5, inu))
        if inu:
            kept += store.copy_bars([Bar("upbit", mk, m, "crypto", *x[:5], 0, x[5], 0) for m, x in raw.items()])
        if (idx + 1) % 10 == 0 or idx + 1 == len(cands):
            store.write_refs(refs[-((idx % 10) + 1):])
            print(f"[backfill upbit] {idx + 1}/{len(cands)}, 요청 {calls}, 넣은 봉 {kept}, "
                  f"목록 {sum(r[-1] for r in refs)}, {time.time() - t0:.0f}s", flush=True)
    print(f"[backfill upbit] 끝: 종목 목록 {sum(r[-1] for r in refs)}개", flush=True)


def upbit_gap(hours: float, store: Store) -> None:
    """실시간 수집 전후로 빈 최근 N시간을 업비트 공식 1분봉으로 메운다. 평소 값은 건드리지 않고,
    이미 있는 행(실시간으로 만든 봉)도 건드리지 않는다. 대상은 오늘 목록의 종목."""
    with store.conn.cursor() as cur:
        cur.execute("SELECT symbol FROM symbol_ref WHERE source = 'upbit' AND in_universe")
        syms = [r[0] for r in cur.fetchall()]
    store.conn.commit()
    end_ms = (int(time.time() * 1000) // MIN - 2) * MIN             # 아직 바뀔 수 있는 최근 2분은 뺀다
    start_ms = end_ms - int(hours * 60) * MIN
    kept = 0
    for mk in syms:
        rows_all, to = [], datetime.fromtimestamp(end_ms / 1000, timezone.utc)
        while True:
            rows = _upbit_get("/v1/candles/minutes/1", {"market": mk, "count": 200, "to": to.strftime("%Y-%m-%dT%H:%M:%SZ")})
            time.sleep(0.11)
            if not rows:
                break
            rows_all += rows
            oldest = min(int(datetime.fromisoformat(r["candle_date_time_utc"]).replace(tzinfo=timezone.utc).timestamp() * 1000)
                         for r in rows)
            if oldest <= start_ms:
                break
            to = datetime.fromtimestamp(oldest / 1000, timezone.utc)
        bars = []
        for r in rows_all:
            m = int(datetime.fromisoformat(r["candle_date_time_utc"]).replace(tzinfo=timezone.utc).timestamp() * 1000)
            v = float(r["candle_acc_trade_volume"])
            if start_ms <= m < end_ms:
                bars.append(Bar("upbit", mk, m, "crypto", float(r["opening_price"]), float(r["high_price"]),
                                float(r["low_price"]), float(r["trade_price"]), v, 0,
                                (float(r["candle_acc_trade_price"]) / v) if v > 0 else None, 0))
        kept += store.copy_bars(bars)
    print(f"[backfill upbit-gap] {len(syms)}종목, 최근 {hours}시간, 새로 넣은 봉 {kept}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("source", choices=["alpaca", "upbit", "upbit-gap"])
    ap.add_argument("--days", type=int, default=None)
    ap.add_argument("--top", type=int, default=150)
    ap.add_argument("--hours", type=float, default=2.0, help="upbit-gap: 최근 몇 시간을 메울지")
    a = ap.parse_args()
    store = Store()
    try:
        if a.source == "upbit-gap":
            upbit_gap(a.hours, store)
        elif a.source == "alpaca":
            alpaca(a.days or 20, store)
        else:
            upbit(a.days or 7, a.top, store)
    finally:
        store.close()


if __name__ == "__main__":
    main()
