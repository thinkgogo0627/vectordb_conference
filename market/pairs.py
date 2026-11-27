"""같은 기초자산 묶음 (설계 4절): 사실상 같은 것을 추종하는 종목 쌍을 데이터로 찾는다.

    python -m market.pairs build --days 20     # symbol_pair 테이블 갱신 (과거 데이터 채우기 뒤, 벡터 워커 시작 전)
    python -m market.pairs show KRW-BTC        # 한 종목의 짝

- 직전 거래일들의 미국 정규장 5분 수익률로 종목 쌍의 상관을 잰다. 코인도 같은 시각의 5분 수익률을 쓴다.
- |상관| >= SAME_CORR이면 같은 기초자산으로 본다. 현물 ETF(BTC ↔ IBIT), 레버리지·인버스(NVDA ↔ NVDL)가 여기 든다.
  우리 벡터는 종목마다 자기 평소 변동성으로 나누므로 2배 레버리지와 원본의 벡터가 거의 같아진다. 그래서 접지 않으면
  기능 1의 상위를 늘 이런 쌍이 차지한다.
- 묶음을 사슬로 잇지 않는다 (A~B, B~C라도 A~C는 따로 본다). 지수 ETF끼리 사슬로 이어져 큰 덩어리가 되는 것을 막는다.
- 구성종목과 그 ETF(엔비디아 ↔ 반도체 ETF)는 기준이 애매해서 묶지 않는다. 상관이 기준값보다 낮으므로 대개 저절로 빠진다.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from .store import Store

SAME_CORR = 0.95            # 같은 기초자산 기준. 실측 분포를 보고 정한다 (worklog)
STORE_MIN = 0.8             # 이 이상만 저장해 두고 기준값은 읽을 때 적용한다
MIN_N = 200                 # 겹친 5분 수익률이 이보다 적으면 상관을 믿지 않는다 (약 2.5거래일)
MANUAL = Path(__file__).with_name("same_underlying_manual.json")


def corr_matrix(R: np.ndarray, min_n: int = MIN_N) -> tuple[np.ndarray, np.ndarray]:
    """R: (시점, 종목) 수익률, 없는 값은 NaN. 겹친 시점끼리만 쓰는 상관(평균 0 가정)과 겹친 수."""
    M = np.isfinite(R).astype(np.float64)
    X = np.where(M > 0, R, 0.0)
    num = X.T @ X
    sq = (X * X).T @ M                       # sq[i, j] = 종목 i 제곱합 (j와 겹친 시점만)
    n = M.T @ M
    den = np.sqrt(sq * sq.T)
    with np.errstate(divide="ignore", invalid="ignore"):
        c = np.where((den > 0) & (n >= min_n), num / den, np.nan)
    np.fill_diagonal(c, np.nan)
    return c, n


def returns_grid(rows, sessions: list[tuple[int, int]], bucket_ms: int = 300_000):
    """rows: (source, symbol, bucket_start_ms, close). 정규장 5분 격자 위의 로그수익률 (시점, 종목).
    빈 묶음은 직전 종가로 채우고(수익률 0), 장 첫 묶음과 첫 체결 전은 NaN."""
    grid = [b for o, c in sessions for b in range(o, c, bucket_ms)]
    pos = {b: i for i, b in enumerate(grid)}
    keys = sorted({(s, sym) for s, sym, _, _ in rows})
    col = {k: j for j, k in enumerate(keys)}
    P = np.full((len(grid), len(keys)), np.nan)
    for s, sym, b, c in rows:
        i = pos.get(b)
        if i is not None and c and c > 0:
            P[i, col[(s, sym)]] = c
    R = np.full_like(P, np.nan)
    for o, c in sessions:
        i0, i1 = pos[o], pos[o] + (c - o) // bucket_ms
        seg = P[i0:i1].copy()
        for t in range(1, len(seg)):                       # 장 안에서만 직전 값 채우기
            miss = ~np.isfinite(seg[t])
            seg[t, miss] = seg[t - 1, miss]
        with np.errstate(divide="ignore", invalid="ignore"):
            R[i0 + 1:i1] = np.diff(np.log(seg), axis=0)
    return keys, R


def same_pairs(keys, c, n, thr: float = STORE_MIN):
    out = []
    ii, jj = np.where(np.abs(np.nan_to_num(c)) >= thr)
    for i, j in zip(ii, jj):
        out.append((*keys[i], *keys[j], float(c[i, j]), int(n[i, j])))
    return out


def load_manual() -> dict:
    if not MANUAL.exists():
        return {"same": [], "not_same": []}
    return json.loads(MANUAL.read_text(encoding="utf-8"))


def build(days: int, store: Store) -> None:
    with store.conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT (minute_ts AT TIME ZONE 'America/New_York')::date d FROM bars_1m
                       WHERE source = 'alpaca' AND minute_ts > now() - make_interval(days => %s) ORDER BY d""",
                    (int(days * 7 / 5) + 10,))
        dates = [r[0] for r in cur.fetchall()][-days:]
        if not dates:
            store.conn.commit()
            print("[pairs] 미국 주식 1분봉이 없다 (Alpaca 과거 데이터 전). 같은 시각 비교 기준이 없어 쌍을 만들지 않는다.", flush=True)
            return
        from .features import us_session_ms
        sessions = [us_session_ms(int(datetime(d.year, d.month, d.day, 15, tzinfo=timezone.utc).timestamp() * 1000))
                    for d in dates]
        print(f"[pairs] 거래일 {len(dates)} ({dates[0]} ~ {dates[-1]})", flush=True)
        t0 = datetime.fromtimestamp(sessions[0][0] / 1000, timezone.utc)
        cur.execute("""SELECT b.source, b.symbol,
                              (floor(extract(epoch FROM b.minute_ts) / 300) * 300000)::bigint AS bk,
                              (array_agg(b.c ORDER BY b.minute_ts DESC))[1]
                       FROM bars_1m b JOIN symbol_ref r ON r.source = b.source AND r.symbol = b.symbol AND r.in_universe
                       WHERE b.minute_ts >= %s GROUP BY 1, 2, 3""", (t0,))
        rows = cur.fetchall()
    store.conn.commit()
    keys, R = returns_grid(rows, sessions)
    c, n = corr_matrix(R)
    pairs = same_pairs(keys, c, n)
    man = load_manual()
    drop = {tuple(p) for p in man.get("not_same", [])} | {tuple(reversed(p)) for p in man.get("not_same", [])}
    pairs = [p for p in pairs if (p[1], p[3]) not in drop]
    have = {(p[1], p[3]) for p in pairs}
    src = {sym: s for s, sym in keys}
    for a, b in man.get("same", []):
        for x, y in ((a, b), (b, a)):
            if (x, y) not in have and x in src and y in src:
                pairs.append((src[x], x, src[y], y, 1.0, 0))
    asof = datetime.now(timezone.utc)
    with store.conn.cursor() as cur:
        cur.execute("DELETE FROM symbol_pair")
        cur.executemany("INSERT INTO symbol_pair VALUES (%s, %s, %s, %s, %s, %s, %s)", [(*p, asof) for p in pairs])
    store.conn.commit()
    a = np.abs(c[np.triu_indices_from(c, 1)])
    a = a[np.isfinite(a)]
    print(f"[pairs] 종목 {len(keys)}, 상관을 잰 쌍 {len(a)}", flush=True)
    for t in (0.8, 0.9, 0.95, 0.98):
        print(f"[pairs]   |상관| >= {t}: {int((a >= t).sum())}쌍", flush=True)
    top = sorted({tuple(sorted([(p[0], p[1]), (p[2], p[3])])) + (p[4],) for p in pairs if p[5]},
                 key=lambda x: -abs(x[2]))
    print(f"[pairs] 저장 {len(pairs) // 2}쌍 (|상관| >= {STORE_MIN}). 0.9 근처 예시:", flush=True)
    near = [x for x in top if 0.88 <= abs(x[2]) < 0.97][:25]
    for (sa, a_), (sb, b_), cc in near:
        print(f"           {a_:<10} {b_:<10} {cc:+.3f}", flush=True)


def load(store: Store) -> dict[tuple[str, str], dict[tuple[str, str], float]]:
    out: dict = defaultdict(dict)
    with store.conn.cursor() as cur:
        cur.execute("SELECT source_a, symbol_a, source_b, symbol_b, corr FROM symbol_pair")
        for sa, a, sb, b, c in cur.fetchall():
            out[(sa, a)][(sb, b)] = c
    store.conn.commit()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--days", type=int, default=20)
    s = sub.add_parser("show")
    s.add_argument("symbol")
    a = ap.parse_args()
    store = Store()
    if a.cmd == "build":
        build(a.days, store)
    else:
        for (src, sym), d in load(store).items():
            if sym == a.symbol:
                for (sb, b_), c in sorted(d.items(), key=lambda x: -abs(x[1])):
                    mark = "같은 기초자산" if abs(c) >= SAME_CORR else ""
                    print(f"{b_:<12} {sb:<7} {c:+.3f} {mark}")


if __name__ == "__main__":
    main()
