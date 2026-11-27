"""같은 기초자산 묶음 (설계 4절): 사실상 같은 것을 추종하는 종목 쌍. 기능 1이 결과에서 접는다.

    python -m market.pairs build --days 20     # symbol_pair 갱신 (과거 데이터 채우기 뒤, 벡터 워커 시작 전)
    python -m market.pairs show KRW-BTC        # 한 종목의 짝

- **기본은 손으로 정한 상품군 목록**(`same_underlying_manual.json`)이다. 한 묶음 안 종목끼리는 모두 같은 기초자산이다.
  - 2026-11-27 서버 실측: 상관만으로는 가를 수 없었다.
    - IEX 표본 잡음 때문에 진짜 짝이 0.93 근처로 깎였다 (FBTC ↔ BTC 0.936, QID ↔ SQQQ 0.926).
    - 다른 지수끼리는 0.95를 넘었다 (QQQ ↔ SPY 0.950, SPY ↔ TQQQ 0.953).
- 상관(직전 거래일들의 미국 정규장 5분 수익률, 코인도 같은 시각)은 두 가지에만 쓴다.
  - |상관| >= AUTO_CORR(0.98)이면 목록에 없어도 자동으로 묶는다 (같은 지수를 추종하는 ETF처럼 거의 같은 상품).
  - |상관| >= REVIEW_CORR(0.85)인데 목록에 없는 쌍은 검토 후보로 출력한다. 사람이 보고 목록에 넣는다.
- 묶음을 사슬로 잇지 않는다. 목록 묶음과 자동 쌍만 같은 기초자산이다.
- 구성종목과 그 ETF(엔비디아 ↔ 반도체 ETF)는 기준이 애매해서 묶지 않는다.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from .store import Store

AUTO_CORR = 0.98            # 목록에 없어도 자동으로 묶는 상관
REVIEW_CORR = 0.85          # 검토 후보로 보여 줄 상관
STORE_MIN = 0.8             # 이 이상인 상관은 저장해 둔다 (show용)
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
    """|상관| >= thr인 쌍 (양방향): (source_a, a, source_b, b, corr, n)."""
    out = []
    ii, jj = np.where(np.abs(np.nan_to_num(c)) >= thr)
    for i, j in zip(ii, jj):
        out.append((*keys[i], *keys[j], float(c[i, j]), int(n[i, j])))
    return out


def load_manual(path: Path = MANUAL) -> dict:
    if not path.exists():
        return {"groups": [], "not_same": []}
    return json.loads(path.read_text(encoding="utf-8"))


def manual_pairs(groups: list[dict], src_of: dict[str, str]):
    """상품군 → 같은 기초자산 쌍 (양방향 키). 데이터에 없는 종목은 따로 돌려준다."""
    out, missing = set(), []
    for g in groups:
        have = [(src_of[s], s) for s in g["symbols"] if s in src_of]
        missing += [s for s in g["symbols"] if s not in src_of]
        for a in have:
            for b in have:
                if a != b:
                    out.add((a, b))
    return out, missing


def decide(corr_rows, manual: set, not_same: set):
    """상관 쌍과 상품군을 합쳐 행을 만든다. 반환: {(ka, kb): (corr|None, n, manual, same)}."""
    rows: dict = {}
    for sa, a, sb, b, c, n in corr_rows:
        rows[((sa, a), (sb, b))] = [c, n, False]
    for ka, kb in manual:
        rows.setdefault((ka, kb), [None, 0, False])[2] = True
    out = {}
    for (ka, kb), (c, n, man) in rows.items():
        same = man or (c is not None and abs(c) >= AUTO_CORR)
        if (ka[1], kb[1]) in not_same:
            same = False
        out[(ka, kb)] = (c, n, man, same)
    return out


def build(days: int, store: Store) -> None:
    man = load_manual()
    not_same = {tuple(p) for p in man.get("not_same", [])}
    not_same |= {(b, a) for a, b in not_same}
    with store.conn.cursor() as cur:
        cur.execute("SELECT symbol, source FROM symbol_ref")
        src_of = dict(cur.fetchall())
        cur.execute("""SELECT DISTINCT (minute_ts AT TIME ZONE 'America/New_York')::date d FROM bars_1m
                       WHERE source = 'alpaca' AND minute_ts > now() - make_interval(days => %s) ORDER BY d""",
                    (int(days * 7 / 5) + 10,))
        dates = [r[0] for r in cur.fetchall()][-days:]
        rows = []
        if dates:
            from .features import us_session_ms
            sessions = [us_session_ms(int(datetime(d.year, d.month, d.day, 15, tzinfo=timezone.utc).timestamp() * 1000))
                        for d in dates]
            t0 = datetime.fromtimestamp(sessions[0][0] / 1000, timezone.utc)
            cur.execute("""SELECT b.source, b.symbol,
                                  (floor(extract(epoch FROM b.minute_ts) / 300) * 300000)::bigint AS bk,
                                  (array_agg(b.c ORDER BY b.minute_ts DESC))[1]
                           FROM bars_1m b JOIN symbol_ref r ON r.source = b.source AND r.symbol = b.symbol AND r.in_universe
                           WHERE b.minute_ts >= %s GROUP BY 1, 2, 3""", (t0,))
            rows = cur.fetchall()
    store.conn.commit()

    corr_rows, c = [], None
    if dates:
        print(f"[pairs] 거래일 {len(dates)} ({dates[0]} ~ {dates[-1]})", flush=True)
        keys, R = returns_grid(rows, sessions)
        c, n = corr_matrix(R)
        corr_rows = same_pairs(keys, c, n)
    else:
        print("[pairs] 미국 주식 1분봉이 없다 (Alpaca 과거 데이터 전). 상관 없이 상품군 목록만 쓴다.", flush=True)
    mp, missing = manual_pairs(man.get("groups", []), src_of)
    dec = decide(corr_rows, mp, not_same)

    asof = datetime.now(timezone.utc)
    with store.conn.cursor() as cur:
        cur.execute("DELETE FROM symbol_pair")
        cur.executemany("""INSERT INTO symbol_pair (source_a, symbol_a, source_b, symbol_b, corr, n, manual, same, asof)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        [(*ka, *kb, cc, nn, m, s, asof) for (ka, kb), (cc, nn, m, s) in dec.items()])
    store.conn.commit()

    # 요약
    if c is not None:
        a = np.abs(c[np.triu_indices_from(c, 1)])
        a = a[np.isfinite(a)]
        print(f"[pairs] 상관을 잰 쌍 {len(a)}: " + ", ".join(f">= {t} {int((a >= t).sum())}쌍" for t in (0.85, 0.9, 0.95, 0.98)),
              flush=True)
    half = {tuple(sorted(k)): v for k, v in dec.items()}
    n_man = sum(1 for v in half.values() if v[2] and v[3])
    auto = sorted(((k, v) for k, v in half.items() if v[3] and not v[2]), key=lambda x: -abs(x[1][0]))
    print(f"[pairs] 같은 기초자산 {sum(1 for v in half.values() if v[3])}쌍 = 상품군 목록 {n_man} + 자동(|상관| >= {AUTO_CORR}) {len(auto)}",
          flush=True)
    for (ka, kb), (cc, *_rest) in auto:
        print(f"           자동  {ka[1]:<10} {kb[1]:<10} {cc:+.3f}", flush=True)
    if missing:
        print(f"[pairs] 목록에 있지만 데이터에 없는 종목 {len(missing)}개 (무시): {', '.join(missing)}", flush=True)
    review = sorted(((k, v) for k, v in half.items()
                     if not v[3] and v[0] is not None and abs(v[0]) >= REVIEW_CORR
                     and (k[0][1], k[1][1]) not in not_same), key=lambda x: -abs(x[1][0]))
    print(f"[pairs] 검토 후보 {len(review)}쌍 (|상관| >= {REVIEW_CORR}, 목록에 없음). 같은 상품이면 상품군에, 아니면 그대로 둔다:",
          flush=True)
    for (ka, kb), (cc, *_rest) in review:
        print(f"           {ka[1]:<10} {kb[1]:<10} {cc:+.3f}", flush=True)


def load(store: Store) -> dict[tuple[str, str], dict[tuple[str, str], float | None]]:
    """같은 기초자산 쌍만: {종목: {짝: 상관(없으면 None)}}."""
    out: dict = defaultdict(dict)
    with store.conn.cursor() as cur:
        cur.execute("SELECT source_a, symbol_a, source_b, symbol_b, corr FROM symbol_pair WHERE same")
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
        return
    with store.conn.cursor() as cur:
        cur.execute("""SELECT symbol_b, source_b, corr, manual, same FROM symbol_pair WHERE symbol_a = %s
                       ORDER BY same DESC, abs(coalesce(corr, 1)) DESC""", (a.symbol,))
        for b_, sb, c, m, sm in cur.fetchall():
            why = "같은 기초자산 (상품군)" if sm and m else "같은 기초자산 (자동)" if sm else ""
            print(f"{b_:<12} {sb:<7} {'  -   ' if c is None else f'{c:+.3f}'} {why}")


if __name__ == "__main__":
    main()
