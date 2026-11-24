"""업비트 1분봉 검증: bars_1m(우리가 체결로 만든 봉)과 업비트 공식 1분봉(REST)을 비교한다.

    python -m market.verify_upbit --since "2026-11-24T16:30:00Z" --until "2026-11-24T16:34:00Z" --markets 20

비교 범위는 수집기가 쭉 연결돼 있던 구간 안쪽으로 준다 (경계의 분은 일부만 봤을 수 있다).
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from .store import Store

FIELDS = (("o", "opening_price"), ("h", "high_price"), ("l", "low_price"), ("c", "trade_price"),
          ("v", "candle_acc_trade_volume"))


def rest_candles(market: str, until_utc: datetime, count: int) -> dict[datetime, dict]:
    q = urllib.parse.urlencode({"market": market, "count": count, "to": until_utc.strftime("%Y-%m-%dT%H:%M:%SZ")})
    with urllib.request.urlopen(f"https://api.upbit.com/v1/candles/minutes/1?{q}", timeout=10) as r:
        rows = json.load(r)
    return {datetime.fromisoformat(x["candle_date_time_utc"]).replace(tzinfo=timezone.utc): x for x in rows}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True)
    ap.add_argument("--until", required=True, help="이 시각 이전에 시작한 봉까지")
    ap.add_argument("--markets", type=int, default=20, help="거래가 많은 순서로 몇 종목")
    a = ap.parse_args()
    since = datetime.fromisoformat(a.since.replace("Z", "+00:00"))
    until = datetime.fromisoformat(a.until.replace("Z", "+00:00"))
    st = Store()
    with st.conn.cursor() as cur:
        cur.execute("""SELECT symbol, count(*) FROM bars_1m WHERE source='upbit' AND minute_ts >= %s AND minute_ts < %s
                       GROUP BY symbol ORDER BY sum(n) DESC LIMIT %s""", (since, until, a.markets))
        markets = [r[0] for r in cur.fetchall()]
        ours = {}
        cur.execute("""SELECT symbol, minute_ts, o, h, l, c, v FROM bars_1m WHERE source='upbit'
                       AND minute_ts >= %s AND minute_ts < %s AND symbol = ANY(%s)""", (since, until, markets))
        for s, m, *vals in cur.fetchall():
            ours[(s, m)] = dict(zip("ohlcv", vals))
    st.close()
    n_min = int((until - since).total_seconds() // 60)
    total = match = missing_ours = missing_rest = 0
    bad = []
    for mk in markets:
        rest = {m: x for m, x in rest_candles(mk, until, n_min + 2).items() if since <= m < until}
        time.sleep(0.15)                                 # REST 속도 제한 여유
        for m in sorted(set(rest) | {k[1] for k in ours if k[0] == mk}):
            r, o = rest.get(m), ours.get((mk, m))
            if r is None:
                missing_rest += 1
                continue
            if o is None:
                missing_ours += 1
                bad.append((mk, m, "우리 쪽에 없음"))
                continue
            total += 1
            diff = [k for k, rk in FIELDS if abs(o[k] - float(r[rk])) > 1e-9 * max(1.0, abs(float(r[rk])))]
            if diff:
                bad.append((mk, m, {k: (o[k], float(r[rk])) for k, rk in FIELDS if k in diff}))
            else:
                match += 1
    print(f"종목 {len(markets)}개, {since:%H:%M}~{until:%H:%M} UTC: 비교 {total}봉, 일치 {match}, "
          f"우리 쪽에 없음 {missing_ours}, 공식 쪽에 없음 {missing_rest}")
    for b in bad[:10]:
        print("  차이:", b)


if __name__ == "__main__":
    main()
