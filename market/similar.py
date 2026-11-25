"""기능 1 조회: 지금 비슷하게 움직이는 종목 (vector_worker HTTP).

    python -m market.similar KRW-BTC
    python -m market.similar NVDA --k 15 --scope crypto
    python -m market.similar --status
"""
from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol", nargs="?")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--scope", choices=["all", "crypto", "us_equity"], default="all")
    ap.add_argument("--stale", action="store_true", help="갱신이 멈춘 종목(장 마감 등)도 포함")
    ap.add_argument("--port", type=int, default=8780)
    a = ap.parse_args()
    base = f"http://localhost:{a.port}"
    if not a.symbol:
        print(json.dumps(json.load(urllib.request.urlopen(base + "/status")), ensure_ascii=False, indent=1))
        return
    q = urllib.parse.urlencode({"symbol": a.symbol, "k": a.k, "scope": a.scope, "stale": int(a.stale)})
    r = json.load(urllib.request.urlopen(f"{base}/similar?{q}"))
    if "error" in r:
        print(f"{a.symbol}: {r['error']} ({r['reason']})")
        return
    print(f"{a.symbol} 기준 {r['as_of'][11:16]} UTC 시세 (인덱스 반영 {r['indexed_s_ago']}초 전), 후보 {r['candidates']}종목")
    for i, x in enumerate(r["results"], 1):
        print(f"{i:2d}. {x['symbol']:<12} {x['asset_class']:<9} 거리 {x['distance']:.3f}  ({x['as_of'][11:16]} 기준)")


if __name__ == "__main__":
    main()
