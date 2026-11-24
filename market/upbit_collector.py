"""① 업비트 수집기: 원화 마켓 전 종목 체결(WebSocket) → Kafka raw.upbit.trade (키 = 종목 코드).

키가 필요 없다. 종목 목록은 연결할 때마다 REST로 다시 받는다 (신규 상장 반영).

    python -m market.upbit_collector                      # Kafka로, 멈출 때까지
    python -m market.upbit_collector --sink jsonl --seconds 60   # data/market/raw/*.jsonl 로 1분
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
import urllib.request
import uuid

import websockets

from . import config as C
from .collect import Stats, run
from .sink import make_sink, wrap

WS_URL = "wss://api.upbit.com/websocket/v1"
MARKETS_URL = "https://api.upbit.com/v1/market/all?is_details=false"


def krw_markets() -> list[str]:
    with urllib.request.urlopen(MARKETS_URL, timeout=10) as r:
        return sorted(m["market"] for m in json.load(r) if m["market"].startswith("KRW-"))


def make_session(sink):
    async def session(stats: Stats) -> None:
        codes = krw_markets()
        async with websockets.connect(WS_URL, max_size=None, ping_interval=30) as ws:
            await ws.send(json.dumps([{"ticket": str(uuid.uuid4())},
                                      {"type": "trade", "codes": codes, "is_only_realtime": True},
                                      {"format": "DEFAULT"}]))
            sink.send(C.T_COLLECTOR, "upbit", wrap("upbit", {"event": "subscribed", "markets": len(codes)}))
            # 같은 토픽에 세션 시작 표지를 넣는다. bar_builder는 이 표지로 "끊김 전후의 봉은 일부만 봤다"는 것을 안다.
            sink.send(C.T_UPBIT_TRADE, None, wrap("upbit", {"type": C.SESSION_START, "markets": len(codes)}))
            print(f"[upbit] 연결, 원화 마켓 {len(codes)}개 구독", flush=True)
            async for raw in ws:
                recv = int(time.time() * 1000)
                m = json.loads(raw)
                if m.get("type") != "trade":
                    continue
                sink.send(C.T_UPBIT_TRADE, m["code"], wrap("upbit", m, recv))
                stats.hit(C.T_UPBIT_TRADE, recv)
    return session


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sink", choices=["kafka", "jsonl"], default="kafka")
    ap.add_argument("--seconds", type=float, default=None, help="이 시간만 돌고 끝낸다 (없으면 계속)")
    a = ap.parse_args()
    sink = make_sink(a.sink)
    try:
        asyncio.run(run("upbit", make_session(sink), sink, a.seconds))
    except KeyboardInterrupt:
        pass
    finally:
        sink.close()


if __name__ == "__main__":
    main()
