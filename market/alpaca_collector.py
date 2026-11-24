"""① Alpaca 수집기: 미국 주식 IEX 실시간 → Kafka raw.alpaca.* (키 = 종목 코드).

무료 등급 실측(2026-11-23, worklog): 1분봉·1분봉 정정·거래 상태는 전 종목(`*`)을 받을 수 있다.
체결·호가만 30종목 한도가 있다. 키 하나당 연결은 1개다.

    python -m market.alpaca_collector                          # 1분봉·정정·상태 전 종목
    python -m market.alpaca_collector --trades AAPL,NVDA,COIN  # 체결도 (최대 30)
    python -m market.alpaca_collector --test --sink jsonl --seconds 90   # 24시간 테스트 스트림(FAKEPACA)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time

import websockets

from . import config as C
from .collect import FatalError, Stats, run
from .sink import make_sink, wrap

URL = {"iex": "wss://stream.data.alpaca.markets/v2/iex", "test": "wss://stream.data.alpaca.markets/v2/test"}
CONTROL = ("success", "subscription", "error")
FATAL_CODES = {401, 402, 404, 409}     # 인증 안 됨·실패, 구독 등급 부족 → 재시도해도 같다
TRADE_LIMIT = 30


def route(m: dict) -> str | None:
    """Alpaca 메시지 하나 → 토픽. 제어 메시지(success·subscription·error)는 None."""
    return C.T_ALPACA.get(m.get("T"))


def make_session(sink, feed: str, trades: list[str]):
    key, secret = C.alpaca_keys()
    syms = ["FAKEPACA"] if feed == "test" else ["*"]
    sub = {"action": "subscribe", "bars": syms, "updatedBars": syms, "statuses": syms}
    if trades:
        sub["trades"] = trades

    async def control(m: dict) -> None:
        sink.send(C.T_COLLECTOR, "alpaca", wrap("alpaca", m))
        if m["T"] == "error":
            print(f"[alpaca] 오류 {m.get('code')}: {m.get('msg')}", flush=True)
            if m.get("code") in FATAL_CODES:
                raise FatalError(f"{m.get('code')} {m.get('msg')}")
            raise ConnectionError(f"{m.get('code')} {m.get('msg')}")   # 406 연결 수 초과 등 → 재연결
        if m["T"] == "subscription":
            print("[alpaca] 구독 " + ", ".join(f"{k} {v if v == ['*'] else len(v)}" for k, v in m.items()
                                                if k != "T" and v), flush=True)

    async def session(stats: Stats) -> None:
        async with websockets.connect(URL[feed], max_size=None, ping_interval=30) as ws:
            for m in json.loads(await ws.recv()):
                await control(m)
            await ws.send(json.dumps({"action": "auth", "key": key, "secret": secret}))
            for m in json.loads(await asyncio.wait_for(ws.recv(), 10)):
                await control(m)
            await ws.send(json.dumps(sub))
            async for raw in ws:
                recv = int(time.time() * 1000)
                for m in json.loads(raw):
                    topic = route(m)
                    if topic is None:
                        await control(m)
                        continue
                    sink.send(topic, m.get("S"), wrap("alpaca", m, recv))
                    stats.hit(topic, recv)
    return session


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sink", choices=["kafka", "jsonl"], default="kafka")
    ap.add_argument("--seconds", type=float, default=None)
    ap.add_argument("--test", action="store_true", help="24시간 테스트 스트림 (FAKEPACA)")
    ap.add_argument("--trades", default="", help=f"체결을 받을 종목, 쉼표 구분 (최대 {TRADE_LIMIT})")
    a = ap.parse_args()
    trades = [s.strip().upper() for s in a.trades.split(",") if s.strip()]
    if len(trades) > TRADE_LIMIT:
        raise SystemExit(f"--trades는 최대 {TRADE_LIMIT}종목 (무료 등급)")
    sink = make_sink(a.sink)
    try:
        asyncio.run(run("alpaca", make_session(sink, "test" if a.test else "iex", trades), sink, a.seconds))
    except (KeyboardInterrupt, FatalError):
        pass
    finally:
        sink.close()


if __name__ == "__main__":
    main()
