"""market 수집기의 네트워크 없는 부분: 메시지 라우팅, 원천 시각 파싱, 재연결 루프."""
import asyncio

from market import config as C
from market.alpaca_collector import route
from market.collect import FatalError, run
from market.peek import source_ms


class MemSink:
    def __init__(self):
        self.out = []

    def send(self, topic, key, value):
        self.out.append((topic, key, value))


def test_route_alpaca():
    assert route({"T": "b", "S": "AAPL"}) == "raw.alpaca.bar"
    assert route({"T": "u", "S": "AAPL"}) == "raw.alpaca.bar_update"
    assert route({"T": "s", "S": "AAPL"}) == "raw.alpaca.status"
    assert route({"T": "x"}) == route({"T": "c"}) == "raw.alpaca.trade_fix"
    assert route({"T": "subscription"}) is None and route({"T": "error"}) is None


def test_source_ms():
    assert source_ms({"src": "upbit", "data": {"trade_timestamp": 1795444200123}}) == 1795444200123
    assert source_ms({"src": "alpaca", "data": {"t": "2026-11-23T14:30:00Z"}}) == 1795444200000
    assert source_ms({"src": "alpaca", "data": {"T": "b", "t": "2026-11-23T14:30:00Z"}}) == 1795444260000
    assert source_ms({"src": "alpaca", "data": {"t": "2026-11-23T14:12:33.360839328Z"}}) == 1795443153360


def test_run_reconnects_and_records_gaps():
    sink, calls = MemSink(), []

    async def session(stats):
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("drop")
        await asyncio.sleep(10)          # 세 번째 연결은 시간이 끝날 때까지 유지

    asyncio.run(run("t", session, sink, duration=0.5, report_every=60, max_backoff=0.01))
    events = [v["data"]["event"] for t, _, v in sink.out if t == C.T_COLLECTOR]
    assert len(calls) == 3
    assert events.count("disconnected") == 2 and events[-1] == "stopped"


def test_run_stops_on_fatal():
    sink = MemSink()

    async def session(stats):
        raise FatalError("401 not authenticated")

    try:
        asyncio.run(run("t", session, sink, duration=5, report_every=60))
        raise AssertionError("FatalError가 전파되어야 한다")
    except FatalError:
        pass
    assert [v["data"]["event"] for _, _, v in sink.out][-2:] == ["fatal", "stopped"]
