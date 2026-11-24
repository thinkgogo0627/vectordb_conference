"""수집기 공통 루프: 연결 → 세션 → 끊기면 재연결(지수 백오프). 끊긴 구간을 raw.collector.events에 남긴다."""
from __future__ import annotations

import asyncio
import time
from collections import Counter
from typing import Awaitable, Callable

from . import config as C


class Stats:
    def __init__(self, src: str):
        self.src, self.n, self.last_msg_ms, self.t0 = src, Counter(), None, time.time()

    def hit(self, topic: str, recv_ms: int) -> None:
        self.n[topic] += 1
        self.last_msg_ms = recv_ms

    def line(self) -> str:
        dt = max(1e-9, time.time() - self.t0)
        per = ", ".join(f"{t.split('.', 1)[1]} {c} ({c / dt:.1f}/s)" for t, c in sorted(self.n.items()))
        return f"[{self.src}] {dt:.0f}s {per}"


class FatalError(Exception):
    """재시도해도 소용없는 오류 (인증 실패 등)."""


async def run(src: str, session: Callable[[Stats], Awaitable[None]], sink, duration: float | None,
              report_every: float = 10.0, max_backoff: float = 60.0) -> Stats:
    """session(stats)는 연결 하나의 수명 동안 돌고, 끊기면 예외를 던지거나 반환한다."""
    stats = Stats(src)
    t_end = time.time() + duration if duration else None

    def event(kind: str, **kw) -> None:
        sink.send(C.T_COLLECTOR, src, {"src": src, "recv_ts": int(time.time() * 1000), "data": {"event": kind, **kw}})

    async def reporter() -> None:
        while True:
            await asyncio.sleep(report_every)
            print(stats.line(), flush=True)

    rep = asyncio.create_task(reporter())
    backoff = min(1.0, max_backoff)
    try:
        while t_end is None or time.time() < t_end:
            gap_from = stats.last_msg_ms
            event("connecting", last_msg_ts=gap_from)
            try:
                remaining = None if t_end is None else max(0.1, t_end - time.time())
                await asyncio.wait_for(session(stats), timeout=remaining)
                err = "session ended"
            except asyncio.TimeoutError:
                break                                   # 정해진 시간이 끝남
            except FatalError as e:
                event("fatal", error=str(e))
                print(f"[{src}] 중단: {e}", flush=True)
                raise
            except Exception as e:                      # 네트워크 끊김 등 → 재연결
                err = f"{type(e).__name__}: {e}"
            event("disconnected", error=err, last_msg_ts=stats.last_msg_ms)
            if stats.last_msg_ms != gap_from:           # 이번 연결에서 데이터를 받았으면 백오프를 처음부터
                backoff = min(1.0, max_backoff)
            wait = backoff if t_end is None else min(backoff, max(0.0, t_end - time.time()))
            print(f"[{src}] 끊김 ({err}), {wait:.1f}초 뒤 재연결", flush=True)
            await asyncio.sleep(wait)
            backoff = min(max_backoff, backoff * 2)
    finally:
        rep.cancel()
        event("stopped", last_msg_ts=stats.last_msg_ms)
        print(stats.line(), flush=True)
    return stats
