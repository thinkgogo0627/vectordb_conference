"""② 1분봉 정규화 로직 (Kafka·DB 없이 테스트할 수 있는 부분).

- 업비트: 체결 → 이벤트 시각(trade_timestamp) 기준 1분봉. 워터마크가 봉 끝 + 유예 시간을 지나면 확정한다.
  확정한 뒤 늦게 온 체결은 그 봉을 다시 계산해 내보낸다 (DB에서 revision이 오른다).
  같은 체결(sequential_id)이 다시 와도 한 번만 센다.
- Alpaca: 1분봉·1분봉 정정을 같은 형식으로 바꾼다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .store import Bar

MIN = 60_000


def parse_rfc3339_ms(t: str) -> int:
    base, _, frac = t.rstrip("Z").partition(".")
    return int(datetime.fromisoformat(base + "+00:00").timestamp() * 1000) + int((frac + "000")[:3] or 0)


def alpaca_bar(m: dict, recv_ts: int) -> Bar:
    return Bar("alpaca", m["S"], parse_rfc3339_ms(m["t"]), "us_equity", float(m["o"]), float(m["h"]), float(m["l"]),
               float(m["c"]), float(m["v"]), int(m["n"]), float(m["vw"]) if m.get("vw") is not None else None, recv_ts)


def alpaca_status(m: dict, recv_ts: int) -> tuple:
    return ("alpaca", m["S"], m.get("sc", ""), m.get("sm"), m.get("rc"), m.get("rm"), parse_rfc3339_ms(m["t"]), recv_ts)


@dataclass
class _Acc:
    first: tuple = (float("inf"), 0)     # (trade_ts, seq) of open
    last: tuple = (-1, 0)                # (trade_ts, seq) of close
    o: float = 0.0
    h: float = float("-inf")
    l: float = float("inf")
    c: float = 0.0
    v: float = 0.0
    pv: float = 0.0
    seen: set = field(default_factory=set)
    recv_ts: int = 0
    closed: bool = False

    def add(self, ts: int, seq: int, p: float, q: float, recv: int) -> bool:
        if seq in self.seen:
            return False
        self.seen.add(seq)
        if (ts, seq) < self.first:
            self.first, self.o = (ts, seq), p
        if (ts, seq) > self.last:
            self.last, self.c = (ts, seq), p
        self.h, self.l = max(self.h, p), min(self.l, p)
        self.v += q
        self.pv += p * q
        self.recv_ts = max(self.recv_ts, recv)
        return True


class UpbitMinuteBars:
    """체결을 받아 1분봉을 만든다.

    워터마크 = 지금까지 본 가장 늦은 체결 시각 + (그 뒤 흐른 호스트 시간) − 유예.
    호스트 시계의 '차이'만 쓰므로 호스트 시계 오차(수 초)의 영향을 받지 않는다.
    """

    def __init__(self, grace_ms: int = 5_000, keep_ms: int = 10 * MIN, start_after_ms: int | None = None):
        self.grace, self.keep = grace_ms, keep_ms
        self.accs: dict[tuple[str, int], _Acc] = {}
        self.max_ts: int | None = None
        self.recv_at_max = 0
        self.first_complete: int | None = start_after_ms   # 이 분 이전은 재시작 때문에 일부만 봤을 수 있다 → 버린다
        self.stats = {"trades": 0, "dup": 0, "late_updates": 0, "late_dropped": 0, "partial_skipped": 0,
                      "partial_dropped": 0, "sessions": 0}

    def session_start(self) -> None:
        """수집기가 다시 연결됐다. 끊기기 전에 열려 있던 봉은 끝부분을 못 봤으니 버리고,
        다시 연결된 뒤 첫 분도 앞부분을 못 봤으니 버린다 (다음 체결로 경계를 다시 정한다)."""
        self.stats["sessions"] += 1
        for key, acc in list(self.accs.items()):
            if not acc.closed:
                del self.accs[key]
                self.stats["partial_dropped"] += 1
        self.first_complete = None

    def watermark(self, now_recv: int) -> int | None:
        if self.max_ts is None:
            return None
        return self.max_ts + max(0, now_recv - self.recv_at_max) - self.grace

    def _bar(self, code: str, minute: int, a: _Acc) -> Bar:
        return Bar("upbit", code, minute, "crypto", a.o, a.h, a.l, a.c, a.v, len(a.seen),
                   a.pv / a.v if a.v > 0 else None, a.recv_ts)

    def add(self, t: dict, recv: int) -> list[Bar]:
        """체결 하나. 이미 확정한 봉에 늦게 들어온 체결이면 고친 봉을 돌려준다."""
        ts, seq = int(t["trade_timestamp"]), int(t["sequential_id"])
        code, minute = t["code"], ts - ts % MIN
        if self.first_complete is None:                 # 첫 체결: 이 분은 앞부분을 못 봤을 수 있다
            self.first_complete = (ts + self.grace) - (ts + self.grace) % MIN + MIN
        if minute < self.first_complete:
            self.stats["partial_skipped"] += 1
            return []
        if self.max_ts is None or ts > self.max_ts:
            self.max_ts, self.recv_at_max = ts, recv
        wm = self.watermark(recv)
        acc = self.accs.get((code, minute))
        if acc is None:
            if wm is not None and minute + MIN + self.keep <= wm:   # 너무 늦음: 상태를 이미 버렸다
                self.stats["late_dropped"] += 1
                return []
            acc = self.accs[(code, minute)] = _Acc()
        if not acc.add(ts, seq, float(t["trade_price"]), float(t["trade_volume"]), recv):
            self.stats["dup"] += 1
            return []
        self.stats["trades"] += 1
        if acc.closed:
            self.stats["late_updates"] += 1
            return [self._bar(code, minute, acc)]
        return []

    def advance(self, now_recv: int) -> list[Bar]:
        """워터마크가 지난 봉을 확정해 돌려주고, 오래된 상태를 버린다."""
        wm = self.watermark(now_recv)
        if wm is None:
            return []
        out = []
        for (code, minute), acc in list(self.accs.items()):
            if not acc.closed and minute + MIN <= wm:
                acc.closed = True
                out.append(self._bar(code, minute, acc))
            elif acc.closed and minute + MIN + self.keep <= wm:
                del self.accs[(code, minute)]
        out.sort(key=lambda b: (b.minute_ms, b.symbol))
        return out
