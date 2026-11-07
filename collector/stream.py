"""Wikimedia EventStreams(SSE) 구독.

- 연결이 끊기면(서버가 15분마다 끊음) 마지막 이벤트 ID로 이어서 받는다.
- 마지막 ID는 디스크에 저장돼서, 컨테이너가 재시작돼도 빈 구간 없이 이어진다
  (스트림 보관 기간 안이라면).
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Iterator

import requests

log = logging.getLogger(__name__)


class StateStore:
    """마지막으로 '안전하게 저장된' 이벤트 ID를 파일에 보관."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def load(self) -> str | None:
        try:
            with open(self.path, encoding="utf-8") as f:
                return f.read().strip() or None
        except FileNotFoundError:
            return None

    def save(self, last_event_id: str) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(last_event_id)
        os.replace(tmp, self.path)  # 원자적 교체


def parse_sse(lines: Iterator[str]) -> Iterator[tuple[str | None, str]]:
    """SSE 텍스트 라인 → (event_id, data) 묶음."""
    event_id: str | None = None
    data: list[str] = []
    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data:
                yield event_id, "\n".join(data)
            event_id, data = None, []
            continue
        if line.startswith(":"):
            continue  # 주석/heartbeat
        key, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if key == "id":
            event_id = value
        elif key == "data":
            data.append(value)


def stream_events(url: str, user_agent: str, last_event_id: str | None = None,
                  stop=lambda: False) -> Iterator[tuple[str | None, dict]]:
    """끊기면 지수 백오프로 재연결하면서 (event_id, event dict)를 계속 낸다."""
    backoff = 1.0
    while not stop():
        headers = {"Accept": "text/event-stream", "User-Agent": user_agent}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        try:
            with requests.get(url, headers=headers, stream=True, timeout=(10, 120)) as r:
                r.raise_for_status()
                log.info("stream connected (resume=%s)", bool(last_event_id))
                backoff = 1.0
                for eid, data in parse_sse(r.iter_lines(decode_unicode=True)):
                    if stop():
                        return
                    try:
                        evt = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    if eid:
                        last_event_id = eid
                    yield eid, evt
        except (requests.RequestException, ValueError) as e:
            log.warning("stream error: %s (retry in %.0fs)", e, backoff)
        if stop():
            return
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)
