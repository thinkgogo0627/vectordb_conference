"""수집기 출력: Kafka(기본) 또는 JSONL 파일(Kafka 없이 확인할 때)."""
from __future__ import annotations

import json
import time
from pathlib import Path

from . import config as C


def wrap(src: str, data: dict, recv_ms: int | None = None) -> dict:
    return {"src": src, "recv_ts": recv_ms if recv_ms is not None else int(time.time() * 1000), "data": data}


class KafkaSink:
    def __init__(self, bootstrap: str = C.KAFKA):
        from confluent_kafka import Producer
        self.p = Producer({"bootstrap.servers": bootstrap, "linger.ms": 5, "compression.type": "lz4",
                           "enable.idempotence": True})
        self.errors = 0

    def _cb(self, err, _msg) -> None:
        if err is not None:
            self.errors += 1

    def send(self, topic: str, key: str | None, value: dict) -> None:
        while True:
            try:
                self.p.produce(topic, json.dumps(value, ensure_ascii=False).encode(), key.encode() if key else None,
                               on_delivery=self._cb)
                break
            except BufferError:          # 로컬 큐가 가득 차면 비울 때까지 기다린다
                self.p.poll(0.1)
        self.p.poll(0)

    def close(self) -> None:
        self.p.flush(10)


class JsonlSink:
    """data/market/raw/<topic>.jsonl 에 한 줄씩 쓴다."""

    def __init__(self, root: Path = C.ROOT / "data" / "market" / "raw"):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self.files: dict[str, object] = {}
        self.errors = 0

    def send(self, topic: str, key: str | None, value: dict) -> None:
        f = self.files.get(topic)
        if f is None:
            f = self.files[topic] = open(self.root / f"{topic}.jsonl", "a", encoding="utf-8")
        f.write(json.dumps({"key": key, **value}, ensure_ascii=False) + "\n")

    def close(self) -> None:
        for f in self.files.values():
            f.close()


def make_sink(kind: str):
    return KafkaSink() if kind == "kafka" else JsonlSink()
