"""raw.* 토픽 확인: 토픽별 메시지 수, 종목 수, 수신 지연(호스트 시각 − 원천 시각), 예시 한 건.

    python -m market.peek                  # Kafka의 raw.* 전부
    python -m market.peek --jsonl          # data/market/raw/*.jsonl

수신 지연은 호스트 시계 오차가 그대로 섞인다 (메모: 11-23에 약 +6초). 비교는 같은 시계끼리만 한다.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np

from . import config as C


def source_ms(value: dict) -> int | None:
    d = value.get("data", {})
    if value.get("src") == "upbit" and "trade_timestamp" in d:
        return int(d["trade_timestamp"])
    t = d.get("t")
    if isinstance(t, str):                       # Alpaca RFC-3339 (나노초까지 올 수 있다)
        base, _, frac = t.rstrip("Z").partition(".")
        dt = datetime.fromisoformat(base + "+00:00")
        ms = int(dt.timestamp() * 1000) + int((frac + "000")[:3] or 0)
        return ms + 60_000 if d.get("T") in ("b", "u") else ms   # 1분봉의 t는 시작 시각 → 확정 시각 기준
    return None


def summarize(rows: dict[str, list[tuple[str | None, dict]]]) -> None:
    for topic in sorted(rows):
        items = rows[topic]
        keys = {k for k, _ in items if k}
        lag = [v["recv_ts"] - s for _, v in items if (s := source_ms(v)) is not None]
        lag_s = (f"수신−원천 p50 {np.percentile(lag, 50) / 1000:.2f}s p95 {np.percentile(lag, 95) / 1000:.2f}s"
                 if lag else "")
        print(f"{topic}: {len(items)}건, 키 {len(keys)}개 {lag_s}")
        print("   예:", json.dumps(items[-1][1], ensure_ascii=False)[:260])


def from_kafka() -> dict:
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": C.KAFKA, "group.id": "market-peek", "enable.auto.commit": False})
    topics = [t for t in c.list_topics(timeout=10).topics if t.startswith("raw.")]
    rows: dict = defaultdict(list)
    for t in topics:
        parts = c.list_topics(t, timeout=10).topics[t].partitions
        for p in parts:
            lo, hi = c.get_watermark_offsets(TopicPartition(t, p), timeout=10)
            if hi <= lo:
                continue
            c.assign([TopicPartition(t, p, lo)])
            while True:
                msgs = c.consume(1000, timeout=2)
                for m in msgs:
                    if not m.error():
                        rows[t].append((m.key().decode() if m.key() else None, json.loads(m.value())))
                if not msgs or msgs[-1].offset() >= hi - 1:
                    break
    c.close()
    return rows


def from_jsonl() -> dict:
    rows: dict = defaultdict(list)
    for f in sorted((C.ROOT / "data" / "market" / "raw").glob("*.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            v = json.loads(line)
            rows[f.stem].append((v.pop("key", None), v))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", action="store_true")
    summarize(from_jsonl() if ap.parse_args().jsonl else from_kafka())


if __name__ == "__main__":
    main()
