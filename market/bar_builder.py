"""② 1분봉 정규화: Kafka raw.* → Postgres bars_1m · symbol_status.

    python -m market.bar_builder                 # 멈출 때까지
    python -m market.bar_builder --seconds 180

- 업비트 체결 → 1분봉 (market.bars.UpbitMinuteBars). Alpaca 1분봉·정정·상태는 형식만 바꿔 그대로 쓴다.
- Kafka 오프셋은 커밋하지 않는다. 시작할 때 소스마다 DB에 있는 마지막 봉 − 3분 지점(Kafka 타임스탬프 기준)으로
  되감아 다시 읽는다. 쓰기는 값이 바뀔 때만 반영되므로(store.py) 다시 읽어도 결과가 같다.
  DB가 비어 있으면 토픽 처음부터 읽는다.
- 워터마크는 메시지를 처리하는 동안에는 메시지의 수신 시각으로, 메시지가 없으면 현재 호스트 시각으로 민다.
  그래서 밀린 메시지를 빠르게 따라잡는 동안 봉을 너무 일찍 확정하지 않는다.
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter

import numpy as np
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from . import config as C
from .bars import UpbitMinuteBars, alpaca_bar, alpaca_status
from .store import Store

LOOKBACK_MS = 3 * 60_000
IDLE_FREEZE_S = 10.0
SOURCES = {"upbit": [C.T_UPBIT_TRADE],
           "alpaca": [C.T_ALPACA["b"], C.T_ALPACA["u"], C.T_ALPACA["s"]]}


def start_offsets(c: Consumer, store: Store) -> list[TopicPartition]:
    with store.conn.cursor() as cur:
        cur.execute("SELECT source, (extract(epoch FROM max(minute_ts)) * 1000)::bigint FROM bars_1m GROUP BY source")
        last = dict(cur.fetchall())
    store.conn.commit()
    existing = c.list_topics(timeout=10).topics
    tps = []
    for src, topics in SOURCES.items():
        for t in topics:
            if t not in existing:
                continue
            for p in existing[t].partitions:
                if src in last:
                    tps.append(TopicPartition(t, p, int(last[src] - LOOKBACK_MS)))
                else:
                    tps.append(TopicPartition(t, p, OFFSET_BEGINNING))
    timed = [tp for tp in tps if tp.offset >= 0]
    resolved = c.offsets_for_times(timed, timeout=10) if timed else []
    out = [tp for tp in tps if tp.offset < 0] + resolved
    for tp in out:
        print(f"[bars] {tp.topic}[{tp.partition}] 시작 오프셋 {tp.offset}"
              + (f" (DB 마지막 봉 − 3분)" if tp.offset >= 0 else " (처음부터 또는 끝)"), flush=True)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=None)
    ap.add_argument("--grace-ms", type=int, default=5_000, help="업비트 봉 확정 전 늦은 체결을 기다리는 시간")
    a = ap.parse_args()

    store = Store()
    c = Consumer({"bootstrap.servers": C.KAFKA, "group.id": "market-bar-builder", "enable.auto.commit": False,
                  "auto.offset.reset": "earliest"})
    c.assign(start_offsets(c, store))
    agg = UpbitMinuteBars(grace_ms=a.grace_ms)
    n = Counter()
    lat: list[int] = []
    t_end = time.time() + a.seconds if a.seconds else None
    t_rep = time.time() + 10
    newest_recv = 0
    try:
        while t_end is None or time.time() < t_end:
            msgs = c.consume(2000, timeout=0.5)
            bars, statuses, last_recv = [], [], None
            for m in msgs:
                if m.error():
                    continue
                v = json.loads(m.value())
                d, recv = v["data"], v["recv_ts"]
                last_recv = recv
                t = m.topic()
                if t == C.T_UPBIT_TRADE:
                    if d.get("type") == C.SESSION_START:
                        bars += agg.advance(recv)       # 끊기기 전에 이미 확정할 수 있던 봉은 먼저 내보낸다
                        agg.session_start()
                    elif d.get("type") == "trade":
                        bars += agg.add(d, recv)
                elif t in (C.T_ALPACA["b"], C.T_ALPACA["u"]):
                    bars.append(alpaca_bar(d, recv))
                    n["alpaca_bar_update" if t == C.T_ALPACA["u"] else "alpaca_bar"] += 1
                elif t == C.T_ALPACA["s"]:
                    statuses.append(alpaca_status(d, recv))
            if last_recv is not None:
                newest_recv = max(newest_recv, last_recv)
                bars += agg.advance(last_recv)
            elif int(time.time() * 1000) - newest_recv < IDLE_FREEZE_S * 1000:
                bars += agg.advance(int(time.time() * 1000))
            # 가장 최근에 받은 메시지가 IDLE_FREEZE_S보다 오래됐으면 (수집기가 멈췄거나, 되감아 읽은 옛 데이터의 끝)
            # 워터마크를 멈춘다. 그래야 수집기가 멈출 때 열려 있던 분을 일부만 본 채로 확정하지 않는다.
            # 수집기가 다시 연결되면 session_start 표지가 그 분을 버린다.
            if bars:
                r = store.write_bars(bars)
                n.update({f"rows_{k}": val for k, val in r.items()})
                built = int(time.time() * 1000)
                lat += [built - b.recv_ts for b in bars]
                n.update(f"bars_{b.source}" for b in bars)
            if statuses:
                store.write_statuses(statuses)
                n["statuses"] += len(statuses)
            if time.time() >= t_rep:
                t_rep = time.time() + 10
                l50 = f"{np.percentile(lat, 50) / 1000:.2f}s" if lat else "-"
                print(f"[bars] {dict(n)} upbit {agg.stats} 마지막 수신→DB p50 {l50}", flush=True)
                lat = lat[-5000:]
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[bars] 끝 {dict(n)} upbit {agg.stats}", flush=True)
        c.close()
        store.close()


if __name__ == "__main__":
    main()
