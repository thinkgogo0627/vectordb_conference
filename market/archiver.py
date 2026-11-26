"""원천 메시지 보관기: Kafka raw.* → 10분마다 Parquet → R2 `market/raw/<토픽>/date=YYYY-MM-DD/p<파티션>-<첫 오프셋>-<끝 오프셋>.parquet`.

    python -m market.archiver
    python -m market.archiver --flush-s 60 --seconds 300      # 확인용

- Kafka는 48시간만 보관한다. R2가 원천 메시지의 장기 보관소다 (버그를 고친 뒤 처음부터 다시 만들 때 쓴다).
- 올리기에 성공한 뒤에만 오프셋을 커밋한다. 그래서 서버가 죽어도 빠지는 메시지는 없다.
  대신 같은 메시지가 두 파일에 들어갈 수 있으니, 읽을 때 (토픽, 파티션, offset)으로 중복을 걸러 낸다.
- `^raw\\.` 패턴으로 구독하므로, 나중에 생긴 토픽도 자동으로 따라간다.
- 열: offset, kafka_ts(ms), key, value(원천 JSON 문자열 그대로)
"""
from __future__ import annotations

import argparse
import io
import time
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from . import config as C
from .r2 import PREFIX

FLUSH_S = 600
MAX_ROWS = 500_000


class Archiver:
    def __init__(self, prefix: str = PREFIX):
        self.prefix = prefix
        self.buf: dict[tuple[str, int], list[tuple]] = defaultdict(list)
        self.rows = 0

    def add(self, topic: str, partition: int, offset: int, ts: int, key: bytes | None, value: bytes | None) -> None:
        self.buf[(topic, partition)].append((offset, ts, key.decode() if key else None,
                                             value.decode() if value else None))
        self.rows += 1

    @staticmethod
    def object_key(prefix: str, topic: str, partition: int, rows: list[tuple]) -> str:
        day = datetime.fromtimestamp(rows[0][1] / 1000, timezone.utc).strftime("%Y-%m-%d")
        return f"{prefix}/raw/{topic}/date={day}/p{partition}-{rows[0][0]:012d}-{rows[-1][0]:012d}.parquet"

    @staticmethod
    def to_parquet(rows: list[tuple]) -> bytes:
        t = pa.table({"offset": pa.array([r[0] for r in rows], pa.int64()),
                      "kafka_ts": pa.array([r[1] for r in rows], pa.int64()),
                      "key": pa.array([r[2] for r in rows], pa.string()),
                      "value": pa.array([r[3] for r in rows], pa.string())})
        out = io.BytesIO()
        pq.write_table(t, out, compression="zstd")
        return out.getvalue()

    def flush(self, put) -> tuple[list[tuple[str, int, int]], dict]:
        """버퍼를 (토픽, 파티션)마다 파일 하나로 올린다. 반환: 커밋할 (토픽, 파티션, 다음 오프셋), 통계.
        하나라도 실패하면 그 (토픽, 파티션)은 버퍼에 남겨 다음에 다시 올리고 커밋하지 않는다."""
        commits, stat = [], {"files": 0, "rows": 0, "bytes": 0, "failed": 0}
        for (topic, part), rows in list(self.buf.items()):
            if not rows:
                continue
            rows.sort(key=lambda r: r[0])
            data = self.to_parquet(rows)
            try:
                put(self.object_key(self.prefix, topic, part, rows), data)
            except Exception as e:                    # 네트워크·R2 오류 → 다음 주기에 다시
                stat["failed"] += 1
                print(f"[archiver] 올리기 실패 {topic}[{part}] {type(e).__name__}: {e}", flush=True)
                continue
            commits.append((topic, part, rows[-1][0] + 1))
            stat["files"] += 1
            stat["rows"] += len(rows)
            stat["bytes"] += len(data)
            self.rows -= len(rows)
            del self.buf[(topic, part)]
        return commits, stat


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--flush-s", type=float, default=FLUSH_S)
    ap.add_argument("--seconds", type=float, default=None)
    a = ap.parse_args()
    from confluent_kafka import Consumer, TopicPartition
    from .r2 import put_bytes
    c = Consumer({"bootstrap.servers": C.KAFKA, "group.id": "market-archiver", "enable.auto.commit": False,
                  "auto.offset.reset": "earliest", "topic.metadata.refresh.interval.ms": 30000})
    c.subscribe(["^raw\\..*"])
    arc = Archiver()
    t_end = time.time() + a.seconds if a.seconds else None
    t_flush = time.time() + a.flush_s
    print(f"[archiver] raw.* → R2 {PREFIX}/raw/, {a.flush_s:.0f}초마다", flush=True)
    try:
        while t_end is None or time.time() < t_end:
            for m in c.consume(5000, timeout=1.0):
                if m.error():
                    continue
                arc.add(m.topic(), m.partition(), m.offset(), m.timestamp()[1], m.key(), m.value())
            if time.time() >= t_flush or arc.rows >= MAX_ROWS:
                t_flush = time.time() + a.flush_s
                t0 = time.time()
                commits, st = arc.flush(put_bytes)
                if commits:
                    c.commit(offsets=[TopicPartition(t, p, o) for t, p, o in commits], asynchronous=False)
                print(f"[archiver] 파일 {st['files']}, 행 {st['rows']}, {st['bytes'] / 2**20:.2f} MB, "
                      f"실패 {st['failed']}, 남은 행 {arc.rows}, {time.time() - t0:.1f}s", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        commits, st = arc.flush(put_bytes)            # 끝날 때 남은 것도 올린다
        if commits:
            c.commit(offsets=[TopicPartition(t, p, o) for t, p, o in commits], asynchronous=False)
        print(f"[archiver] 끝: 마지막 파일 {st['files']}, 행 {st['rows']}", flush=True)
        c.close()


if __name__ == "__main__":
    main()
