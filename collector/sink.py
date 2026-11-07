"""버퍼 → Parquet 파일 → 레이크(R2/S3 또는 로컬) 업로드.

파일 경로 (Hive 파티션):
  {prefix}/source={wiki}/date=YYYY-MM-DD/hour=HH/part-{ts}-{uuid}.parquet

- 날짜/시간 파티션은 '이벤트 시각(event_dt)' 기준 → 리플레이할 때 시간 범위로 바로 고를 수 있음
- 로컬 outbox에 먼저 쓰고, 업로드 성공 후에만 지운다 → 네트워크가 끊겨도 유실 없음
"""
from __future__ import annotations

import logging
import os
import shutil
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("stream_offset_id", pa.string()),
    ("event_dt", pa.string()),
    ("rc_timestamp", pa.int64()),
    ("wiki", pa.string()),
    ("domain", pa.string()),
    ("op", pa.string()),
    ("rc_type", pa.string()),
    ("log_type", pa.string()),
    ("log_action", pa.string()),
    ("namespace", pa.int32()),
    ("page_title", pa.string()),
    ("page_id", pa.int64()),
    ("rev_id", pa.int64()),
    ("parent_rev_id", pa.int64()),
    ("rev_timestamp", pa.string()),
    ("sha1", pa.string()),
    ("rev_size", pa.int64()),
    ("length_old", pa.int64()),
    ("length_new", pa.int64()),
    ("is_bot", pa.bool_()),
    ("is_minor", pa.bool_()),
    ("comment", pa.string()),
    ("tags", pa.list_(pa.string())),
    ("content", pa.large_string()),
    ("content_status", pa.string()),
    ("raw_event", pa.large_string()),
    ("ingested_at", pa.string()),
])


def partition_of(row: dict) -> tuple[str, str, str]:
    dt = row.get("event_dt") or row.get("ingested_at")
    try:
        d = datetime.fromisoformat(dt.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        d = datetime.now(timezone.utc)
    return row.get("wiki") or "unknown", d.strftime("%Y-%m-%d"), d.strftime("%H")


def write_parts(rows: list[dict], outbox_dir: str, prefix: str) -> list[str]:
    """파티션별로 나눠 outbox에 Parquet 파일을 쓰고, 상대 경로 목록을 돌려준다."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[partition_of(r)].append(r)
    written = []
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for (wiki, date, hour), grp in groups.items():
        rel = f"{prefix}/source={wiki}/date={date}/hour={hour}/part-{ts}-{uuid.uuid4().hex[:8]}.parquet"
        path = os.path.join(outbox_dir, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        table = pa.Table.from_pylist(grp, schema=SCHEMA)
        tmp = path + ".tmp"
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, path)
        written.append(rel)
    return written


class Uploader:
    def __init__(self, cfg):
        self.cfg = cfg
        self.outbox = os.path.join(cfg.data_dir, "outbox")
        os.makedirs(self.outbox, exist_ok=True)
        self.client = None
        if cfg.storage_backend == "s3":
            import boto3
            from botocore.config import Config as BotoConfig
            self.client = boto3.client(
                "s3", endpoint_url=cfg.s3_endpoint, region_name=cfg.s3_region,
                aws_access_key_id=cfg.s3_access_key, aws_secret_access_key=cfg.s3_secret_key,
                config=BotoConfig(retries={"max_attempts": 5, "mode": "standard"}),
            )

    def pending(self) -> list[str]:
        out = []
        for root, _, files in os.walk(self.outbox):
            for f in files:
                if f.endswith(".parquet"):
                    out.append(os.path.relpath(os.path.join(root, f), self.outbox))
        return sorted(out)

    def upload_all(self) -> tuple[int, int]:
        ok = fail = 0
        for rel in self.pending():
            src = os.path.join(self.outbox, rel)
            try:
                if self.client is not None:
                    self.client.upload_file(src, self.cfg.s3_bucket, rel)
                else:
                    dst = os.path.join(self.cfg.local_lake_dir, rel)
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.copy2(src, dst + ".tmp")
                    os.replace(dst + ".tmp", dst)
                os.remove(src)
                ok += 1
            except Exception as e:  # 다음 주기에 재시도
                log.warning("upload failed %s: %s", rel, e)
                fail += 1
        return ok, fail

    def run_forever(self, stop, interval: int = 30):
        while not stop():
            ok, fail = self.upload_all()
            if ok or fail:
                log.info("upload: ok=%d fail=%d pending=%d", ok, fail, len(self.pending()))
            for _ in range(interval):
                if stop():
                    break
                time.sleep(1)
        self.upload_all()  # 종료 직전 마지막 업로드
