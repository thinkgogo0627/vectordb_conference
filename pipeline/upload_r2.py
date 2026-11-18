"""로컬 디렉터리 ↔ R2 (S3 호환). .env의 S3_* 사용.

    python -m pipeline.upload_r2 up   data/lake/emb  embeddings/bge-m3/until=2026-11-17
    python -m pipeline.upload_r2 down embeddings/bge-m3/until=2026-11-17  data/lake/emb
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig

from . import config as C


def client():
    if not (C.S3_ENDPOINT and C.S3_ACCESS_KEY and C.S3_SECRET_KEY):
        raise SystemExit("S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY 가 .env에 없다")
    return boto3.client("s3", endpoint_url=C.S3_ENDPOINT, aws_access_key_id=C.S3_ACCESS_KEY,
                        aws_secret_access_key=C.S3_SECRET_KEY, region_name="auto",
                        config=BotoConfig(retries={"max_attempts": 10, "mode": "standard"}))


def up(local: Path, prefix: str) -> None:
    s3 = client()
    files = sorted(p for p in local.rglob("*") if p.is_file() and p.name != "progress.json")
    total = sum(p.stat().st_size for p in files)
    sent = 0
    for p in files:
        key = f"{prefix.rstrip('/')}/{p.relative_to(local).as_posix()}"
        s3.upload_file(str(p), C.S3_BUCKET, key)
        sent += p.stat().st_size
        print(f"  {key}  ({sent / 2**20:.0f}/{total / 2**20:.0f} MB)", flush=True)
    print(f"uploaded {len(files)} files, {total / 2**20:.1f} MB -> s3://{C.S3_BUCKET}/{prefix}")


def down(prefix: str, local: Path) -> None:
    s3 = client()
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=C.S3_BUCKET, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            dst = local / obj["Key"][len(prefix.rstrip("/")) + 1:]
            dst.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(C.S3_BUCKET, obj["Key"], str(dst))
            n += 1
    print(f"downloaded {n} files -> {local}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["up", "down"])
    ap.add_argument("src")
    ap.add_argument("dst")
    a = ap.parse_args()
    if a.mode == "up":
        up(Path(a.src), a.dst)
    else:
        down(a.src, Path(a.dst))


if __name__ == "__main__":
    sys.exit(main())
