"""R2(S3 호환) 접근: 올리기, 목록, 오래된 것 지우기. .env의 S3_ENDPOINT·S3_BUCKET·S3_ACCESS_KEY·S3_SECRET_KEY.

    python -m market.r2 put market/pgdump/2026-11-27.dump /tmp/x.dump
    python -m market.r2 ls market/pgdump/
    python -m market.r2 prune market/pgdump/ --keep 3
"""
from __future__ import annotations

import argparse
import os

from . import config as C

PREFIX = os.environ.get("MARKET_R2_PREFIX", "market")


def _cfg() -> dict:
    vals = {**C.env_file_values(), **os.environ}
    need = ("S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY")
    miss = [k for k in need if not vals.get(k)]
    if miss:
        raise SystemExit(f".env에 R2 설정이 없다: {', '.join(miss)}")
    return {k: vals[k] for k in need}


def client():
    import boto3
    from botocore.config import Config
    c = _cfg()
    s3 = boto3.client("s3", endpoint_url=c["S3_ENDPOINT"], aws_access_key_id=c["S3_ACCESS_KEY"],
                      aws_secret_access_key=c["S3_SECRET_KEY"], region_name="auto",
                      config=Config(retries={"max_attempts": 5, "mode": "standard"}))
    return s3, c["S3_BUCKET"]


def put_file(key: str, path: str) -> None:
    s3, bucket = client()
    s3.upload_file(path, bucket, key)


def put_bytes(key: str, data: bytes) -> None:
    s3, bucket = client()
    s3.put_object(Bucket=bucket, Key=key, Body=data)


def ls(prefix: str) -> list[dict]:
    s3, bucket = client()
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        out += page.get("Contents", [])
    return out


def prune(prefix: str, keep: int) -> list[str]:
    """prefix 아래 객체를 이름순으로 정렬해 마지막 keep개만 남긴다 (이름에 날짜가 들어간 덤프용)."""
    s3, bucket = client()
    objs = sorted(o["Key"] for o in ls(prefix))
    gone = objs[:-keep] if keep > 0 else objs
    for k in gone:
        s3.delete_object(Bucket=bucket, Key=k)
    return gone


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("put"); p.add_argument("key"); p.add_argument("path")
    p = sub.add_parser("ls"); p.add_argument("prefix")
    p = sub.add_parser("prune"); p.add_argument("prefix"); p.add_argument("--keep", type=int, required=True)
    a = ap.parse_args()
    if a.cmd == "put":
        put_file(a.key, a.path)
        print(f"올림 {a.key} ({os.path.getsize(a.path) / 2**20:.1f} MB)")
    elif a.cmd == "ls":
        objs = ls(a.prefix)
        for o in objs:
            print(f"{o['Key']}  {o['Size'] / 2**20:.2f} MB  {o['LastModified']:%Y-%m-%d %H:%M}")
        print(f"{len(objs)}개, {sum(o['Size'] for o in objs) / 2**20:.1f} MB")
    else:
        for k in prune(a.prefix, a.keep):
            print("지움", k)


if __name__ == "__main__":
    main()
