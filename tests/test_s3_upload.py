"""boto3 업로드 경로 검증 (moto로 가짜 S3)."""
import boto3
from moto import mock_aws

from collector.config import Config
from collector.sink import Uploader, write_parts
from tests.test_pipeline import rc
from collector.transform import to_row


@mock_aws
def test_upload_to_s3(tmp_path):
    cfg = Config()
    cfg.data_dir = str(tmp_path)
    cfg.storage_backend = "s3"
    cfg.s3_bucket = "wiki-lake"
    cfg.s3_region = "us-east-1"
    cfg.s3_access_key = cfg.s3_secret_key = "test"
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="wiki-lake")

    up = Uploader(cfg)
    rel = write_parts([to_row("id1", rc(1, "edit", old=1, new=2))], up.outbox, cfg.s3_prefix)
    ok, fail = up.upload_all()
    assert (ok, fail) == (1, 0) and up.pending() == []
    keys = [o["Key"] for o in boto3.client("s3", region_name="us-east-1").list_objects_v2(Bucket="wiki-lake")["Contents"]]
    assert keys == rel
