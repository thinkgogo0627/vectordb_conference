"""실험 파이프라인 공통 설정. 환경변수(.env)로 바꾼다."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    p = ROOT / ".env"
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()

PG_DSN = os.environ.get("EXP_PG_DSN", "host=localhost port=5432 user=exp password=exp dbname=exp")
KAFKA_BOOTSTRAP = os.environ.get("EXP_KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("EXP_KAFKA_TOPIC", "exp0.public.wiki_revisions")
CONNECT_URL = os.environ.get("EXP_CONNECT_URL", "http://localhost:8083")

DATA_DIR = Path(os.environ.get("EXP_DATA_DIR", ROOT / "data" / "exp0"))
SUBSET_PATH = DATA_DIR / "subset.parquet"
EMBED_PATH = DATA_DIR / "embeddings.parquet"
RUNS_DIR = DATA_DIR / "runs"

EMBED_MODEL = os.environ.get("EXP_EMBED_MODEL", "BAAI/bge-m3")
CHUNK_CHARS = int(os.environ.get("EXP_CHUNK_CHARS", "800"))

WORKER_PORT = int(os.environ.get("EXP_WORKER_PORT", "8765"))

# R2 (T-01 부분집합 추출용). 수집기 .env와 같은 변수명.
S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
S3_BUCKET = os.environ.get("S3_BUCKET", "wiki-lake")
S3_PREFIX = os.environ.get("S3_PREFIX", "raw/wiki_recentchange")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")


def chunk_id(page_id: int, chunk_idx: int) -> int:
    """FAISS int64 id. page_id는 2^47 미만, chunk_idx는 2^16 미만이라고 가정."""
    assert 0 <= chunk_idx < (1 << 16)
    return (int(page_id) << 16) | int(chunk_idx)


def split_chunk_id(cid: int) -> tuple[int, int]:
    return int(cid) >> 16, int(cid) & 0xFFFF
