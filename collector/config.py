"""환경변수 기반 설정. 모든 값은 .env에서 바꾼다."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    return (_env(name, "true" if default else "false") or "").lower() in ("1", "true", "yes")


@dataclass
class Config:
    # 수집 대상
    stream_url: str = "https://stream.wikimedia.org/v2/stream/recentchange"
    wikis: set[str] = field(default_factory=lambda: {"kowiki"})   # 예: kowiki,enwiki
    namespaces: set[int] = field(default_factory=lambda: {0})      # 0 = 일반 문서
    include_bots: bool = True

    # 본문·메타 보강 (Action API)
    fetch_content: bool = True
    enrich_batch_size: int = 50          # Action API revids 최대 50
    enrich_max_wait_s: int = 10          # 배치가 덜 차도 이 시간이 지나면 조회

    # 버퍼 → Parquet
    flush_seconds: int = 300
    flush_rows: int = 5000
    data_dir: str = "/data"

    # 저장소
    storage_backend: str = "s3"          # s3 | local
    s3_endpoint: str | None = None       # R2: https://<accountid>.r2.cloudflarestorage.com
    s3_bucket: str = "wiki-lake"
    s3_prefix: str = "raw/wiki_recentchange"
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "auto"
    local_lake_dir: str = "/data/lake"   # storage_backend=local 일 때

    # Wikimedia 정책: User-Agent에 연락처 필수
    user_agent: str = "ybigta-vectordb-sync-collector/0.1"
    contact: str | None = None

    log_level: str = "INFO"

    @property
    def http_user_agent(self) -> str:
        return f"{self.user_agent} ({self.contact})" if self.contact else self.user_agent

    @classmethod
    def from_env(cls) -> "Config":
        c = cls()
        c.stream_url = _env("STREAM_URL", c.stream_url)
        c.wikis = {w.strip() for w in _env("WIKIS", "kowiki").split(",") if w.strip()}
        c.namespaces = {int(n) for n in _env("NAMESPACES", "0").split(",") if n.strip()}
        c.include_bots = _env_bool("INCLUDE_BOTS", c.include_bots)
        c.fetch_content = _env_bool("FETCH_CONTENT", c.fetch_content)
        c.enrich_batch_size = min(50, _env_int("ENRICH_BATCH_SIZE", c.enrich_batch_size))
        c.enrich_max_wait_s = _env_int("ENRICH_MAX_WAIT_S", c.enrich_max_wait_s)
        c.flush_seconds = _env_int("FLUSH_SECONDS", c.flush_seconds)
        c.flush_rows = _env_int("FLUSH_ROWS", c.flush_rows)
        c.data_dir = _env("DATA_DIR", c.data_dir)
        c.storage_backend = _env("STORAGE_BACKEND", c.storage_backend)
        c.s3_endpoint = _env("S3_ENDPOINT")
        c.s3_bucket = _env("S3_BUCKET", c.s3_bucket)
        c.s3_prefix = _env("S3_PREFIX", c.s3_prefix).strip("/")
        c.s3_access_key = _env("S3_ACCESS_KEY")
        c.s3_secret_key = _env("S3_SECRET_KEY")
        c.s3_region = _env("S3_REGION", c.s3_region)
        c.local_lake_dir = _env("LOCAL_LAKE_DIR", os.path.join(c.data_dir, "lake"))
        c.user_agent = _env("USER_AGENT", c.user_agent)
        c.contact = _env("CONTACT")
        c.log_level = _env("LOG_LEVEL", c.log_level)
        return c
