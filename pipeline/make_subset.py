"""T-01: 실험 0용 부분집합을 만든다 → data/exp0/subset.parquet

출력 컬럼: page_id, rev_id, parent_rev_id, page_title, event_dt(UTC ISO), content

세 가지 입력원:
  --source r2        .env의 S3_* 로 R2(wiki-lake)에서 읽는다 (duckdb httpfs)
  --source local DIR 수집기가 남긴 parquet 디렉터리/글롭
  --source synthetic 같은 스키마의 합성 편집 흐름 (R2 접근 전 파이프라인 검증용)

실제 데이터: event_id 중복 제거 → op in (create, update), content_status='ok' →
리비전이 2개 이상인 문서만 → 문서 수 --pages 개로 제한 (리비전 많은 순) → event_dt 순 정렬.
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import config as C

OUT_SCHEMA = pa.schema([
    ("page_id", pa.int64()),
    ("rev_id", pa.int64()),
    ("parent_rev_id", pa.int64()),
    ("page_title", pa.string()),
    ("event_dt", pa.string()),
    ("content", pa.string()),
    ("op", pa.string()),
    ("is_bot", pa.bool_()),
])


def _duckdb_select(src_sql: str, pages: int, min_revs: int = 2, until: str | None = None) -> pa.Table:
    import duckdb

    con = duckdb.connect()
    if src_sql.startswith("'s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        host = C.S3_ENDPOINT.replace("https://", "").replace("http://", "")
        con.execute(f"""
            CREATE SECRET r2 (TYPE S3, KEY_ID '{C.S3_ACCESS_KEY}', SECRET '{C.S3_SECRET_KEY}',
                              ENDPOINT '{host}', REGION 'auto', URL_STYLE 'path');
        """)
    until_sql = f"AND event_dt < '{until}T99'" if until else ""   # until 날짜(UTC)까지 포함
    limit_sql = f"LIMIT {pages}" if pages else ""
    q = f"""
    WITH dedup AS (
        SELECT * FROM read_parquet({src_sql}, union_by_name=true)
        QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingested_at) = 1
    ), edits AS (
        SELECT page_id, rev_id, parent_rev_id, page_title, event_dt, content, op, is_bot
        FROM dedup
        WHERE op IN ('create','update') AND content_status = 'ok'
          AND page_id IS NOT NULL AND rev_id IS NOT NULL AND content IS NOT NULL
          {until_sql}
    ), pages AS (
        SELECT page_id, count(*) AS n FROM edits GROUP BY page_id HAVING count(*) >= {min_revs}
        ORDER BY n DESC, page_id {limit_sql}
    )
    SELECT e.* FROM edits e JOIN pages p USING (page_id) ORDER BY e.event_dt, e.rev_id
    """
    return con.execute(q).to_arrow_table().cast(OUT_SCHEMA)


def from_r2(pages: int, date_glob: str, min_revs: int = 2, until: str | None = None) -> pa.Table:
    if not (C.S3_ENDPOINT and C.S3_ACCESS_KEY and C.S3_SECRET_KEY):
        raise SystemExit("S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY 가 .env에 없다 (T-01)")
    src = f"'s3://{C.S3_BUCKET}/{C.S3_PREFIX}/source=kowiki/date={date_glob}/*/*.parquet'"
    return _duckdb_select(src, pages, min_revs, until)


def from_local(path: str, pages: int, min_revs: int = 2, until: str | None = None) -> pa.Table:
    return _duckdb_select(f"'{path}'", pages, min_revs, until)


# ---------------------------------------------------------------- synthetic
_WORDS = ("서울 부산 대구 인천 광주 대전 울산 세종 경기 강원 충북 충남 전북 전남 경북 경남 제주 "
          "역사 문화 경제 인구 교통 행정 지리 기후 교육 산업 관광 축제 음식 건축 인물 사건 조약 "
          "wiki edit revision vector index gap replay stream kafka postgres faiss chunk embed").split()


def _paragraph(rng: random.Random, n_words: int) -> str:
    return " ".join(rng.choice(_WORDS) for _ in range(n_words)) + "."


def synthetic(pages: int, revs_per_page: int, window_s: float, seed: int) -> pa.Table:
    """문서마다 문단 3~6개. 리비전마다 문단 1개를 고쳐 쓴다 → 바뀐 청크가 1개 이상 생긴다.
    편집 시각은 window_s 안에 균일 랜덤. 리비전 번호는 전역 시각 순으로 증가 (실제 위키와 같다)."""
    rng = random.Random(seed)
    base = datetime(2026, 11, 10, 0, 0, 0, tzinfo=timezone.utc)
    events: list[tuple[float, int, int, str, str]] = []  # (t, page_id, k, title, content)
    for p in range(pages):
        page_id = 100_000 + p
        title = f"합성문서_{p:04d}"
        paras = [_paragraph(rng, rng.randint(60, 120)) for _ in range(rng.randint(3, 6))]
        times = sorted(rng.uniform(0, window_s) for _ in range(revs_per_page))
        for k, t in enumerate(times):
            if k > 0:
                i = rng.randrange(len(paras))
                paras[i] = _paragraph(rng, rng.randint(60, 120))
            events.append((t, page_id, k, title, "\n\n".join(paras)))
    events.sort()
    rows = []
    last_rev: dict[int, int] = {}
    for i, (t, page_id, _k, title, content) in enumerate(events):
        rev_id = 10_000_000 + i
        rows.append({
            "page_id": page_id,
            "rev_id": rev_id,
            "parent_rev_id": last_rev.get(page_id),
            "page_title": title,
            "event_dt": (base + timedelta(seconds=t)).isoformat().replace("+00:00", "Z"),
            "content": content,
            "op": "update" if page_id in last_rev else "create",
            "is_bot": False,
        })
        last_rev[page_id] = rev_id
    return pa.Table.from_pylist(rows, schema=OUT_SCHEMA)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["r2", "local", "synthetic"], default="r2")
    ap.add_argument("--path", help="--source local 일 때 parquet 글롭")
    ap.add_argument("--date", default="2026-11-*", help="--source r2 일 때 date= 파티션 글롭")
    ap.add_argument("--pages", type=int, default=200, help="리비전 많은 순 상위 N 문서. 0이면 전부")
    ap.add_argument("--min-revs", type=int, default=2, help="문서당 최소 리비전 수")
    ap.add_argument("--until", help="이 날짜(UTC, YYYY-MM-DD)까지 포함")
    ap.add_argument("--revs", type=int, default=3, help="synthetic: 문서당 리비전 수")
    ap.add_argument("--window", type=float, default=300.0, help="synthetic: 편집이 퍼지는 시간(초)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out", default=str(C.SUBSET_PATH))
    a = ap.parse_args()

    if a.source == "r2":
        t = from_r2(a.pages, a.date, a.min_revs, a.until)
    elif a.source == "local":
        t = from_local(a.path, a.pages, a.min_revs, a.until)
    else:
        t = synthetic(a.pages, a.revs, a.window, a.seed)

    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(t, a.out, compression="zstd")
    n_pages = len(set(t.column("page_id").to_pylist()))
    dts = t.column("event_dt").to_pylist()
    print(f"subset: rows={t.num_rows} pages={n_pages} span={dts[0]} .. {dts[-1]} -> {a.out}")


if __name__ == "__main__":
    main()
