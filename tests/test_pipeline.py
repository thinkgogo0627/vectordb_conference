"""네트워크 없이 수집 흐름 전체를 검증: SSE 파싱 → 필터 → 보강 → Parquet → 로컬 레이크 → 재개 위치."""
import glob
import json
import os

import pyarrow.parquet as pq

from collector.config import Config
from collector.main import Collector
from collector.sink import Uploader
from collector.stream import parse_sse


def rc(i, typ="edit", wiki="kowiki", ns=0, old=None, new=None, **kw):
    e = {
        "meta": {"id": f"uuid-{i}", "dt": f"2026-11-07T03:0{i % 10}:00Z", "domain": "ko.wikipedia.org"},
        "type": typ, "wiki": wiki, "namespace": ns, "title": f"문서{i}",
        "timestamp": 1794020000 + i, "bot": False, "minor": False, "comment": "c",
        "revision": {"old": old, "new": new}, "length": {"old": 100, "new": 120},
    }
    e.update(kw)
    return e


def sse_text(events):
    lines = [": heartbeat"]
    for i, e in enumerate(events):
        lines += [f'id: [{{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":{i}}}]',
                  "event: message", f"data: {json.dumps(e, ensure_ascii=False)}", ""]
    return lines


class FakeEnricher:
    def __init__(self):
        self.calls = []

    def fetch(self, domain, revids):
        self.calls.append(revids)
        out = {}
        for r in revids:
            if r == 999:  # 삭제/숨김 리비전
                out[r] = {"content_status": "missing"}
                continue
            out[r] = {"page_id": r // 10, "rev_timestamp": "2026-11-07T03:00:00Z",
                      "sha1": f"sha{r}", "rev_size": 120,
                      "tags": ["mw-rollback"] if r == 103 else [],
                      "content": f"본문 {r}", "content_status": "ok"}
        return out


def test_parse_sse_handles_comments_and_multiline():
    lines = [": hb", "id: 1", "data: {\"a\":", "data: 1}", "", "id: 2", "data: {}", ""]
    assert list(parse_sse(iter(lines))) == [("1", '{"a":\n1}'), ("2", "{}")]


def test_end_to_end_local(tmp_path):
    cfg = Config()
    cfg.data_dir = str(tmp_path)
    cfg.local_lake_dir = str(tmp_path / "lake")
    cfg.storage_backend = "local"
    cfg.flush_rows = 10_000
    cfg.enrich_batch_size = 2  # 배치 분할도 검증

    events = [
        rc(1, "new", new=101),
        rc(2, "edit", old=101, new=102),
        rc(3, "edit", old=102, new=103),                  # 되돌림 태그
        rc(4, "edit", wiki="enwiki", new=500),            # 다른 위키 → 버림
        rc(5, "edit", ns=1, new=600),                     # 토론 문서 → 버림
        rc(6, "categorize"),                              # 버림
        rc(7, "log", log_type="delete", log_action="delete"),
        rc(8, "edit", old=103, new=999),                  # 본문 없음
    ]
    fake = FakeEnricher()
    col = Collector(cfg, enricher=fake, uploader=Uploader(cfg))

    last_id = None
    for eid, data in parse_sse(iter(sse_text(events))):
        col.handle(eid, json.loads(data))
        last_id = eid
    col.maybe_flush(force=True)
    ok, fail = col.uploader.upload_all()
    assert fail == 0 and ok >= 1
    assert col.uploader.pending() == []                 # outbox 비워짐

    files = glob.glob(str(tmp_path / "lake/raw/wiki_recentchange/source=kowiki/date=2026-11-07/hour=03/*.parquet"))
    assert files, "Hive 파티션 경로에 파일이 있어야 함"
    rows = pq.read_table(files).to_pylist()
    by_rev = {r["rev_id"]: r for r in rows}

    assert len(rows) == 5                                # 101,102,103,999 + delete
    assert {r["op"] for r in rows} == {"create", "update", "delete"}
    assert by_rev[101]["content"] == "본문 101" and by_rev[101]["sha1"] == "sha101"
    assert by_rev[102]["parent_rev_id"] == 101
    assert by_rev[103]["tags"] == ["mw-rollback"]
    assert by_rev[999]["content_status"] == "missing"
    delete_row = next(r for r in rows if r["op"] == "delete")
    assert delete_row["content_status"] == "none"
    assert all(len(c) <= 2 for c in fake.calls)          # 50개 제한(테스트에선 2) 지킴

    # 재개 위치는 마지막으로 본 이벤트 ID
    with open(tmp_path / "state/last_event_id") as f:
        assert f.read() == last_id
    assert os.path.exists(tmp_path / "state/heartbeat")


def test_failed_enrichment_marked_for_backfill(tmp_path):
    cfg = Config()
    cfg.data_dir = str(tmp_path)
    cfg.local_lake_dir = str(tmp_path / "lake")
    cfg.storage_backend = "local"

    class Down:
        def fetch(self, domain, revids):
            return {}

    col = Collector(cfg, enricher=Down(), uploader=Uploader(cfg))
    col.handle("x", rc(1, "edit", old=1, new=2))
    col.maybe_flush(force=True)
    col.uploader.upload_all()
    rows = pq.read_table(glob.glob(str(tmp_path / "lake/**/*.parquet"), recursive=True)).to_pylist()
    assert rows[0]["content_status"] == "failed"


def test_enricher_dedupes_revids():
    from collector.enrich import Enricher

    class Resp:
        status_code = 200
        headers = {}
        def raise_for_status(self): pass
        def json(self): return {"query": {"pages": []}}

    class Sess:
        headers = {}
        def __init__(self): self.params = None
        def get(self, url, params, timeout):
            self.params = params
            return Resp()

    s = Sess()
    Enricher("ua", session=s).fetch("ko.wikipedia.org", [101, 102, 101, 102])
    assert s.params["revids"] == "101|102"
