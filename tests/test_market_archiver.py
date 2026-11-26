"""원천 메시지 보관기: 파일 이름, Parquet 내용, 실패 시 커밋하지 않음."""
import io

import pyarrow.parquet as pq

from market.archiver import Archiver

TS = 1_795_444_200_000      # 2026-11-23 14:30 UTC


def test_flush_writes_one_file_per_partition_and_commits_next_offset():
    a = Archiver(prefix="market")
    for off in (12, 10, 11):                                    # 순서가 섞여 들어와도 정렬한다
        a.add("raw.upbit.trade", 0, off, TS + off, b"KRW-BTC", b'{"x": %d}' % off)
    a.add("raw.alpaca.bar", 0, 5, TS, b"AAPL", b"{}")
    put = {}
    commits, st = a.flush(lambda k, d: put.__setitem__(k, d))
    assert sorted(commits) == [("raw.alpaca.bar", 0, 6), ("raw.upbit.trade", 0, 13)]
    key = "market/raw/raw.upbit.trade/date=2026-11-23/p0-000000000010-000000000012.parquet"
    assert key in put and st["files"] == 2 and st["rows"] == 4 and a.rows == 0
    t = pq.read_table(io.BytesIO(put[key])).to_pydict()
    assert t["offset"] == [10, 11, 12] and t["key"] == ["KRW-BTC"] * 3 and t["value"][0] == '{"x": 10}'


def test_failed_upload_keeps_buffer_and_does_not_commit():
    a = Archiver(prefix="market")
    a.add("raw.upbit.trade", 0, 1, TS, None, b"{}")

    def boom(k, d):
        raise ConnectionError("r2 down")

    commits, st = a.flush(boom)
    assert commits == [] and st["failed"] == 1 and a.rows == 1
    commits, _ = a.flush(lambda k, d: None)                    # 다음 주기에 다시 올린다
    assert commits == [("raw.upbit.trade", 0, 2)] and a.rows == 0
