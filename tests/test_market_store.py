"""bars_1m upsert: 재처리 안전성. 로컬 Postgres(docker-compose.exp.yml)가 없으면 건너뛴다."""
import psycopg
import pytest

from market.store import DSN, Bar, Store

T0 = 1_795_444_200_000


@pytest.fixture
def store():
    try:
        s = Store()
    except psycopg.OperationalError:
        pytest.skip("로컬 Postgres 없음")
    with s.conn.cursor() as cur:
        cur.execute("DELETE FROM bars_1m WHERE source = 'test'")
    s.conn.commit()
    yield s
    with s.conn.cursor() as cur:
        cur.execute("DELETE FROM bars_1m WHERE source = 'test'")
    s.conn.commit()
    s.close()


def bar(c, recv):
    return Bar("test", "X", T0, "crypto", 1.0, 2.0, 0.5, c, 10.0, 3, 1.2, recv)


def rev_and_close(s):
    with s.conn.cursor() as cur:
        cur.execute("SELECT revision, c FROM bars_1m WHERE source='test'")
        r = cur.fetchone()
    s.conn.commit()
    return r


def test_insert_then_same_value_is_unchanged(store):
    assert store.write_bars([bar(1.5, 100)])["inserted"] == 1
    assert store.write_bars([bar(1.5, 100)])["unchanged"] == 1
    assert rev_and_close(store) == (0, 1.5)


def test_correction_bumps_revision_and_older_message_cannot_revert(store):
    store.write_bars([bar(1.5, 100)])                  # 원본 (b)
    assert store.write_bars([bar(1.7, 130)])["updated"] == 1   # 정정 (u), 더 늦게 받음
    assert store.write_bars([bar(1.5, 100)])["unchanged"] == 1  # 되감기로 원본을 다시 받아도 되돌아가지 않음
    assert store.write_bars([bar(1.7, 130)])["unchanged"] == 1
    assert rev_and_close(store) == (1, 1.7)
