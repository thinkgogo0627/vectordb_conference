"""② 1분봉 정규화: 업비트 체결 → 1분봉, Alpaca 변환."""
from market.bars import MIN, UpbitMinuteBars, alpaca_bar, parse_rfc3339_ms

T0 = 1_795_444_200_000          # 2026-11-23 14:30:00 UTC (분 경계)


def tr(code, ts, seq, p, q):
    return {"code": code, "trade_timestamp": ts, "sequential_id": seq, "trade_price": p, "trade_volume": q}


def test_bar_close_ohlcv_and_out_of_order():
    b = UpbitMinuteBars(grace_ms=5_000, start_after_ms=T0)
    # 늦게 도착했지만 시각이 이른 체결이 시가가 된다
    for t in [tr("KRW-BTC", T0 + 2_000, 2, 101, 1), tr("KRW-BTC", T0 + 1_000, 1, 100, 2),
              tr("KRW-BTC", T0 + 30_000, 3, 105, 1), tr("KRW-BTC", T0 + 59_000, 4, 99, 4)]:
        assert b.add(t, recv=t["trade_timestamp"] + 100) == []
    assert b.advance(T0 + 64_200) == []                  # 워터마크 = 59 + (64.2−59.1) − 5 = 59.1s < 60s
    bars = b.advance(T0 + 66_200)                        # 워터마크 = 59 + (66.2−59.1) − 5 = 61.1s ≥ 60s
    assert len(bars) == 1
    x = bars[0]
    assert (x.o, x.h, x.l, x.c, x.v, x.n) == (100, 105, 99, 99, 8, 4)
    assert abs(x.vw - (101 + 200 + 105 + 396) / 8) < 1e-9
    assert x.minute_ms == T0 and x.source == "upbit" and x.asset_class == "crypto"


def test_duplicate_trade_counted_once():
    b = UpbitMinuteBars(start_after_ms=T0)
    t = tr("KRW-ETH", T0 + 1_000, 7, 10, 1)
    b.add(t, T0 + 1_100)
    b.add(dict(t), T0 + 1_200)
    bars = b.advance(T0 + 2 * MIN)
    assert bars[0].n == 1 and bars[0].v == 1 and b.stats["dup"] == 1


def test_late_trade_updates_closed_bar_then_too_late_dropped():
    b = UpbitMinuteBars(grace_ms=1_000, keep_ms=2 * MIN, start_after_ms=T0)
    b.add(tr("KRW-XRP", T0 + 10_000, 1, 1.0, 1), T0 + 10_000)
    b.add(tr("KRW-BTC", T0 + 70_000, 2, 50, 1), T0 + 70_000)     # 워터마크를 다음 분으로 민다
    assert [x.symbol for x in b.advance(T0 + 70_000)] == ["KRW-XRP"]
    late = b.add(tr("KRW-XRP", T0 + 20_000, 3, 2.0, 1), T0 + 71_000)
    assert len(late) == 1 and late[0].h == 2.0 and late[0].n == 2 and b.stats["late_updates"] == 1
    b.add(tr("KRW-BTC", T0 + 5 * MIN, 4, 50, 1), T0 + 5 * MIN)
    b.advance(T0 + 5 * MIN)                                      # 오래된 상태 정리
    assert b.add(tr("KRW-XRP", T0 + 30_000, 5, 3.0, 1), T0 + 5 * MIN) == []
    assert b.stats["late_dropped"] == 1


def test_first_partial_minute_skipped_after_restart():
    b = UpbitMinuteBars(grace_ms=5_000)                  # start_after 없음 → 첫 체결로 정한다
    b.add(tr("KRW-BTC", T0 + 40_000, 1, 1, 1), T0 + 40_000)     # 이 분은 앞부분을 못 봤을 수 있다
    b.add(tr("KRW-BTC", T0 + MIN + 1_000, 2, 1, 1), T0 + MIN + 1_000)
    bars = b.advance(T0 + 3 * MIN)
    assert [x.minute_ms for x in bars] == [T0 + MIN] and b.stats["partial_skipped"] == 1


def test_session_start_drops_partial_minutes_around_gap():
    b = UpbitMinuteBars(grace_ms=1_000, start_after_ms=T0)
    b.add(tr("KRW-BTC", T0 + 10_000, 1, 1, 1), T0 + 10_000)          # 분 0: 완전
    b.add(tr("KRW-BTC", T0 + MIN + 5_000, 2, 1, 1), T0 + MIN + 5_000)  # 분 1: 여기서 끊김 (끝부분 못 봄)
    assert [x.minute_ms for x in b.advance(T0 + MIN + 5_000)] == [T0]
    b.session_start()                                                   # 3분 뒤 다시 연결
    b.add(tr("KRW-BTC", T0 + 4 * MIN + 30_000, 3, 1, 1), T0 + 4 * MIN + 30_000)  # 분 4: 앞부분 못 봄
    b.add(tr("KRW-BTC", T0 + 5 * MIN + 1_000, 4, 1, 1), T0 + 5 * MIN + 1_000)    # 분 5: 완전
    assert [x.minute_ms for x in b.advance(T0 + 7 * MIN)] == [T0 + 5 * MIN]
    assert b.stats["partial_dropped"] == 1 and b.stats["partial_skipped"] == 1


def test_alpaca_bar():
    m = {"T": "b", "S": "AAPL", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 300, "n": 4, "vw": 1.2,
         "t": "2026-11-23T14:30:00Z"}
    x = alpaca_bar(m, recv_ts=5)
    assert (x.source, x.symbol, x.minute_ms, x.asset_class, x.n) == ("alpaca", "AAPL", T0, "us_equity", 4)
    assert parse_rfc3339_ms("2026-11-23T14:30:00.123456789Z") == T0 + 123
