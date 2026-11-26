"""⑤·⑥ 벡터 워커: CDC(market.public.bars_1m) → 종목별 최근 1분봉 → 30차원 상태 벡터 → 현재 상태 인덱스(Flat).
기능 1 "지금 비슷하게 움직이는 종목"을 HTTP로 제공한다.

    python -m market.vector_worker                     # http://localhost:8780
    curl "localhost:8780/similar?symbol=KRW-BTC&k=10&scope=all"
    curl "localhost:8780/status"

- 시작: Kafka 끝 오프셋을 먼저 잡고 → DB에서 최근 80분 1분봉을 읽은 뒤 → 잡아 둔 오프셋부터 CDC를 소비한다.
  그 사이에 바뀐 행은 CDC로 다시 오며, 수신 시각(recv_ts)이 더 늦은 값만 반영하므로 결과가 같다.
- 소스(업비트·Alpaca)마다 지금 시각 t = 들어온 가장 늦은 1분봉 + 1분. t가 바뀌고 그 소스의 메시지가 1초 동안
  멈추면(한 분의 봉이 다 들어옴) 그 소스 종목 전부를 다시 계산한다. 지난 분의 정정이 오면 그 종목만 다시 계산한다.
- 현재 상태 인덱스는 종목당 벡터 1개, 약 2,300개라 Flat(전수조사)으로 둔다. 매분 통째로 바뀌므로 꺼두기를 쓰지 않는다.
- 지연: 1분봉마다 (인덱스에 반영된 시각 − 수집기 수신 시각), (반영 − bar_builder 쓰기)를 호스트 시계로 잰다.
"""
from __future__ import annotations

import argparse
import json
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import numpy as np
from confluent_kafka import Consumer, TopicPartition

from . import config as C
from .cdc import TOPIC
from .features import DIM, MIN, Ref, state_vector
from .store import Store

KEEP_MIN = 80
SETTLE_S = 1.0


def _ms(ts) -> int:
    if isinstance(ts, str):
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
    return int(ts.timestamp() * 1000)


class Worker:
    def __init__(self, store: Store | None = None):
        self.lock = threading.Lock()
        self.store = store if store is not None else Store()
        self.refs: dict[tuple[str, str], Ref] = {}
        self.cls: dict[tuple[str, str], str] = {}
        self.bars: dict[tuple[str, str], dict[int, tuple]] = defaultdict(dict)
        self.recv: dict[tuple[str, str, int], int] = {}
        self.t_src: dict[str, int] = {}
        self.computed_t: dict[str, int] = {}
        self.last_msg_wall: dict[str, float] = {}
        self.dirty: set[tuple[str, str]] = set()
        self.pending_lat: list[tuple[str, int, int]] = []        # (source, recv_ts, built_ts) 아직 반영 전
        self.lat = {"upbit": deque(maxlen=5000), "alpaca": deque(maxlen=5000)}
        self.lat_built = {"upbit": deque(maxlen=5000), "alpaca": deque(maxlen=5000)}
        self.lat_cdc = {"upbit": deque(maxlen=5000), "alpaca": deque(maxlen=5000)}   # DB 쓰기 → CDC 도착
        self.index: dict[tuple[str, str], tuple[np.ndarray, int, int, dict]] = {}   # 벡터, t, 반영 시각, 정보
        self.reasons: dict[tuple[str, str], str] = {}
        self.mat = np.zeros((0, DIM), np.float32)
        self.keys: list[tuple[str, str]] = []
        self.stats = defaultdict(int)

    # ---------- 준비 ----------
    def load_refs(self) -> None:
        with self.store.conn.cursor() as cur:
            cur.execute("SELECT source, symbol, asset_class, sigma5, vref, fill5, in_universe FROM symbol_ref")
            for s, sym, ac, sg, vref, f, inu in cur.fetchall():
                self.refs[(s, sym)] = Ref(sg, np.array(vref, float), f, inu)
                self.cls[(s, sym)] = ac
        self.store.conn.commit()
        print(f"[vec] 평소 값 {len(self.refs)}종목, 오늘 목록 {sum(r.in_universe for r in self.refs.values())}종목", flush=True)

    def bootstrap(self) -> None:
        with self.store.conn.cursor() as cur:
            for s in ("upbit", "alpaca"):
                # 소스별 가장 늦은 분: 최근 하루 안에서 찾는다 (minute_ts 인덱스를 타게)
                cur.execute("SELECT max(minute_ts) FROM bars_1m WHERE minute_ts > now() - interval '4 days' AND source = %s", (s,))
                last = cur.fetchone()[0]
                if last is None:
                    continue
                cur.execute("""SELECT b.symbol, b.minute_ts, b.o, b.h, b.l, b.c, b.v, b.recv_ts FROM bars_1m b
                               JOIN symbol_ref r ON r.source = b.source AND r.symbol = b.symbol AND r.in_universe
                               WHERE b.source = %s AND b.minute_ts > %s - make_interval(mins => %s)""", (s, last, KEEP_MIN))
                for sym, m, o, h, l, c, v, rv in cur.fetchall():
                    ms = _ms(m)
                    self.bars[(s, sym)][ms] = (o, h, l, c, v)
                    self.recv[(s, sym, ms)] = rv
                    self.t_src[s] = max(self.t_src.get(s, 0), ms + MIN)
        self.store.conn.commit()
        for s, t in self.t_src.items():
            self.recompute(s, t, all_symbols=True)
        print(f"[vec] 시작 상태: {{{', '.join(f'{s}: t={datetime.fromtimestamp(t / 1000, timezone.utc):%H:%M}' for s, t in self.t_src.items())}}} "
              f"인덱스 {len(self.index)}", flush=True)

    # ---------- CDC ----------
    def on_change(self, after: dict) -> None:
        s, sym = after["source"], after["symbol"]
        key = (s, sym)
        if key not in self.refs or not self.refs[key].in_universe:
            self.stats["skipped_not_universe"] += 1
            return
        m = _ms(after["minute_ts"])
        rv = int(after["recv_ts"])
        if self.recv.get((s, sym, m), -1) > rv:
            self.stats["older_ignored"] += 1
            return
        self.recv[(s, sym, m)] = rv
        self.bars[key][m] = (after["o"], after["h"], after["l"], after["c"], after["v"])
        self.last_msg_wall[s] = time.time()
        if rv > 0:
            self.pending_lat.append((s, rv, int(after["built_ts"])))
            self.lat_cdc[s].append(int(self.last_msg_wall[s] * 1000) - int(after["built_ts"]))
        if m + MIN > self.t_src.get(s, 0):
            self.t_src[s] = m + MIN
        elif m + MIN <= self.computed_t.get(s, 0):
            self.dirty.add(key)                          # 이미 계산한 분의 정정
            self.stats["corrections"] += 1

    def tick(self) -> None:
        now = time.time()
        for s, t in list(self.t_src.items()):
            if t != self.computed_t.get(s) and now - self.last_msg_wall.get(s, 0) >= SETTLE_S:
                self.recompute(s, t, all_symbols=True)
        if self.dirty:
            for s in {k[0] for k in self.dirty}:
                self.recompute(s, self.computed_t.get(s, self.t_src[s]), all_symbols=False)

    def recompute(self, s: str, t: int, all_symbols: bool) -> None:
        keys = [k for k in self.refs if k[0] == s and self.refs[k].in_universe] if all_symbols else \
            [k for k in self.dirty if k[0] == s]
        built_at = int(time.time() * 1000)
        n_ok = 0
        with self.lock:
            for k in keys:
                b = self.bars.get(k, {})
                for m in [m for m in b if m < t - KEEP_MIN * MIN]:
                    del b[m]
                    self.recv.pop((k[0], k[1], m), None)
                vec, info = state_vector(b, t, self.refs[k], self.cls[k])
                self.reasons[k] = info["reason"]
                if vec is None:
                    self.index.pop(k, None)
                else:
                    self.index[k] = (vec, t, built_at, info)
                    n_ok += 1
            self.dirty -= set(keys)
            self.keys = list(self.index)
            self.mat = np.stack([self.index[k][0] for k in self.keys]) if self.keys else np.zeros((0, DIM), np.float32)
            done = [p for p in self.pending_lat if p[0] == s]
            self.pending_lat = [p for p in self.pending_lat if p[0] != s]
            for _, rv, bt in done:
                self.lat[s].append(built_at - rv)
                self.lat_built[s].append(built_at - bt)
        self.computed_t[s] = max(self.computed_t.get(s, 0), t)
        self.stats[f"recompute_{s}"] += 1
        if all_symbols:
            l = np.array(self.lat[s]) if self.lat[s] else None
            print(f"[vec] {s} t={datetime.fromtimestamp(t / 1000, timezone.utc):%H:%M} 유효 {n_ok}/{len(keys)} "
                  f"인덱스 전체 {len(self.keys)}"
                  + (f" | 수집기 수신→검색 가능 p50 {np.percentile(l, 50) / 1000:.2f}s p95 {np.percentile(l, 95) / 1000:.2f}s"
                     if l is not None else ""), flush=True)

    # ---------- 검색 (기능 1) ----------
    def similar(self, symbol: str, k: int, scope: str, include_stale: bool) -> dict:
        with self.lock:
            key = next((kk for kk in self.keys if kk[1] == symbol), None)
            if key is None:
                src = next((kk for kk in self.refs if kk[1] == symbol), None)
                return {"symbol": symbol, "error": "벡터 없음",
                        "reason": self.reasons.get(src, "unknown_symbol" if src is None else "no_data")}
            q, tq, built_q, info_q = self.index[key]
            fresh_t = dict(self.computed_t)
            mask = np.ones(len(self.keys), bool)
            for i, kk in enumerate(self.keys):
                vec_t = self.index[kk][1]
                if kk == key or (scope != "all" and self.cls[kk] != scope):
                    mask[i] = False
                elif not include_stale and vec_t < fresh_t.get(kk[0], vec_t):
                    mask[i] = False
            idx = np.flatnonzero(mask)
            d = np.linalg.norm(self.mat[idx] - q, axis=1)
            order = idx[np.argsort(d)[:k]]
            dist = np.sort(d)[:k]
            now = int(time.time() * 1000)
            res = [{"symbol": self.keys[i][1], "asset_class": self.cls[self.keys[i]], "distance": round(float(dd), 4),
                    "as_of": datetime.fromtimestamp(self.index[self.keys[i]][1] / 1000, timezone.utc).isoformat(),
                    "indexed_s_ago": round((now - self.index[self.keys[i]][2]) / 1000, 1)}
                   for i, dd in zip(order, dist)]
            return {"symbol": symbol, "as_of": datetime.fromtimestamp(tq / 1000, timezone.utc).isoformat(),
                    "indexed_s_ago": round((now - built_q) / 1000, 1), "candidates": int(len(idx)), "results": res}

    def status(self) -> dict:
        def pct(dq):
            return {"p50_s": round(float(np.percentile(dq, 50)) / 1000, 3), "p95_s": round(float(np.percentile(dq, 95)) / 1000, 3),
                    "n": len(dq)} if dq else None
        with self.lock:
            by = defaultdict(int)
            for k in self.keys:
                by[self.cls[k]] += 1
            reasons = defaultdict(int)
            for k, r in self.reasons.items():
                reasons[r] += 1
        return {"t": {s: datetime.fromtimestamp(t / 1000, timezone.utc).isoformat() for s, t in self.computed_t.items()},
                "index": dict(by), "reasons": dict(reasons), "stats": dict(self.stats),
                "collector_recv_to_searchable": {s: pct(d) for s, d in self.lat.items()},
                "bar_written_to_searchable": {s: pct(d) for s, d in self.lat_built.items()},
                "bar_written_to_cdc_arrival": {s: pct(d) for s, d in self.lat_cdc.items()}}


def serve(w: Worker, port: int) -> None:
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            if u.path == "/similar":
                body = w.similar(q.get("symbol", ""), int(q.get("k", 10)), q.get("scope", "all"), q.get("stale") == "1")
            elif u.path == "/status":
                body = w.status()
            else:
                self.send_response(404)
                self.end_headers()
                return
            data = json.dumps(body, ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(data)

    threading.Thread(target=ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever, daemon=True).start()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8780)
    ap.add_argument("--seconds", type=float, default=None)
    a = ap.parse_args()
    w = Worker()
    w.load_refs()
    c = Consumer({"bootstrap.servers": C.KAFKA, "group.id": "market-vector-worker", "enable.auto.commit": False})
    parts = c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    tps = []
    for p in parts:                                   # 끝 오프셋을 먼저 잡는다
        lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
        tps.append(TopicPartition(TOPIC, p, hi))
    w.bootstrap()
    c.assign(tps)
    serve(w, a.port)
    print(f"[vec] http://localhost:{a.port}/similar?symbol=KRW-BTC", flush=True)
    t_end = time.time() + a.seconds if a.seconds else None
    try:
        while t_end is None or time.time() < t_end:
            for m in c.consume(5000, timeout=0.2):
                if m.error() or m.value() is None:
                    continue
                v = json.loads(m.value())
                if v.get("op") in ("c", "u", "r") and v.get("after"):
                    with w.lock:
                        w.on_change(v["after"])
            w.tick()
    except KeyboardInterrupt:
        pass
    finally:
        c.close()


if __name__ == "__main__":
    main()
