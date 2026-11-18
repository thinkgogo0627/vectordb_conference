"""T-05: 임베딩 워커 + FAISS 서비스. 실험 1 조건(워커 수, 묶음 반영, embed_ms 배율)을 지원한다.

Kafka(Debezium 토픽) 소비 → (page_id, rev_id)로 사전 계산 벡터 조회 → embed_ms 만큼 대기 → FAISS 교체.
같은 프로세스에서 HTTP로 검색·상태를 제공한다 (평가 하네스 T-06가 사용).

조건
  --workers k          임베딩 슬롯 k개. 문서 번호로 나눠 배정해 같은 문서의 편집 순서는 지킨다.
                       임베딩은 사전 계산이라 "GPU k대"를 대기 시간으로 흉내 낸다 (GR-03: 주입 효과).
  --batch-interval S   S초마다 쌓인 편집을 한꺼번에 반영 (임베딩 대기는 묶음 안 embed_ms의 합)
  --batch-size N       N건이 쌓이면 반영. 마지막 덜 찬 묶음은 --batch-max-hold 초 뒤 반영
  --embed-scale x      embed_ms 대기 배율. 0이면 대기 없음

base 리비전(리플레이 직전 상태)은 시작할 때 FAISS에 바로 넣고, CDC로 오는 base 이벤트는 무시한다.

  GET  /health            {"assigned": bool, "applied": n, "ntotal": n}
  GET  /state             {"page_rev": {...}, "chunk_rev": {...}, ...}
  POST /pause, /resume    소비 일시정지·재개 (T-06 검증용)
  POST /chunk_revs        {"ids": [...]} -> {"revs": [...]}  (인덱스에 그 청크가 어느 리비전으로 있는지, 없으면 -1)
  POST /search            {"keys": [[page_id, rev_id, chunk_id]], "k": 1, "want_ids": [...]}

    python -m pipeline.worker --run-id A [--embed-scale 1.0] [--workers 1] [--batch-interval 0] [--batch-size 0]
"""
from __future__ import annotations

import argparse
import json
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np
from confluent_kafka import Consumer, KafkaException, TopicPartition

from . import config as C
from .embstore import load_by_rev
from .faiss_store import VersionedFlatIndex


def load_embeddings(path: str):  # 하위 호환 (check_t06 등)
    return load_by_rev(path)


class Service:
    def __init__(self, run_id: str, dim: int, by_rev: dict, embed_scale: float, log_path,
                 workers: int = 1, batch_interval: float = 0.0, batch_size: int = 0, batch_max_hold: float = 30.0):
        self.run_id = run_id
        self.store = VersionedFlatIndex(dim)
        self.by_rev = by_rev
        self.vec = {k: {cid: v for cid, v in d["chunks"]} for k, d in by_rev.items()}
        self.embed_scale = embed_scale
        self.workers = workers
        self.batch_interval = batch_interval
        self.batch_size = batch_size
        self.batch_max_hold = batch_max_hold
        self.assigned = False
        self.log = open(log_path, "a", encoding="utf-8")
        self.log_lock = threading.Lock()
        self.n_msgs = 0
        self.paused = False   # POST /pause 로 소비를 멈춘다 (T-06 검증용)
        self.queues = [queue.Queue() for _ in range(workers)]
        # base(리플레이 직전 상태) 미리 반영
        base = [(p, r, d["chunks"]) for (p, r), d in by_rev.items() if d.get("is_base")]
        if base:
            self.store.apply_many(base)
        self.n_base = len(base)

    # ---------------------------------------------------------------- consume
    def parse(self, msg) -> dict | None:
        consumed_at = time.time()
        payload = json.loads(msg.value())
        if payload is None:
            return None
        if "payload" in payload and "op" not in payload:
            payload = payload["payload"]
        if payload.get("op") not in ("c", "r", "u"):
            return None
        after = payload.get("after") or {}
        if after.get("run_id") != self.run_id or after.get("is_base"):
            return None
        _ts_type, kafka_ts_ms = msg.timestamp()
        return {"page_id": int(after["page_id"]), "rev_id": int(after["rev_id"]),
                "t0_host": after.get("t0_host"),
                "src_ts_ms": (payload.get("source") or {}).get("ts_ms"),
                "dbz_ts_ms": payload.get("ts_ms"), "kafka_ts_ms": kafka_ts_ms,
                "consumed_at": consumed_at}

    def consume_loop(self, stop: threading.Event) -> None:
        conf = {"bootstrap.servers": C.KAFKA_BOOTSTRAP, "group.id": f"worker-{self.run_id}",
                "auto.offset.reset": "latest", "enable.auto.commit": False,
                "fetch.wait.max.ms": 5, "fetch.min.bytes": 1}
        consumer = Consumer(conf)
        # 컨슈머 그룹 관리(subscribe)를 쓰지 않고 파티션을 직접 배정한다. 그룹 세션이 끊겨 재배정되면
        # 커밋된 오프셋이 없어 latest로 건너뛰며 이벤트를 잃는다 (run B에서 185건 유실로 실측).
        # 시작 시점의 끝 오프셋부터 읽는다 → 이전 run의 이벤트는 읽지 않는다.
        while not stop.is_set():
            md = consumer.list_topics(C.KAFKA_TOPIC, timeout=10).topics.get(C.KAFKA_TOPIC)
            if md is not None and md.error is None and md.partitions:
                break
            time.sleep(0.5)
        parts = []
        for pid in md.partitions:
            _lo, hi = consumer.get_watermark_offsets(TopicPartition(C.KAFKA_TOPIC, pid), timeout=10)
            parts.append(TopicPartition(C.KAFKA_TOPIC, pid, hi))
        consumer.assign(parts)
        self.assigned = True
        print(f"[worker] assigned {[(p.topic, p.partition, p.offset) for p in parts]}", flush=True)
        try:
            while not stop.is_set():
                if self.paused:
                    time.sleep(0.05)
                    continue
                msg = consumer.poll(0.05)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                ev = self.parse(msg)
                if ev is not None:
                    self.queues[ev["page_id"] % self.workers].put(ev)
        finally:
            consumer.close()

    # ---------------------------------------------------------------- process
    def _write(self, rows: list[dict]) -> None:
        with self.log_lock:
            for row in rows:
                self.log.write(json.dumps(row) + "\n")
            self.log.flush()
            self.n_msgs += len(rows)

    def _process(self, evs: list[dict], slot: int) -> None:
        """한 묶음(즉시 모드면 1건): embed 대기 → FAISS 반영."""
        known = [(ev, self.by_rev.get((ev["page_id"], ev["rev_id"]))) for ev in evs]
        embed_start = time.time()
        wait = sum(rec["embed_ms"] for _ev, rec in known if rec) * self.embed_scale / 1000.0
        if wait > 0:
            time.sleep(wait)
        embed_done = time.time()
        items = [(ev["page_id"], ev["rev_id"], rec["chunks"]) for ev, rec in known if rec]
        if len(items) == 1:
            results = [self.store.apply(*items[0])]
        else:
            results = self.store.apply_many(items) if items else []
        applied_at = time.time()
        it = iter(results)
        rows = []
        for ev, rec in known:
            ev = dict(ev, slot=slot, batch_n=len(evs), embed_start=embed_start, embed_done=embed_done,
                      applied_at=applied_at, embed_ms=rec["embed_ms"] if rec else None,
                      result=next(it) if rec else "no_embedding")
            rows.append(ev)
        self._write(rows)

    def slot_loop(self, slot: int, stop: threading.Event) -> None:
        q = self.queues[slot]
        buf: list[dict] = []
        next_tick = time.time() + self.batch_interval if self.batch_interval > 0 else None
        while not stop.is_set():
            if self.batch_interval <= 0 and self.batch_size <= 0:          # 즉시 반영
                try:
                    ev = q.get(timeout=0.05)
                except queue.Empty:
                    continue
                self._process([ev], slot)
                continue
            if self.batch_interval > 0:                                     # N초마다
                if time.time() >= next_tick:
                    if buf:
                        self._process(buf, slot)
                        buf = []
                    next_tick += self.batch_interval
                    continue
                try:
                    buf.append(q.get(timeout=min(max(0.0, next_tick - time.time()), 0.05)))
                except queue.Empty:
                    pass
                continue
            # N건마다
            try:
                buf.append(q.get(timeout=0.05))
            except queue.Empty:
                pass
            if buf and (len(buf) >= self.batch_size or time.time() - buf[0]["consumed_at"] >= self.batch_max_hold):
                self._process(buf, slot)
                buf = []


def make_handler(svc: Service):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"   # keep-alive (평가기가 연결 하나를 계속 쓴다)

        def log_message(self, *a):  # 조용히
            pass

        def _json(self, obj, code=200):
            b = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def do_GET(self):
            st = svc.store.state()
            if self.path == "/health":
                self._json({"assigned": svc.assigned, "applied": st["applied"], "ntotal": st["ntotal"],
                            "msgs": svc.n_msgs, "run_id": svc.run_id, "base": svc.n_base})
            elif self.path == "/state":
                self._json(st)
            else:
                self._json({"error": "not found"}, 404)

        def do_POST(self):
            if self.path in ("/pause", "/resume"):
                svc.paused = self.path == "/pause"
                return self._json({"paused": svc.paused})
            n = int(self.headers.get("Content-Length", 0))
            req = json.loads(self.rfile.read(n))
            if self.path == "/chunk_revs":
                return self._json({"revs": svc.store.chunk_revs(req["ids"]), "t_server": time.time()})
            if self.path != "/search":
                return self._json({"error": "not found"}, 404)
            if "keys" in req:   # [[page_id, rev_id, chunk_id], ...] → 워커가 가진 사전 계산 벡터로 질의 (전송 부담 없음)
                if not req["keys"]:
                    return self._json({"ids": [], "scores": [], "revs": [], "want_revs": [], "t_server": time.time()})
                vecs = np.stack([svc.vec[(int(p), int(r))][int(c)] for p, r, c in req["keys"]])
            else:
                vecs = np.asarray(req["vectors"], dtype=np.float32)
            ids, scores = svc.store.search(vecs, int(req.get("k", 1)))
            with svc.store.lock:
                revs = [[svc.store.chunk_rev.get(int(i), -1) for i in row] for row in ids]
                want_revs = [svc.store.chunk_rev.get(int(i), -1) for i in req.get("want_ids", [])]
            self._json({"ids": ids.tolist(), "scores": scores.tolist(), "revs": revs,
                        "want_revs": want_revs, "t_server": time.time()})
    return H


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--embeddings", default=str(C.EMBED_PATH))
    ap.add_argument("--embed-scale", type=float, default=1.0, help="embed_ms 대기 배율. 0이면 대기 없음 (GR-03 분리용)")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--batch-interval", type=float, default=0.0)
    ap.add_argument("--batch-size", type=int, default=0)
    ap.add_argument("--batch-max-hold", type=float, default=30.0)
    ap.add_argument("--port", type=int, default=C.WORKER_PORT)
    a = ap.parse_args()

    run_dir = C.RUNS_DIR / a.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    dim, by_rev = load_by_rev(a.embeddings)
    svc = Service(a.run_id, dim, by_rev, a.embed_scale, run_dir / "worker_log.jsonl",
                  a.workers, a.batch_interval, a.batch_size, a.batch_max_hold)
    print(f"[worker] run={a.run_id} dim={dim} revs={len(by_rev)} base={svc.n_base} ntotal={svc.store.index.ntotal} "
          f"embed_scale={a.embed_scale} workers={a.workers} batch_interval={a.batch_interval} "
          f"batch_size={a.batch_size}", flush=True)

    stop = threading.Event()
    threads = [threading.Thread(target=svc.consume_loop, args=(stop,), daemon=True)]
    threads += [threading.Thread(target=svc.slot_loop, args=(i, stop), daemon=True) for i in range(a.workers)]
    for th in threads:
        th.start()
    httpd = ThreadingHTTPServer(("127.0.0.1", a.port), make_handler(svc))
    print(f"[worker] http on {a.port}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        for th in threads:
            th.join(timeout=5)
        svc.log.close()


if __name__ == "__main__":
    main()
