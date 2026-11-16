"""T-05: 임베딩 워커 + FAISS 서비스.

Kafka(Debezium 토픽) 소비 → (page_id, rev_id)로 사전 계산 벡터 조회 → embed_ms 만큼 대기 → FAISS 교체.
같은 프로세스에서 HTTP로 검색·상태를 제공한다 (평가 하네스 T-06가 사용).

  GET  /health            {"assigned": bool, "applied": n, "ntotal": n}
  GET  /state             {"page_rev": {...}, "chunk_rev": {...}, ...}
  POST /pause, /resume    소비 일시정지·재개 (T-06 검증용)
  POST /search            {"keys": [[page_id, rev_id, chunk_id]], "k": 1, "want_ids": [...]} -> {"ids": [[...]], "scores": [[...]], "revs": [[...]]}

    python -m pipeline.worker --run-id A [--embed-scale 1.0]
"""
from __future__ import annotations

import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException, TopicPartition

from . import config as C
from .faiss_store import VersionedFlatIndex


def load_embeddings(path: str):
    t = pq.read_table(path)
    dim = t.schema.field("vector").type.list_size
    by_rev: dict[tuple[int, int], dict] = {}
    cols = {k: t.column(k).to_pylist() for k in ("page_id", "rev_id", "chunk_id", "embed_ms")}
    vecs = np.asarray(t.column("vector").combine_chunks().flatten().to_numpy(zero_copy_only=False),
                      dtype=np.float32).reshape(-1, dim)
    for i in range(t.num_rows):
        key = (cols["page_id"][i], cols["rev_id"][i])
        d = by_rev.setdefault(key, {"embed_ms": cols["embed_ms"][i], "chunks": []})
        d["chunks"].append((cols["chunk_id"][i], vecs[i]))
    return dim, by_rev


class Service:
    def __init__(self, run_id: str, dim: int, by_rev: dict, embed_scale: float, log_path):
        self.run_id = run_id
        self.store = VersionedFlatIndex(dim)
        self.by_rev = by_rev
        self.vec = {k: {cid: v for cid, v in d["chunks"]} for k, d in by_rev.items()}
        self.embed_scale = embed_scale
        self.assigned = False
        self.log = open(log_path, "a", encoding="utf-8")
        self.n_msgs = 0
        self.paused = False   # POST /pause 로 소비를 멈춘다 (T-06 검증용)

    def handle(self, msg) -> None:
        consumed_at = time.time()
        payload = json.loads(msg.value())
        if payload is None:
            return
        # Debezium 봉투: {"before","after","source","op","ts_ms"} (schemas.enable=false)
        if "payload" in payload and "op" not in payload:
            payload = payload["payload"]
        if payload.get("op") not in ("c", "r", "u"):
            return
        after = payload.get("after") or {}
        if after.get("run_id") != self.run_id:
            return
        page_id, rev_id = int(after["page_id"]), int(after["rev_id"])
        rec = self.by_rev.get((page_id, rev_id))
        _ts_type, kafka_ts_ms = msg.timestamp()
        row = {"page_id": page_id, "rev_id": rev_id,
               "t0_host": after.get("t0_host"),
               "src_ts_ms": (payload.get("source") or {}).get("ts_ms"),
               "dbz_ts_ms": payload.get("ts_ms"), "kafka_ts_ms": kafka_ts_ms,
               "consumed_at": consumed_at}
        if rec is None:
            row.update(result="no_embedding", applied_at=time.time(), embed_ms=None)
        else:
            wait = rec["embed_ms"] * self.embed_scale / 1000.0
            if wait > 0:
                time.sleep(wait)
            res = self.store.apply(page_id, rev_id, rec["chunks"])
            row.update(result=res, applied_at=time.time(), embed_ms=rec["embed_ms"])
        self.n_msgs += 1
        self.log.write(json.dumps(row) + "\n")
        self.log.flush()

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
                self.handle(msg)
        finally:
            consumer.close()


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
                            "msgs": svc.n_msgs, "run_id": svc.run_id})
            elif self.path == "/state":
                self._json(st)
            else:
                self._json({"error": "not found"}, 404)

        def do_POST(self):
            if self.path in ("/pause", "/resume"):
                svc.paused = self.path == "/pause"
                return self._json({"paused": svc.paused})
            if self.path != "/search":
                return self._json({"error": "not found"}, 404)
            n = int(self.headers.get("Content-Length", 0))
            req = json.loads(self.rfile.read(n))
            if "keys" in req:   # [[page_id, rev_id, chunk_id], ...] → 워커가 가진 사전 계산 벡터로 질의 (전송 부담 없음)
                vecs = np.stack([svc.vec[(int(p), int(r))][int(c)] for p, r, c in req["keys"]])
            else:
                vecs = np.asarray(req["vectors"], dtype=np.float32)
            ids, scores = svc.store.search(vecs, int(req.get("k", 1)))
            with svc.store.lock:
                revs = [[svc.store.chunk_rev.get(int(i), -1) for i in row] for row in ids]
                # 질의한 청크 자체가 인덱스에 어느 리비전으로 있는지 (누락 판정용)
                want_revs = [svc.store.chunk_rev.get(int(i), -1) for i in req.get("want_ids", [])]
            self._json({"ids": ids.tolist(), "scores": scores.tolist(), "revs": revs,
                        "want_revs": want_revs, "t_server": time.time()})
    return H


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--embeddings", default=str(C.EMBED_PATH))
    ap.add_argument("--embed-scale", type=float, default=1.0, help="embed_ms 대기 배율. 0이면 대기 없음 (GR-03 분리용)")
    ap.add_argument("--port", type=int, default=C.WORKER_PORT)
    a = ap.parse_args()

    run_dir = C.RUNS_DIR / a.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    dim, by_rev = load_embeddings(a.embeddings)
    svc = Service(a.run_id, dim, by_rev, a.embed_scale, run_dir / "worker_log.jsonl")
    print(f"[worker] run={a.run_id} dim={dim} revs={len(by_rev)} embed_scale={a.embed_scale}", flush=True)

    stop = threading.Event()
    th = threading.Thread(target=svc.consume_loop, args=(stop,), daemon=True)
    th.start()
    httpd = ThreadingHTTPServer(("127.0.0.1", a.port), make_handler(svc))
    print(f"[worker] http on {a.port}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        th.join(timeout=5)
        svc.log.close()


if __name__ == "__main__":
    main()
