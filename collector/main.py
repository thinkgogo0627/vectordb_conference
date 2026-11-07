"""위키 편집 스트림 수집기 진입점.

흐름:  SSE 구독 → 필터 → 행 변환 → (rev_id 모아서) Action API 보강
      → 버퍼 → Parquet(outbox) → 업로더가 R2/S3로 올림

보장:  at-least-once. 재시작 시 마지막으로 Parquet에 기록된 이벤트 다음부터 다시 받는다.
      중복이 생길 수 있으니 읽는 쪽에서 event_id로 dedup 한다.
"""
from __future__ import annotations

import logging
import os
import signal
import threading
import time
from collections import defaultdict

from .config import Config
from .enrich import Enricher, apply_enrichment
from .sink import Uploader, write_parts
from .stream import StateStore, stream_events
from .transform import keep_event, to_row

log = logging.getLogger("collector")


class Collector:
    def __init__(self, cfg: Config, enricher: Enricher | None = None, uploader: Uploader | None = None):
        self.cfg = cfg
        self._stop = threading.Event()
        self.enricher = enricher or Enricher(cfg.http_user_agent, cfg.fetch_content, stop=self._stop.is_set)
        self.uploader = uploader or Uploader(cfg)
        self.state = StateStore(os.path.join(cfg.data_dir, "state", "last_event_id"))
        self.pending: list[dict] = []        # 보강 대기
        self.buffer: list[dict] = []         # Parquet 대기
        self.last_flush = time.monotonic()
        self.last_enrich = time.monotonic()
        self.stats = defaultdict(int)
        self.last_seen_id: str | None = None
        self.heartbeat_path = os.path.join(cfg.data_dir, "state", "heartbeat")

    # ---- 단계별 처리 ----
    def handle(self, event_id: str | None, evt: dict) -> None:
        self.stats["seen"] += 1
        if event_id:
            self.last_seen_id = event_id   # 필터로 버린 이벤트도 '처리 완료'로 본다
        if not keep_event(evt, self.cfg.wikis, self.cfg.namespaces, self.cfg.include_bots):
            return
        row = to_row(event_id, evt)
        self.stats[f"op_{row['op']}"] += 1
        (self.pending if row["content_status"] == "pending" else self.buffer).append(row)
        self.maybe_enrich()
        self.maybe_flush()

    def maybe_enrich(self, force: bool = False) -> None:
        due = time.monotonic() - self.last_enrich >= self.cfg.enrich_max_wait_s
        if not self.pending or not (force or due or len(self.pending) >= self.cfg.enrich_batch_size):
            return
        by_domain: dict[str, list[dict]] = defaultdict(list)
        for r in self.pending:
            by_domain[r["domain"]].append(r)
        for domain, rows in by_domain.items():
            for i in range(0, len(rows), self.cfg.enrich_batch_size):
                chunk = rows[i:i + self.cfg.enrich_batch_size]
                info = self.enricher.fetch(domain, [r["rev_id"] for r in chunk])
                apply_enrichment(chunk, info)
        self.buffer.extend(self.pending)
        self.pending = []
        self.last_enrich = time.monotonic()

    def maybe_flush(self, force: bool = False) -> None:
        due = time.monotonic() - self.last_flush >= self.cfg.flush_seconds
        if not (force or due or len(self.buffer) + len(self.pending) >= self.cfg.flush_rows):
            return
        # 보강 대기분까지 모두 처리한 뒤 기록 → 재개 위치보다 앞선 이벤트가 남지 않음
        self.maybe_enrich(force=True)
        if self.buffer:
            parts = write_parts(self.buffer, os.path.join(self.cfg.data_dir, "outbox"), self.cfg.s3_prefix)
            self.stats["rows_written"] += len(self.buffer)
            log.info("flush: rows=%d files=%d | %s", len(self.buffer), len(parts), dict(self.stats))
            self.buffer = []
        # Parquet에 안전하게 기록된 뒤에만 재개 위치를 전진시킨다
        if self.last_seen_id:
            self.state.save(self.last_seen_id)
        self.last_flush = time.monotonic()
        self._heartbeat()

    def _heartbeat(self) -> None:
        os.makedirs(os.path.dirname(self.heartbeat_path), exist_ok=True)
        with open(self.heartbeat_path, "w") as f:
            f.write(str(int(time.time())))

    # ---- 실행 ----
    def run(self) -> None:
        up = threading.Thread(target=self.uploader.run_forever, args=(self._stop.is_set,), daemon=True)
        up.start()
        ticker = threading.Thread(target=self._tick, daemon=True)  # 이벤트가 뜸해도 주기적 flush
        ticker.start()
        resume = self.state.load()
        log.info("start: wikis=%s ns=%s resume_from=%s", self.cfg.wikis, self.cfg.namespaces, resume)
        try:
            for eid, evt in stream_events(self.cfg.stream_url, self.cfg.http_user_agent, resume, self._stop.is_set):
                with self._lock:
                    self.handle(eid, evt)
        finally:
            self.shutdown()
            up.join(timeout=30)
            ok, fail = self.uploader.upload_all()   # 종료 직전 flush한 파일까지 업로드
            log.info("shutdown upload: ok=%d fail=%d (남은 파일은 다음 실행 때 업로드)", ok, fail)

    _lock = threading.Lock()

    def _tick(self) -> None:
        while not self._stop.is_set():
            time.sleep(5)
            with self._lock:
                self.maybe_enrich()
                self.maybe_flush()

    def shutdown(self) -> None:
        with self._lock:
            self.maybe_enrich(force=True)
            self.maybe_flush(force=True)
        self._stop.set()


def main() -> None:
    cfg = Config.from_env()
    logging.basicConfig(level=cfg.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not cfg.contact:
        log.warning("CONTACT가 비어 있음: Wikimedia API 정책상 User-Agent에 연락처를 넣어야 함")
    c = Collector(cfg)

    def _sig(*_):
        log.info("stop signal, flushing...")
        c._stop.set()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    c.run()


if __name__ == "__main__":
    main()
