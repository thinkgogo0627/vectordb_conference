"""T-07: 실험 0 한 번 실행 (명령 하나). 실험 1 조건(--workers, --batch-*, --embed-scale, --data-dir)도 같은 명령으로 돈다.

  1. Debezium 커넥터 RUNNING 확인
  2. 워커(T-05) 시작 → Kafka 파티션 배정 대기
  3. 평가 하네스(T-06) 시작
  4. 리플레이어(T-04) 실행 → 종료 대기
  5. 평가 하네스 종료 대기 → 워커 종료
  6. run 디렉터리 meta.json 기록, 곡선 그림

    python -m pipeline.run_exp0 --run-id A --speed 1
    python -m pipeline.run_exp0 --run-id B --speed 1
    python -m pipeline.plot A B
    python -m pipeline.run_exp0 --data-dir data/exp1 --subset data/exp1/subset.parquet --embeddings data/exp1/emb         --run-id s100-a --speed 100
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from email.utils import parsedate_to_datetime
from pathlib import Path

import pyarrow.parquet as pq
import requests

from . import config as C
from .register_connector import register


def _py(mod: str, *args: str) -> list[str]:
    return [sys.executable, "-m", mod, *args]


def host_clock_offset_s() -> float | None:
    """호스트 시계 - 인터넷 시각(HTTP Date, 초 단위). 측정 기준 시계가 틀어졌는지 확인용."""
    try:
        t0 = time.time()
        r = requests.head("https://www.cloudflare.com", timeout=5)
        t1 = time.time()
        return round((t0 + t1) / 2 - parsedate_to_datetime(r.headers["Date"]).timestamp(), 2)
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--speed", type=float, default=1.0)
    ap.add_argument("--embed-scale", type=float, default=1.0)
    ap.add_argument("--subset", default=str(C.SUBSET_PATH))
    ap.add_argument("--embeddings", default=str(C.EMBED_PATH))
    ap.add_argument("--max-wait", type=float, default=120.0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--data-dir", default=None, help="결과 루트 (run은 <data-dir>/runs/<run-id>). 기본 data/exp0")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--batch-interval", type=float, default=0.0)
    ap.add_argument("--batch-size", type=int, default=0)
    ap.add_argument("--ignore-clock", action="store_true", help="호스트 시계가 5초 넘게 틀려도 진행")
    ap.add_argument("--label", default="", help="그래프 범례용 조건 이름")
    ap.add_argument("--dbz-poll-ms", type=int, default=500, help="Debezium poll.interval.ms (기본 500)")
    a = ap.parse_args()

    if a.data_dir:
        os.environ["EXP_DATA_DIR"] = a.data_dir
        C.DATA_DIR = Path(a.data_dir)
        C.RUNS_DIR = C.DATA_DIR / "runs"
    run_dir = C.RUNS_DIR / a.run_id
    if run_dir.exists() and any(run_dir.iterdir()):
        raise SystemExit(f"run dir already exists: {run_dir} (다른 --run-id 를 쓰거나 지운다)")
    run_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, PYTHONUTF8="1")
    st_cols = pq.read_schema(a.subset).names
    if "is_base" in st_cols:
        n_rows = sum(1 for b in pq.read_table(a.subset, columns=["is_base"]).column("is_base").to_pylist() if not b)
    else:
        n_rows = pq.read_table(a.subset, columns=["rev_id"]).num_rows
    if a.limit:
        n_rows = min(n_rows, a.limit)
    meta = {"run_id": a.run_id, "started_at": datetime.now(timezone.utc).isoformat(),
            "command": " ".join(sys.argv), "speed": a.speed, "embed_scale": a.embed_scale,
            "subset": a.subset, "embeddings": a.embeddings, "rows": n_rows, "workers": a.workers,
            "batch_interval": a.batch_interval, "batch_size": a.batch_size, "label": a.label}
    emeta = (str(Path(a.embeddings) / "meta.json") if Path(a.embeddings).is_dir()
             else str(a.embeddings).rsplit(".", 1)[0] + ".meta.json")
    if os.path.exists(emeta):
        meta["embed_meta"] = json.loads(open(emeta, encoding="utf-8").read())

    meta["host_minus_internet_s"] = host_clock_offset_s()
    print(f"[run] host clock - internet = {meta['host_minus_internet_s']}s", flush=True)
    if meta["host_minus_internet_s"] is not None and abs(meta["host_minus_internet_s"]) > 5 and not a.ignore_clock:
        raise SystemExit("호스트 시계가 5초 넘게 틀렸다. Windows 시간 동기화 후 다시 실행 (--ignore-clock 으로 무시)")

    st = register({"poll.interval.ms": a.dbz_poll_ms})
    meta["dbz_poll_ms"] = a.dbz_poll_ms
    print(f"[run] connector {st['connector']['state']}", flush=True)
    from .latency import measure_clock_offset
    meta.update(measure_clock_offset())
    print(f"[run] VM clock offset {meta['clock_offset_s']:+.3f}s (rtt {meta['rtt_s'] * 1000:.1f}ms)", flush=True)

    worker_log = open(run_dir / "worker.out", "w", encoding="utf-8")
    worker = subprocess.Popen(_py("pipeline.worker", "--run-id", a.run_id, "--embeddings", a.embeddings,
                                  "--embed-scale", str(a.embed_scale), "--workers", str(a.workers),
                                  "--batch-interval", str(a.batch_interval), "--batch-size", str(a.batch_size)),
                              stdout=worker_log, stderr=subprocess.STDOUT, env=env)
    url = f"http://127.0.0.1:{C.WORKER_PORT}"
    try:
        for _ in range(600):   # 벡터 적재·base 반영에 시간이 걸린다
            try:
                h = requests.get(f"{url}/health", timeout=2).json()
                if h.get("assigned"):
                    break
            except requests.RequestException:
                pass
            if worker.poll() is not None:
                raise SystemExit("worker exited early; see worker.out")
            time.sleep(0.5)
        else:
            raise SystemExit("worker never got a Kafka partition assignment")
        print("[run] worker ready", flush=True)
        time.sleep(1.0)  # 배정 직후 안정화

        eval_log = open(run_dir / "evaluate.out", "w", encoding="utf-8")
        evaluator = subprocess.Popen(_py("pipeline.evaluate", "--run-id", a.run_id, "--expected", str(n_rows),
                                         "--embeddings", a.embeddings, "--max-wait", str(a.max_wait)),
                                     stdout=eval_log, stderr=subprocess.STDOUT, env=env)
        time.sleep(1.0)
        t_rep = time.time()
        rep = subprocess.run(_py("pipeline.replayer", "--run-id", a.run_id, "--speed", str(a.speed),
                                 "--subset", a.subset, *(["--limit", str(a.limit)] if a.limit else [])),
                             env=env, capture_output=True, text=True, encoding="utf-8")
        (run_dir / "replayer.out").write_text(rep.stdout + rep.stderr, encoding="utf-8")
        print(rep.stdout.strip().splitlines()[-1] if rep.stdout.strip() else rep.stderr, flush=True)
        if rep.returncode != 0:
            raise SystemExit("replayer failed; see replayer.out")
        meta["replay_wall_s"] = time.time() - t_rep

        rc = evaluator.wait(timeout=a.max_wait + 60)
        eval_log.close()
        print((run_dir / "evaluate.out").read_text(encoding="utf-8").strip().splitlines()[-1], flush=True)
        if rc != 0:
            raise SystemExit("evaluator failed; see evaluate.out")
        meta["worker_health"] = requests.get(f"{url}/health", timeout=5).json()
        # T-05 기준: 리플레이 종료 후 모든 문서의 FAISS 리비전 = Postgres 최신 리비전
        import psycopg
        with psycopg.connect(C.PG_DSN, autocommit=True) as conn:
            pg_latest = dict(conn.execute("SELECT page_id, rev_id FROM wiki_latest WHERE run_id=%s", (a.run_id,)).fetchall())
        faiss_rev = {int(k): v for k, v in requests.get(f"{url}/state", timeout=10).json()["page_rev"].items()}
        match = sum(1 for p, r in pg_latest.items() if faiss_rev.get(p) == r)
        meta["final_consistency"] = {"pages": len(pg_latest), "faiss_matches_pg_latest": match}
        print(f"[run] final consistency: {match}/{len(pg_latest)} pages FAISS rev == Postgres latest", flush=True)
    finally:
        worker.terminate()
        try:
            worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            worker.kill()
        worker_log.close()

    meta["finished_at"] = datetime.now(timezone.utc).isoformat()
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    env["EXP_DATA_DIR"] = str(C.DATA_DIR)
    subprocess.run(_py("pipeline.plot", a.run_id, "--out", str(run_dir / "stale_curve.png")), env=env, check=True)
    subprocess.run(_py("pipeline.latency", a.run_id), env=env, check=True)


if __name__ == "__main__":
    main()
