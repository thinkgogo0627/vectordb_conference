"""실험 1 (P2, 시간 간극): 조건표 전체를 같은 리플레이 입력으로 2회씩 실행한다 (GR-01).

리플레이 입력: data/exp1/subset.parquet (kowiki 2026-11-16 00:00~06:00 UTC, 편집 1,149건 + base 801개)
임베딩:        data/exp1/emb (bge-m3 fp16, RTX 3060 Laptop에서 실측한 embed_ms. 사용자 결정: GPU 기준)
기준 조건:     ×100, 즉시 반영, 워커 1, embed_ms ×1, Debezium poll 500ms

순서: 모든 조건의 1회차(a)를 먼저, 그다음 2회차(b). 시간에 따른 환경 변화가 한 조건에 몰리지 않게 한다.
이미 끝난 run(meta.json 있음)은 건너뛴다 → 중단 후 같은 명령으로 이어서 돌릴 수 있다.

    python -m pipeline.run_exp1                 # 전체
    python -m pipeline.run_exp1 --only x100     # 일부 조건
    python -m pipeline.run_exp1 --list
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path("data/exp1")
SUBSET = DATA_DIR / "subset.parquet"
EMB = DATA_DIR / "emb"

# 이름: (축, 범례, 인자)
CONDITIONS: dict[str, tuple[str, str, dict]] = {
    "x10":          ("speed",   "×10",                         {"speed": 10}),
    "x100":         ("base",    "×100 (기준)",                 {"speed": 100}),
    "x200":         ("speed",   "×200",                        {"speed": 200}),
    "x400":         ("speed",   "×400",                        {"speed": 400}),
    "x800":         ("speed",   "×800",                        {"speed": 800}),
    "x100-e0":      ("embed",   "×100, embed 대기 0",          {"speed": 100, "embed_scale": 0}),
    "x800-e0":      ("embed",   "×800, embed 대기 0",          {"speed": 800, "embed_scale": 0}),
    "x100-dbz50":   ("cdc",     "×100, Debezium poll 50ms",    {"speed": 100, "dbz_poll_ms": 50}),
    "x100-int1":    ("batch",   "×100, 1초마다 묶음",          {"speed": 100, "batch_interval": 1}),
    "x100-int5":    ("batch",   "×100, 5초마다 묶음",          {"speed": 100, "batch_interval": 5}),
    "x100-n10":     ("batch",   "×100, 10건마다 묶음",         {"speed": 100, "batch_size": 10}),
    "x100-n50":     ("batch",   "×100, 50건마다 묶음",         {"speed": 100, "batch_size": 50}),
    "x800-int1":    ("batch",   "×800, 1초마다 묶음",          {"speed": 800, "batch_interval": 1}),
    "x800-n50":     ("batch",   "×800, 50건마다 묶음",         {"speed": 800, "batch_size": 50}),
    "x800-w2":      ("workers", "×800, 워커 2",                {"speed": 800, "workers": 2}),
    "x800-w4":      ("workers", "×800, 워커 4",                {"speed": 800, "workers": 4}),
}
REPS = ("a", "b")


def run_one(name: str, rep: str, max_wait: float) -> dict:
    axis, label, kw = CONDITIONS[name]
    run_id = f"{name}-{rep}"
    run_dir = DATA_DIR / "runs" / run_id
    if (run_dir / "meta.json").exists():
        return {"run_id": run_id, "status": "skipped (done)"}
    if run_dir.exists():   # 중간에 실패한 run은 지우고 다시
        import shutil
        shutil.rmtree(run_dir)
    args = [sys.executable, "-m", "pipeline.run_exp0", "--data-dir", str(DATA_DIR), "--subset", str(SUBSET),
            "--embeddings", str(EMB), "--run-id", run_id, "--max-wait", str(max_wait), "--label", label,
            "--speed", str(kw["speed"])]
    for k in ("embed_scale", "workers", "batch_interval", "batch_size", "dbz_poll_ms"):
        if k in kw:
            args += [f"--{k.replace('_', '-')}", str(kw[k])]
    t = time.time()
    p = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", env=dict(os.environ, PYTHONUTF8="1"))
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "runner.out").write_text(p.stdout + p.stderr, encoding="utf-8")
    lines = [l for l in p.stdout.splitlines() if l.startswith(("[evaluate] done", "[run] final"))]
    return {"run_id": run_id, "status": "ok" if p.returncode == 0 else f"failed rc={p.returncode}",
            "wall_s": round(time.time() - t), "summary": lines}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", help="조건 이름 일부만")
    ap.add_argument("--reps", nargs="*", default=list(REPS))
    ap.add_argument("--max-wait", type=float, default=600)
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()
    if a.list:
        for k, (axis, label, kw) in CONDITIONS.items():
            print(f"{k:12s} {axis:8s} {label:28s} {kw}")
        return
    names = a.only or list(CONDITIONS)
    log = DATA_DIR / "run_exp1.log.jsonl"
    for rep in a.reps:
        for name in names:
            print(f"[exp1] {datetime.now(timezone.utc).isoformat(timespec='seconds')} start {name}-{rep}", flush=True)
            res = run_one(name, rep, a.max_wait)
            print(f"[exp1] {res}", flush=True)
            with open(log, "a", encoding="utf-8") as f:
                f.write(json.dumps(dict(res, at=datetime.now(timezone.utc).isoformat()), ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
