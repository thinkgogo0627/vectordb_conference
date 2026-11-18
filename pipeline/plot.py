"""T-07: stale 곡선. x = 편집 후 경과 시간(초), y = 아직 stale(또는 누락)인 편집 비율.

    python -m pipeline.plot A [B ...]      # 여러 run을 겹쳐 그린다 → data/exp0/exp0_stale_curve.png
"""
from __future__ import annotations

import argparse
import json
import sys

import numpy as np
import pyarrow.parquet as pq

from . import config as C


def load_sync(run_id: str) -> tuple[np.ndarray, dict]:
    t = pq.read_table(C.RUNS_DIR / run_id / "edits.parquet").to_pylist()
    tracked = [e for e in t if e["final_status"] != "nochange"]
    sync = np.array([e["sync_elapsed"] if e["final_status"] == "normal" else np.inf for e in tracked])
    finite = sync[np.isfinite(sync)]
    summary = {
        "run_id": run_id, "edits": len(t), "tracked": len(tracked),
        "never_synced": int(np.sum(~np.isfinite(sync))),
        "sync_s": {"p50": float(np.median(finite)), "p90": float(np.percentile(finite, 90)),
                   "p99": float(np.percentile(finite, 99)), "max": float(finite.max())} if len(finite) else None,
    }
    for s in ("normal", "nochange"):
        summary[s] = sum(1 for e in t if e["final_status"] == s)
    summary["timeouts"] = sum(1 for e in t if str(e["final_status"]).startswith("timeout"))
    return sync, summary


def stale_curve(sync: np.ndarray, grid: np.ndarray) -> np.ndarray:
    return np.array([(sync > x).mean() for x in grid])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("runs", nargs="+")
    ap.add_argument("--out", default=str(C.DATA_DIR / "exp0_stale_curve.png"))
    ap.add_argument("--step", type=float, default=0.05)
    ap.add_argument("--title", default="실험 0: 원본 DB(Postgres) ↔ FAISS 간극")
    ap.add_argument("--logx", action="store_true")
    a = ap.parse_args()

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams["font.family"] = ["Malgun Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    data = {r: load_sync(r) for r in a.runs}
    xmax = max(s[np.isfinite(s)].max() for s, _ in data.values()) * 1.1 + a.step
    grid = np.arange(0, xmax, a.step)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    curves = {}
    for r, (sync, summ) in data.items():
        y = stale_curve(sync, grid)
        curves[r] = y
        meta_p = C.RUNS_DIR / r / "meta.json"
        meta = json.loads(meta_p.read_text(encoding="utf-8")) if meta_p.exists() else {}
        label = f"run {r} (n={summ['tracked']}, p50={summ['sync_s']['p50']:.2f}s, p90={summ['sync_s']['p90']:.2f}s)"
        if meta.get("label"):
            label = f"{meta['label']} [{r}] (p50={summ['sync_s']['p50']:.2f}s, p90={summ['sync_s']['p90']:.2f}s)"
        elif meta.get("speed") is not None:
            label += f" ×{meta['speed']}"
        ax.step(grid, y, where="post", label=label)
    ax.set_xlabel("편집 후 경과 시간 (초)")
    ax.set_ylabel("아직 stale인 편집 비율")
    ax.set_ylim(0, 1.02)
    ax.set_xlim(0, xmax)
    ax.grid(alpha=0.3)
    ax.set_title(a.title)
    if a.logx:
        ax.set_xscale("symlog", linthresh=1)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(a.out, dpi=150)

    # 곡선 CSV + 요약
    with open(str(a.out).rsplit(".", 1)[0] + ".csv", "w", encoding="utf-8") as f:
        f.write("elapsed_s," + ",".join(a.runs) + "\n")
        for i, x in enumerate(grid):
            f.write(f"{x:.3f}," + ",".join(f"{curves[r][i]:.4f}" for r in a.runs) + "\n")
    out = {r: s for r, (_, s) in data.items()}
    if len(a.runs) >= 2:
        ys = list(curves.values())
        out["reproducibility"] = {
            "max_abs_curve_diff": float(np.max(np.abs(ys[0] - ys[1]))),
            "p50_diff_s": float(abs(out[a.runs[0]]["sync_s"]["p50"] - out[a.runs[1]]["sync_s"]["p50"])),
            "p90_diff_s": float(abs(out[a.runs[0]]["sync_s"]["p90"] - out[a.runs[1]]["sync_s"]["p90"])),
        }
    json.dump(out, sys.stdout, indent=2, ensure_ascii=False)
    print(f"\nsaved {a.out}")


if __name__ == "__main__":
    main()
