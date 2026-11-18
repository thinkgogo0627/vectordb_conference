"""실험 1 결과 정리: 조건별 stale 곡선, 구간별 지연 막대, 요약 표.

    python -m pipeline.summarize_exp1          # → docs/results/exp1/
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from .run_exp1 import CONDITIONS, DATA_DIR

OUT = Path("docs/results/exp1")
RUNS = DATA_DIR / "runs"
# 무효 run → 대체 run. 무효 기준: 리플레이가 예정 시각보다 0.1초 넘게 뒤처진 run (입력 자체가 달라짐, T-04 기준 위반)
REPLACED = {"x800-w2-b": "x800-w2-c"}   # b: 리플레이 26.9초 예정이 44.5초 걸림, 간격 오차 >100ms 83건

# dataviz 기본 팔레트 (validate_palette.js 통과: 범주 5슬롯 adjacent, 파랑 ordinal 5단계)
CAT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
ORD = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281"]
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"


def load_run(run_id: str) -> dict | None:
    d = RUNS / run_id
    if not (d / "meta.json").exists() or not (d / "edits.parquet").exists():
        return None
    edits = [e for e in pq.read_table(d / "edits.parquet").to_pylist() if e["final_status"] != "nochange"]
    sync = np.array([e["sync_elapsed"] if e["final_status"] == "normal" else np.inf for e in edits])
    lat = json.loads((d / "latency.json").read_text(encoding="utf-8")) if (d / "latency.json").exists() else {}
    meta = json.loads((d / "meta.json").read_text(encoding="utf-8"))
    return {"sync": sync, "lat": lat, "meta": meta}


def curve(sync: np.ndarray, grid: np.ndarray) -> np.ndarray:
    return np.array([(sync > x).mean() for x in grid])


def _style(ax, title):
    ax.set_facecolor(SURFACE)
    ax.grid(color=GRID, linewidth=0.8)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(INK2)
    ax.tick_params(colors=INK2, labelsize=9)
    ax.set_title(title, color=INK, fontsize=11, loc="left")


def plot_curves(plt, runs, names, colors, title, path, grid):
    fig, ax = plt.subplots(figsize=(8.5, 4.6), facecolor=SURFACE)
    for name, color in zip(names, colors):
        label = CONDITIONS[name][1]
        for rep, ls in (("a", "-"), ("b", "--")):
            r = runs.get(f"{name}-{rep}")
            if r is None:
                continue
            p50 = np.median(r["sync"][np.isfinite(r["sync"])])
            ax.step(grid, curve(r["sync"], grid), where="post", color=color, linestyle=ls, linewidth=2,
                    label=f"{label}  (p50 {p50:.2f}s)" if rep == "a" else None)
    ax.set_xscale("symlog", linthresh=1, linscale=0.6)
    ax.set_xlim(0, grid[-1])
    ax.set_ylim(0, 1.02)
    ax.set_xlabel("편집 후 경과 시간 (초, 1초 이후 로그 눈금)", color=INK2)
    ax.set_ylabel("아직 stale인 편집 비율", color=INK2)
    _style(ax, title)
    leg = ax.legend(fontsize=8.5, frameon=False, title="실선 1회차, 점선 2회차", title_fontsize=8)
    for t in leg.get_texts():
        t.set_color(INK)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


STAGES = [  # (표시 이름, latency.json 키들)
    ("Postgres 커밋 → Debezium", ["insert_to_pg_commit", "pg_commit_to_debezium"]),
    ("Debezium → Kafka → 워커", ["debezium_to_kafka", "kafka_to_consumed"]),
    ("대기열·묶음 대기", ["queue_or_batch_wait"]),
    ("임베딩 대기 (주입값)", ["embed_wait"]),
    ("FAISS 반영", ["faiss_apply"]),
]


def stage_means(runs, name) -> list[float] | None:
    vals = []
    for rep in ("a", "b"):
        r = runs.get(f"{name}-{rep}")
        if r and r["lat"]:
            vals.append([max(0.0, sum(r["lat"][k]["mean_ms"] for k in keys if k in r["lat"])) for _, keys in STAGES])
    return list(np.mean(vals, axis=0)) if vals else None


def plot_stages(plt, runs, names, path, title):
    rows = [(n, stage_means(runs, n)) for n in names]
    rows = [(n, v) for n, v in rows if v is not None]
    fig, ax = plt.subplots(figsize=(9, 0.42 * len(rows) + 1.6), facecolor=SURFACE)
    for i, (n, v) in enumerate(rows):
        left = 0.0
        for j, w in enumerate(v):
            ax.barh(i, w, left=left, color=CAT[j], height=0.62, edgecolor=SURFACE, linewidth=2,
                    label=STAGES[j][0] if i == 0 else None)
            left += w
        ax.text(left * 1.01 + 5, i, f"{left / 1000:.2f}s", va="center", fontsize=8, color=INK2)
    ax.set_yticks(range(len(rows)), [CONDITIONS[n][1] for n, _ in rows], fontsize=8.5, color=INK)
    ax.invert_yaxis()
    ax.set_xlabel("편집 하나가 반영되기까지 평균 시간 (ms), 구간별", color=INK2)
    _style(ax, title)
    ax.grid(axis="y", visible=False)
    leg = ax.legend(fontsize=8, frameon=False, ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.14))
    for t in leg.get_texts():
        t.set_color(INK)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def main() -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams["font.family"] = ["Malgun Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    OUT.mkdir(parents=True, exist_ok=True)
    runs = {}
    for name in CONDITIONS:
        for rep in ("a", "b"):
            rid = f"{name}-{rep}"
            r = load_run(REPLACED.get(rid, rid))
            if r is not None:
                runs[f"{name}-{rep}"] = r
    finite_max = max(r["sync"][np.isfinite(r["sync"])].max() for r in runs.values())
    grid = np.concatenate([np.arange(0, 1, 0.01), np.arange(1, finite_max * 1.1 + 1, 0.05)])

    plot_curves(plt, runs, ["x10", "x100", "x200", "x400", "x800"], ORD,
                "실험 1: 배속(유입률)별 stale 곡선", OUT / "fig1_speed.png", grid)
    plot_curves(plt, runs, ["x100", "x100-int1", "x100-int5", "x100-n10", "x100-n50"], CAT,
                "실험 1: 반영 주기 (×100)", OUT / "fig2_batch_x100.png", grid)
    plot_curves(plt, runs, ["x800", "x800-int1", "x800-n50", "x800-w2", "x800-w4"], CAT,
                "실험 1: 포화 상태(×800)에서 묶음 반영·워커 수", OUT / "fig3_saturated_x800.png", grid)
    plot_curves(plt, runs, ["x100", "x100-e0", "x100-dbz50", "x800", "x800-e0"], CAT,
                "실험 1: 임베딩 대기 제거·Debezium 폴링 간격", OUT / "fig4_embed_cdc.png", grid)
    fast = ["x10", "x100", "x200", "x100-e0", "x800-e0", "x100-dbz50", "x800-w4"]
    plot_stages(plt, runs, fast, OUT / "fig5a_stages_fast.png", "실험 1: 구간별 지연, 대기열이 거의 없는 조건 (1·2회차 평균)")
    plot_stages(plt, runs, [n for n in CONDITIONS if n not in fast], OUT / "fig5b_stages_queued.png",
                "실험 1: 구간별 지연, 대기열·묶음 대기가 생기는 조건 (1·2회차 평균)")

    # 요약 표
    fields = ["condition", "axis", "label", "rep", "edits", "unsynced", "p50_s", "p90_s", "p99_s", "max_s",
              "stale_at_1s", "stale_at_5s", "stale_at_10s", "final_consistency", "host_clock_off_s"] + \
             [f"mean_ms:{s}" for s, _ in STAGES]
    rows = []
    for name, (axis, label, _kw) in CONDITIONS.items():
        for rep in ("a", "b"):
            r = runs.get(f"{name}-{rep}")
            if r is None:
                continue
            s = r["sync"]
            f = s[np.isfinite(s)]
            fc = r["meta"].get("final_consistency", {})
            row = {"condition": name, "axis": axis, "label": label, "rep": rep, "edits": len(s),
                   "unsynced": int((~np.isfinite(s)).sum()),
                   "p50_s": round(float(np.median(f)), 3), "p90_s": round(float(np.percentile(f, 90)), 3),
                   "p99_s": round(float(np.percentile(f, 99)), 3), "max_s": round(float(f.max()), 3),
                   "stale_at_1s": round(float((s > 1).mean()), 3), "stale_at_5s": round(float((s > 5).mean()), 3),
                   "stale_at_10s": round(float((s > 10).mean()), 3),
                   "final_consistency": f"{fc.get('faiss_matches_pg_latest')}/{fc.get('pages')}",
                   "host_clock_off_s": r["meta"].get("host_minus_internet_s")}
            for (sn, keys) in STAGES:
                row[f"mean_ms:{sn}"] = round(sum(r["lat"][k]["mean_ms"] for k in keys if k in r["lat"]), 1) \
                    if r["lat"] else None
            rows.append(row)
    with open(OUT / "summary.csv", "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    # 재현성: 같은 조건 1·2회차 곡선 최대 차, p50·p90 차
    rep_rows = []
    for name in CONDITIONS:
        a, b = runs.get(f"{name}-a"), runs.get(f"{name}-b")
        if not (a and b):
            continue
        fa, fb = a["sync"][np.isfinite(a["sync"])], b["sync"][np.isfinite(b["sync"])]
        rep_rows.append({"condition": name,
                         "max_curve_diff": round(float(np.max(np.abs(curve(a["sync"], grid) - curve(b["sync"], grid)))), 3),
                         "p50_diff_s": round(float(abs(np.median(fa) - np.median(fb))), 3),
                         "p90_diff_s": round(float(abs(np.percentile(fa, 90) - np.percentile(fb, 90))), 3)})
    (OUT / "reproducibility.json").write_text(json.dumps(rep_rows, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"runs={len(runs)} → {OUT}")
    for r in rows:
        print(f"{r['condition']:12s} {r['rep']} p50 {r['p50_s']:7.2f} p90 {r['p90_s']:7.2f} max {r['max_s']:7.2f} "
              f"stale@1s {r['stale_at_1s']:.2f} @5s {r['stale_at_5s']:.2f} unsynced {r['unsynced']} "
              f"cons {r['final_consistency']}")
    for r in rep_rows:
        print("repro", r)


if __name__ == "__main__":
    main()
