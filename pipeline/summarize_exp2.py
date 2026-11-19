"""실험 2 결과 정리: 누적 업데이트 대비 recall@10·p95·꺼진 노드 비율, 비용, ef 트레이드오프.

    python -m pipeline.summarize_exp2                      # 위키 → docs/results/exp2/
    python -m pipeline.summarize_exp2 --dataset commerce   # 커머스 → docs/results/exp2c/
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np

from .summarize_exp1 import CAT, GRID, INK, INK2, SURFACE, _style

MAIN_EF = 40
LABELS = {"tombstone": "꺼두기", "reinsert": "지웠다 다시 넣기"}
DATASETS = {
    "wiki": {"runs": Path("data/exp2/runs"), "out": Path("docs/results/exp2"), "title": "실험 2 위키",
             "qsets": {"hot": "방금 편집된 청크", "cold": "무작위 청크", "fixed": "고정 청크 100개"},
             "xlabel": "누적 청크 업데이트 수 (교체 + 삭제)"},
    "commerce": {"runs": Path("data/exp2c/runs"), "out": Path("docs/results/exp2c"), "title": "실험 2 커머스",
                 "qsets": {"week": "그 주 조회 이벤트", "fixed": "첫 주 조회 이벤트 100개 고정"},
                 "xlabel": "누적 상품 상태 전환 수 (품절 + 판매 재개)"},
}
COST_KEYS = ("adds", "marks", "replaces", "unmarks", "apply_s", "rebuilds", "rebuild_s")


def label(run_key: str) -> str:
    if run_key.startswith("rebuild-"):
        thr = float(run_key.split("-")[1])
        return "재구축 (매주)" if thr == 0 else f"재구축 (꺼진 노드 {thr * 100:g}%마다)"
    return LABELS[run_key]


def load_runs(runs_dir: Path) -> dict[str, dict[str, list[dict]]]:
    """{조건: {rep: [체크포인트 행]}}"""
    out: dict[str, dict[str, list[dict]]] = {}
    for d in sorted(runs_dir.iterdir()):
        if not (d / "meta.json").exists():
            continue
        key, rep = d.name.rsplit("-", 1)
        out.setdefault(key, {})[rep] = [json.loads(l) for l in (d / "metrics.jsonl").read_text(encoding="utf-8").splitlines()]
    order = ["tombstone", "reinsert"] + sorted(k for k in out if k.startswith("rebuild-"))
    return {k: out[k] for k in order if k in out}


def plot_metric(plt, runs, qset, field, ylabel, title, path, xlabel, ef=MAIN_EF, scale=1.0):
    fig, ax = plt.subplots(figsize=(8.5, 4.6), facecolor=SURFACE)
    lo = 1.0
    for (key, reps), color in zip(runs.items(), CAT):
        for rep, ls in (("a", "-"), ("b", "--")):
            rows = reps.get(rep)
            if not rows:
                continue
            pts = [(r["ops"], r[f"{qset}@ef{ef}"][field] * scale) for r in rows if f"{qset}@ef{ef}" in r]
            x, y = zip(*pts)
            lo = min(lo, min(y))
            ax.plot(x, y, color=color, linestyle=ls, linewidth=2, marker="o" if rep == "a" else None, markersize=3,
                    label=label(key) if rep == "a" else None)
    ax.set_xlabel(xlabel, color=INK2)
    ax.set_ylabel(ylabel, color=INK2)
    if field == "recall":
        ax.set_ylim(min(0.90, np.floor(lo * 20) / 20), 1.0)   # 작은 흔들림이 큰 차이처럼 보이지 않게 0.90 이하부터
    else:
        ax.set_ylim(0, None)
    _style(ax, title)
    leg = ax.legend(fontsize=8.5, frameon=False, title="실선 1회차, 점선 2회차", title_fontsize=8)
    for t in leg.get_texts():
        t.set_color(INK)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def plot_dead(plt, runs, path, xlabel, title):
    fig, ax = plt.subplots(figsize=(8.5, 4.2), facecolor=SURFACE)
    for (key, reps), color in zip(runs.items(), CAT):
        rows = reps.get("a")
        if rows:
            ax.plot([r["ops"] for r in rows], [r["dead_ratio"] * 100 for r in rows], color=color, linewidth=2,
                    marker="o", markersize=3, label=label(key))
    ax.set_xlabel(xlabel, color=INK2)
    ax.set_ylabel("그래프 안 꺼진 노드 비율 (%)", color=INK2)
    _style(ax, f"{title}: 그래프에 남은 꺼진 노드 (1회차)")
    leg = ax.legend(fontsize=8.5, frameon=False)
    for t in leg.get_texts():
        t.set_color(INK)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def plot_ef_tradeoff(plt, runs, qset, qname, path, title):
    fig, ax = plt.subplots(figsize=(8.5, 4.6), facecolor=SURFACE)
    xs_all = []
    for (key, reps), color in zip(runs.items(), CAT):
        rows = reps.get("a")
        if not rows:
            continue
        for row, ls, alpha, tag in ((rows[0], ":", 0.6, "시작"), (rows[-1], "-", 1.0, "끝")):
            efs = sorted(int(k.split("@ef")[1]) for k in row if k.startswith(f"{qset}@ef"))
            xs = [row[f"{qset}@ef{e}"]["p95_ms"] for e in efs]
            ys = [row[f"{qset}@ef{e}"]["recall"] for e in efs]
            xs_all += xs
            ax.plot(xs, ys, color=color, linestyle=ls, linewidth=2, marker="o", markersize=4, alpha=alpha,
                    label=f"{label(key)}, {tag}" if (tag == "끝" or key == "tombstone") else None)
    ax.set_xscale("log")
    from matplotlib.ticker import FuncFormatter, NullFormatter
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.xaxis.set_minor_formatter(NullFormatter())
    cand = [0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5]
    lo, hi = min(xs_all), max(xs_all)
    ax.set_xticks([c for c in cand if lo / 2 <= c <= hi * 2])
    ax.set_xlabel("검색 지연 p95 (ms, 로그), 점은 ef 10·20·40·80·160", color=INK2)
    ax.set_ylabel("recall@10", color=INK2)
    _style(ax, f"{title}: 탐색 폭(ef)별 recall–지연, {qname} 질의 (1회차, 점선=업데이트 전)")
    leg = ax.legend(fontsize=8, frameon=False)
    for t in leg.get_texts():
        t.set_color(INK)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", choices=list(DATASETS), default="wiki")
    cfg = DATASETS[ap.parse_args().dataset]
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams["font.family"] = ["Malgun Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    out, title, qsets, xlabel = cfg["out"], cfg["title"], cfg["qsets"], cfg["xlabel"]
    out.mkdir(parents=True, exist_ok=True)
    runs = load_runs(cfg["runs"])

    for q, qname in qsets.items():
        plot_metric(plt, runs, q, "recall", f"recall@10 (ef={MAIN_EF})",
                    f"{title}: recall@10, {qname} 질의", out / f"fig1_recall_{q}.png", xlabel)
        plot_metric(plt, runs, q, "p95_ms", f"검색 지연 p95 (ms, ef={MAIN_EF})",
                    f"{title}: 검색 지연 p95, {qname} 질의", out / f"fig2_p95_{q}.png", xlabel)
        plot_ef_tradeoff(plt, runs, q, qname, out / f"fig4_ef_tradeoff_{q}.png", title)
    plot_dead(plt, runs, out / "fig3_dead_nodes.png", xlabel, title)

    has_unmarks = any("unmarks" in r["cost"] for reps in runs.values() for cps in reps.values() for r in cps)
    cost_keys = [k for k in COST_KEYS if k != "unmarks" or has_unmarks]
    fields = ["condition", "rep", "ops", "dead_ratio", "nodes", "live"] + cost_keys
    metric_fields = ("recall", "p95_ms", "mix_rate") + (("short", "v5_share") if has_unmarks else ())
    for q in qsets:
        for ef in (10, MAIN_EF, 160):
            fields += [f"{q}@ef{ef}:{f}" for f in metric_fields]
    rows = []
    for key, reps in runs.items():
        for rep, cps in sorted(reps.items()):
            for r in (cps[0], cps[-1]):
                row = {"condition": key, "rep": rep, "ops": r["ops"], "dead_ratio": r["dead_ratio"],
                       "nodes": r["nodes"], "live": r["live"], **{k: r["cost"].get(k, 0) for k in cost_keys}}
                for q in qsets:
                    for ef in (10, MAIN_EF, 160):
                        m = r.get(f"{q}@ef{ef}")
                        for f in metric_fields:
                            row[f"{q}@ef{ef}:{f}"] = m.get(f) if m else None
                rows.append(row)
    with open(out / "summary.csv", "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    rep_rows = []
    for key, reps in runs.items():
        if "a" in reps and "b" in reps:
            for q in qsets:
                k = f"{q}@ef{MAIN_EF}"
                da = [abs(x[k]["recall"] - y[k]["recall"]) for x, y in zip(reps["a"], reps["b"]) if k in x and k in y]
                dp = [abs(x[k]["p95_ms"] - y[k]["p95_ms"]) for x, y in zip(reps["a"], reps["b"]) if k in x and k in y]
                rep_rows.append({"condition": key, "qset": q, "max_recall_diff": round(max(da), 4),
                                 "max_p95_diff_ms": round(max(dp), 4)})
    (out / "reproducibility.json").write_text(json.dumps(rep_rows, indent=2, ensure_ascii=False), encoding="utf-8")
    show = ["condition", "rep", "ops", "dead_ratio"] + [f"{q}@ef{MAIN_EF}:{f}" for q in qsets for f in ("recall", "p95_ms")] \
        + ["rebuilds", "rebuild_s", "apply_s"]
    for r in rows:
        print({k: r[k] for k in show})
    for r in rep_rows:
        print("repro", r)


if __name__ == "__main__":
    main()
