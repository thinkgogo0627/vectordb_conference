"""실험 2 위키 (P3.1): HNSW 업데이트 처리 방식별 누적 업데이트 대비 recall@10·p95·구버전 혼입률·비용.

입력:   data/exp2/stream.npz (pipeline.exp2_data, 레이크 11/17까지. 초기 청크 579,468 + 청크 연산 89,317)
정답:   같은 체크포인트·같은 질의의 Flat 전수조사 (살아 있는 최신 노드 전체 대상, 내적)
질의:   체크포인트마다 hot 100 (방금 upsert된 청크의 최신 벡터, D-06 확장) + cold 100 (살아 있는 청크 무작위)
        + fixed 100 (한 번도 바뀌지 않는 청크, 모든 체크포인트에서 같은 질의 → 질의 표집 잡음 없이 추세를 본다)
인덱스: hnswlib, ip, M=16, ef_construction=200. 같은 rep의 세 방식은 같은 초기 그래프(init-<rep>.bin)에서 출발
지표:   ef마다 recall@10 (동점 허용: 정답 10등 점수 이상인 결과 수/10), 지연 p50·p95 (단일 스레드, 질의마다 5회 잰 중앙값),
        혼입률 (top-10 중 최신이 아닌 라벨 비율), 꺼진 노드 수·비율, 쓰기·삭제 횟수, 반영·재구축 시간

    python -m pipeline.run_exp2 --method tombstone --rep a
    python -m pipeline.run_exp2 --method rebuild --threshold 0.05 --rep a
"""
from __future__ import annotations

import argparse
import json
import platform
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from .exp2_data import OUT as DATA_DIR, load
from .hnsw_store import HnswUpdater

K = 10
EFS = (10, 20, 40, 80, 160)
NQ = 100
M, EFC = 16, 200


def exact_thresholds(vecs: np.ndarray, live_vrow: np.ndarray, Q: np.ndarray, k: int = K) -> np.ndarray:
    """살아 있는 노드 전체에 대한 정확한 k등 점수 (Flat 전수조사)."""
    best = np.full((len(Q), k), -np.inf, np.float32)
    for s in range(0, len(live_vrow), 100_000):
        sc = Q @ vecs[np.sort(live_vrow[s:s + 100_000])].T
        best = -np.partition(-np.concatenate([best, sc], axis=1), k - 1, axis=1)[:, :k]
    return best.min(axis=1)


def pick_queries(upd: HnswUpdater, recent: OrderedDict, cp: int, fixed: np.ndarray) -> dict[str, np.ndarray]:
    """질의 = 청크 라벨 배열. 세 방식에서 같은 체크포인트면 같은 청크가 뽑힌다 (살아 있는 집합이 같으므로)."""
    hot = []
    for cid in reversed(recent):
        lab = upd.chunk_lab.get(cid)
        if lab is not None:
            hot.append(cid)
        if len(hot) == NQ:
            break
    live_chunks = np.sort(np.fromiter(upd.chunk_lab.keys(), np.int64))
    cold = np.random.default_rng(1000 + cp).choice(live_chunks, NQ, replace=False)
    out = {"cold": np.array([upd.chunk_lab[c] for c in cold.tolist()]),
           "fixed": np.array([upd.chunk_lab[c] for c in fixed.tolist()])}
    if hot:
        out["hot"] = np.array([upd.chunk_lab[c] for c in hot])
    return out


def measure(upd: HnswUpdater, vecs: np.ndarray, qsets: dict, cp: int) -> dict:
    gt_dir = DATA_DIR / "gt"
    gt_dir.mkdir(exist_ok=True)
    live = upd.live_labels()
    res = {}
    for name, labs in qsets.items():
        Q = np.ascontiguousarray(vecs[upd.lab_vrow[labs]])
        f = gt_dir / f"cp{cp:03d}-{name}.npz"
        qchunks = upd.lab_chunk[labs]
        if f.exists() and np.array_equal(np.load(f)["qchunks"], qchunks):
            thr = np.load(f)["thr"]
        else:
            thr = exact_thresholds(vecs, upd.lab_vrow[live], Q)
            np.savez(f, qchunks=qchunks, thr=thr)
        res.update(score(upd, vecs, Q, thr, name)[0])
    return res


def score(upd: HnswUpdater, vecs: np.ndarray, Q: np.ndarray, thr: np.ndarray, name: str) -> tuple[dict, dict]:
    """ef마다 HNSW 검색 → recall@10(동점 허용)·혼입률·못 채운 결과 수·지연. 위키·커머스 공통 채점."""
    res, got_by_ef = {}, {}
    for ef in EFS:
        got, ms = upd.search(Q, K, ef)
        got_by_ef[ef] = got
        valid = got >= 0
        sc = np.einsum("qd,qkd->qk", Q, vecs[upd.lab_vrow[np.where(valid, got, 0)]])
        hits = ((sc >= thr[:, None] - 1e-5) & valid & upd.lab_live[np.where(valid, got, 0)]).sum(1)
        old = (valid & ~upd.lab_live[np.where(valid, got, 0)]).sum(1)
        res[f"{name}@ef{ef}"] = {
            "recall": round(float(np.minimum(hits, K).mean() / K), 5),
            "mix_rate": round(float(old.mean() / K), 5),
            "short": int((~valid).sum()),
            "p50_ms": round(float(np.percentile(ms, 50)), 4), "p95_ms": round(float(np.percentile(ms, 95)), 4),
        }
    return res, got_by_ef


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--method", required=True, choices=["tombstone", "reinsert", "rebuild"])
    ap.add_argument("--rep", default="a")
    ap.add_argument("--threshold", type=float, help="rebuild: 꺼진 노드 비율 기준")
    ap.add_argument("--ckpt-every", type=int, default=5000, help="청크 연산 수")
    ap.add_argument("--threads", type=int, default=16, help="초기 구축·재구축 스레드")
    a = ap.parse_args()
    if a.method == "rebuild" and a.threshold is None:
        raise SystemExit("--threshold 필요")

    vecs, s = load()
    vecs = np.asarray(vecs)    # 메모리에 올림 (2.6GB)
    run_id = f"{a.method}{'-' + format(a.threshold, 'g') if a.method == 'rebuild' else ''}-{a.rep}"
    run_dir = DATA_DIR / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    n_up = int((s["op_vrow"] >= 0).sum())
    upd = HnswUpdater(a.method, vecs, vecs.shape[1], len(s["init_chunk"]) + n_up + 16, M, EFC, a.threads, a.threshold)

    init_path = DATA_DIR / f"init-{a.rep}.bin"
    init_meta = DATA_DIR / f"init-{a.rep}.json"
    if init_path.exists():
        upd.build_initial(s["init_chunk"], s["init_vrow"], str(init_path))
    else:
        bt = upd.build_initial(s["init_chunk"], s["init_vrow"])
        upd.index.save_index(str(init_path))
        init_meta.write_text(json.dumps({"build_s": round(bt, 1), "n": len(s["init_chunk"]), "M": M, "efc": EFC,
                                         "threads": a.threads}), encoding="utf-8")
    build_s = json.loads(init_meta.read_text(encoding="utf-8"))["build_s"]

    op_edit, op_chunk, op_vrow = s["op_edit"], s["op_chunk"], s["op_vrow"]
    untouched = np.setdiff1d(s["init_chunk"], op_chunk)
    fixed = np.sort(np.random.default_rng(7).choice(untouched, NQ, replace=False))
    bounds = np.flatnonzero(np.diff(op_edit)) + 1
    starts, ends = np.r_[0, bounds], np.r_[bounds, len(op_edit)]
    recent: OrderedDict[int, None] = OrderedDict()
    log = open(run_dir / "metrics.jsonl", "w", encoding="utf-8")
    t_run = time.time()
    cp, next_cp, upserts = 0, 0, 0

    def checkpoint(ops_done: int, edits_done: int) -> None:
        nonlocal cp
        qs = pick_queries(upd, recent, cp, fixed)
        row = {"cp": cp, "ops": ops_done, "edits": edits_done, "upserts": upserts,
               "nodes": upd.n_nodes, "dead": upd.n_dead, "dead_ratio": round(upd.dead_ratio(), 5),
               "live": len(upd.chunk_lab), "cost": {k: round(v, 3) for k, v in upd.cost.items()},
               "wall_s": round(time.time() - t_run, 1), **measure(upd, vecs, qs, cp)}
        log.write(json.dumps(row) + "\n"); log.flush()
        c = row.get("hot@ef40", row["cold@ef40"])
        f = row["fixed@ef40"]
        print(f"[exp2 {run_id}] cp{cp:02d} ops {ops_done:6d} dead {row['dead_ratio']:.3f} "
              f"hot@40 r {c['recall']:.4f} p95 {c['p95_ms']:.3f}ms cold@40 r {row['cold@ef40']['recall']:.4f} "
              f"fixed@40 r {f['recall']:.4f} p50 {f['p50_ms']:.3f}ms mix {c['mix_rate']} rebuilds {upd.cost['rebuilds']}", flush=True)
        cp += 1

    checkpoint(0, 0)
    next_cp = a.ckpt_every
    for e, (i, j) in enumerate(zip(starts, ends)):
        upd.apply_edit(op_chunk[i:j], op_vrow[i:j])
        for cid, vr in zip(op_chunk[i:j].tolist(), op_vrow[i:j].tolist()):
            recent.pop(cid, None)
            if vr >= 0:
                recent[cid] = None
                upserts += 1
        while len(recent) > 5 * NQ:
            recent.popitem(last=False)
        if j >= next_cp or j == len(op_edit):
            checkpoint(int(j), e + 1)
            next_cp += a.ckpt_every
    log.close()

    meta = {"run_id": run_id, "method": a.method, "rep": a.rep, "threshold": a.threshold,
            "M": M, "ef_construction": EFC, "efs": EFS, "k": K, "nq": NQ, "ckpt_every": a.ckpt_every,
            "init_build_s": build_s, "init_nodes": int(len(s["init_chunk"])), "ops": int(len(op_edit)),
            "final": {"nodes": upd.n_nodes, "dead": upd.n_dead, "live": len(upd.chunk_lab), "cost": upd.cost},
            "wall_s": round(time.time() - t_run, 1), "hnswlib": "chroma-hnswlib 0.7.6",
            "host": platform.node(), "finished_at": datetime.now(timezone.utc).isoformat()}
    # 정합성: 인덱스의 살아 있는 라벨 = 우리가 아는 최신 노드
    ids = set(upd.index.get_ids_list())
    meta["final"]["index_ids_match_live"] = set(upd.chunk_lab.values()) <= ids
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[exp2 {run_id}] done wall {meta['wall_s']}s cost {upd.cost}")


if __name__ == "__main__":
    main()
