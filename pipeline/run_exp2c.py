"""실험 2 커머스 (P3.2): HNSW 업데이트 처리 방식별 누적 상태 전환 대비 recall@10·p95·품절 혼입률·비용.

입력:   data/exp2c/stream.npz (pipeline.exp2c_data. item2vec 64차원, 주간 스냅샷 18개)
정답:   같은 체크포인트·같은 질의의 Flat 전수조사 (그 시점 판매 중인 상품 전체, 내적)
질의:   week 100 (그 주 조회 이벤트, 방문자의 최근 본 상품 5개 평균) + fixed 100 (첫 주 질의를 매번 그대로)
인덱스·채점: 위키(run_exp2)와 같다. hnswlib ip, M=16, ef_construction=200, ef 10~160, 질의마다 5회 잰 지연 중앙값.
체크포인트: 스냅샷마다 (그 주 전환을 모두 반영한 뒤). 재구축은 --threshold 0이면 매주.

    python -m pipeline.run_exp2c --method tombstone --rep a
    python -m pipeline.run_exp2c --method rebuild --threshold 0 --rep a     # 매주 재구축
"""
from __future__ import annotations

import argparse
import json
import platform
import time
from datetime import datetime, timezone

import numpy as np

from .exp2c_data import OUT as DATA_DIR, load
from .hnsw_store import HnswUpdater
from .run_exp2 import EFC, EFS, M, exact_thresholds, score


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--method", required=True, choices=["tombstone", "reinsert", "rebuild"])
    ap.add_argument("--rep", default="a")
    ap.add_argument("--threshold", type=float, help="rebuild: 꺼진 노드 비율 기준 (0 = 매주)")
    ap.add_argument("--threads", type=int, default=16, help="초기 구축·재구축 스레드")
    a = ap.parse_args()
    if a.method == "rebuild" and a.threshold is None:
        raise SystemExit("--threshold 필요")

    vecs, s = load()
    run_id = f"{a.method}{'-' + format(a.threshold, 'g') if a.method == 'rebuild' else ''}-{a.rep}"
    run_dir = DATA_DIR / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    gt_dir = DATA_DIR / "gt"
    gt_dir.mkdir(exist_ok=True)
    S, v5 = s["state"], s["views5"]
    op_snap, op_item, op_on = s["op_snap"], s["op_item"], s["op_on"]
    upd = HnswUpdater(a.method, vecs, vecs.shape[1], len(s["init_item"]) + int(op_on.sum()) + 16, M, EFC,
                      a.threads, a.threshold)

    init_path, init_meta = DATA_DIR / f"init-{a.rep}.bin", DATA_DIR / f"init-{a.rep}.json"
    init = s["init_item"]
    if init_path.exists():
        upd.build_initial(init, init, str(init_path))
    else:
        bt = upd.build_initial(init, init)
        upd.index.save_index(str(init_path))
        init_meta.write_text(json.dumps({"build_s": round(bt, 2), "n": len(init), "M": M, "efc": EFC,
                                         "threads": a.threads}), encoding="utf-8")
    build_s = json.loads(init_meta.read_text(encoding="utf-8"))["build_s"]

    log = open(run_dir / "metrics.jsonl", "w", encoding="utf-8")
    t_run = time.time()
    live_ok = True

    def checkpoint(k: int, ops_done: int) -> None:
        nonlocal live_ok
        live = upd.live_labels()
        expect = set(np.flatnonzero(S[:, k] == 1).tolist())
        ok = set(upd.chunk_lab) == expect
        live_ok &= ok
        row = {"cp": k, "snap_ts": int(s["snap_ts"][k]), "ops": ops_done, "nodes": upd.n_nodes, "dead": upd.n_dead,
               "dead_ratio": round(upd.dead_ratio(), 5), "live": len(live), "live_v5": int(v5[upd.lab_chunk[live]].sum()),
               "live_ok": ok, "cost": {key: round(val, 3) for key, val in upd.cost.items()},
               "wall_s": round(time.time() - t_run, 1)}
        for name, Q in (("week", s["q"][k]), ("fixed", s["q"][0])):
            Q = np.ascontiguousarray(Q)
            f = gt_dir / f"cp{k:02d}-{name}.npz"
            if f.exists() and np.array_equal(np.load(f)["q"], Q):
                thr = np.load(f)["thr"]
            else:
                thr = exact_thresholds(vecs, upd.lab_vrow[live], Q)
                np.savez(f, q=Q, thr=thr)
            res, got = score(upd, vecs, Q, thr, name)
            for ef, g in got.items():           # 결과 중 조회 5회 이상 상품 비율 (드문 상품 벡터 잡음 표시)
                ok_ = g >= 0
                res[f"{name}@ef{ef}"]["v5_share"] = round(float(v5[upd.lab_chunk[g[ok_]]].mean()), 4) if ok_.any() else None
            row.update(res)
        log.write(json.dumps(row) + "\n"); log.flush()
        w, f_ = row["week@ef40"], row["fixed@ef40"]
        print(f"[exp2c {run_id}] cp{k:02d} ops {ops_done:6d} live {row['live']:6d} nodes {upd.n_nodes:6d} "
              f"dead {row['dead_ratio']:.3f} week@40 r {w['recall']:.4f} p95 {w['p95_ms']:.3f}ms "
              f"fixed@40 r {f_['recall']:.4f} p95 {f_['p95_ms']:.3f}ms short {w['short'] + f_['short']} "
              f"rebuilds {upd.cost['rebuilds']} ok {ok}", flush=True)

    checkpoint(0, 0)
    done = 0
    for k in range(1, len(s["snap_ts"])):
        sel = np.flatnonzero(op_snap == k)
        upd.apply_flips(op_item[sel], op_item[sel], op_on[sel])
        done += len(sel)
        checkpoint(k, done)
    log.close()

    meta = {"run_id": run_id, "method": a.method, "rep": a.rep, "threshold": a.threshold, "M": M,
            "ef_construction": EFC, "efs": EFS, "init_build_s": build_s, "init_nodes": int(len(init)),
            "ops": int(len(op_item)), "final": {"nodes": upd.n_nodes, "dead": upd.n_dead, "live": len(upd.chunk_lab),
                                                "cost": upd.cost, "live_matches_state_all_cp": live_ok},
            "wall_s": round(time.time() - t_run, 1), "hnswlib": "chroma-hnswlib 0.7.6", "host": platform.node(),
            "finished_at": datetime.now(timezone.utc).isoformat()}
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[exp2c {run_id}] done wall {meta['wall_s']}s cost {upd.cost} live_ok {live_ok}")


if __name__ == "__main__":
    main()
