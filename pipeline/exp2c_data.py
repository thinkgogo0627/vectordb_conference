"""실험 2 커머스(P3.2) 입력: Retailrocket → item2vec 상품 벡터 + 주별 품절·판매 재개 스트림 + 조회 이벤트 질의.

- 상품 벡터: 조회(view) 이벤트를 방문자별로 시간순 정렬하고 30분 넘게 쉬면 세션을 끊는다.
  세션을 문장, 상품을 단어로 보고 gensim Word2Vec(skip-gram, negative sampling)으로 학습한다 (item2vec).
  min_count=2 (조회 2회 이상인 상품만 벡터가 생김). 한 번 만들고 고정한다.
- 카탈로그: 벡터가 있고 available 기록이 한 번이라도 있는 상품. 기록이 없는 상품은 뺀다 (사용자 결정, 2026-11-19).
  조회 5회 이상 상품은 views5 플래그로 따로 표시한다 (사용자 결정).
- 상태: item_properties의 available은 일요일 03:00 UTC 주간 스냅샷 18개다. 스냅샷 사이 값이 없으면 직전 값을 잇는다.
  - 초기 인덱스: 첫 스냅샷에서 판매 중(available=1)인 상품
  - 스냅샷 k의 연산: 판매 중 → 품절 = off, 품절 → 판매 중 = on(재입고), 처음 나타나며 판매 중 = on(신규).
    처음 나타나며 품절인 상품은 연산 없음 (카탈로그에만 생김). 같은 스냅샷 안에서는 off 먼저, 상품 번호 순.
- 질의: 스냅샷 k가 유효한 한 주 [k, k+1) 안의 조회 이벤트 100개 (시드 2000+k).
  질의 벡터 = 그 방문자가 그 시점까지 본 상품(벡터 있는 것) 최근 5개의 평균. 품절 제외 상위 10개를 찾는다.
  fixed 질의 = 첫 주 질의 100개를 모든 체크포인트에 그대로 쓴다.

결과는 data/exp2c/에 캐시한다.

    python -m pipeline.exp2c_data           # 캐시 생성 + 요약 출력
"""
from __future__ import annotations

import json
import time
import zlib
from pathlib import Path

import numpy as np
import pandas as pd

RAW = Path("data/retail")
OUT = Path("data/exp2c")
SESSION_GAP_MS = 30 * 60 * 1000
DIM, WINDOW, NEG, EPOCHS, MIN_COUNT, SEED = 64, 20, 5, 10, 2, 42
RECENT = 5
NQ = 100
WEEK_MS = 7 * 24 * 3600 * 1000


def _hash(s: str) -> int:
    """gensim 초기 벡터 시드용. 파이썬 hash()는 실행마다 달라져 재현이 안 된다."""
    return zlib.crc32(s.encode())


def _views() -> pd.DataFrame:
    ev = pd.read_csv(RAW / "events.csv", usecols=["timestamp", "visitorid", "event", "itemid"])
    v = ev[ev.event == "view"].drop(columns="event")
    return v.sort_values(["visitorid", "timestamp"], kind="stable").reset_index(drop=True)


def _sessions(v: pd.DataFrame) -> list[list[str]]:
    vis, ts = v.visitorid.to_numpy(), v.timestamp.to_numpy()
    new = np.r_[True, (vis[1:] != vis[:-1]) | (np.diff(ts) > SESSION_GAP_MS)]
    toks = v.itemid.to_numpy().astype(str)
    return [s.tolist() for s in np.split(toks, np.flatnonzero(new)[1:])]


def _train(sessions: list[list[str]]):
    from gensim.models import Word2Vec
    t = time.perf_counter()
    m = Word2Vec(sessions, vector_size=DIM, window=WINDOW, min_count=MIN_COUNT, sg=1, negative=NEG,
                 epochs=EPOCHS, workers=1, seed=SEED, hashfxn=_hash)
    return m.wv, time.perf_counter() - t


def _properties() -> tuple[pd.DataFrame, pd.Series]:
    avail, cat = [], []
    for p in sorted(RAW.glob("item_properties_part*.csv")):
        for ch in pd.read_csv(p, chunksize=5_000_000):
            avail.append(ch[ch.property == "available"])
            cat.append(ch[ch.property == "categoryid"])
    a = pd.concat(avail)
    a["value"] = a.value.astype(np.int8)
    c = pd.concat(cat).sort_values("timestamp").groupby("itemid").value.last().astype(np.int64)
    return a, c


def build(out: Path = OUT) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    v = _views()
    n_views = v.itemid.value_counts()
    sessions = _sessions(v)
    wv, train_s = _train(sessions)
    wv.save(str(out / "item2vec.kv"))
    W = wv.vectors / np.linalg.norm(wv.vectors, axis=1, keepdims=True)
    vocab = {int(k): i for i, k in enumerate(wv.index_to_key)}

    a, cat = _properties()
    snaps = np.sort(a.timestamp.unique())
    a = a[a.itemid.isin(vocab.keys())]
    st = (a.pivot_table(index="itemid", columns="timestamp", values="value", aggfunc="last")
          .reindex(columns=snaps).ffill(axis=1).sort_index())
    items = st.index.to_numpy(np.int64)                     # 카탈로그 = 벡터 있음 ∩ available 기록 있음
    S = st.to_numpy(np.float32)                              # NaN = 아직 기록 없음
    vecs = np.ascontiguousarray(W[[vocab[i] for i in items.tolist()]], dtype=np.float32)
    views = n_views.reindex(items).to_numpy(np.int64)

    init_item = np.flatnonzero(S[:, 0] == 1)
    op_snap, op_item, op_on, op_kind = [], [], [], []          # kind 0 품절, 1 재입고, 2 신규
    for k in range(1, len(snaps)):
        prev, cur = S[:, k - 1], S[:, k]
        off = np.flatnonzero((prev == 1) & (cur == 0))
        rest = np.flatnonzero((prev == 0) & (cur == 1))
        new = np.flatnonzero(np.isnan(prev) & (cur == 1))
        on = np.sort(np.r_[rest, new])
        op_snap += [k] * (len(off) + len(on))
        op_item += off.tolist() + on.tolist()
        op_on += [False] * len(off) + [True] * len(on)
        op_kind += [0] * len(off) + np.where(np.isin(on, rest), 1, 2).tolist()

    # 질의: 벡터 있는 상품 조회만 남겨 방문자별 최근 RECENT개 평균
    vv = v[v.itemid.isin(vocab.keys())].reset_index(drop=True)
    vis, ts = vv.visitorid.to_numpy(), vv.timestamp.to_numpy()
    rows = np.fromiter((vocab[i] for i in vv.itemid.tolist()), np.int64, len(vv))
    gstart = np.maximum.accumulate(np.where(np.r_[True, vis[1:] != vis[:-1]], np.arange(len(vv)), 0))
    Q = np.zeros((len(snaps), NQ, DIM), np.float32)
    q_pos = np.zeros((len(snaps), NQ), np.int64)
    q_n = np.zeros((len(snaps), NQ), np.int8)
    for k, s in enumerate(snaps):
        cand = np.flatnonzero((ts >= s) & (ts < s + WEEK_MS))
        pick = np.sort(np.random.default_rng(2000 + k).choice(cand, NQ, replace=False))
        for j, p in enumerate(pick.tolist()):
            lo = max(int(gstart[p]), p - RECENT + 1)
            q = W[rows[lo:p + 1]].mean(axis=0)
            Q[k, j] = q / np.linalg.norm(q)
            q_pos[k, j], q_n[k, j] = p, p + 1 - lo

    # item2vec 품질 확인: 이웃 10개 중 같은 카테고리 비율 vs 무작위 두 상품이 같은 카테고리일 확률
    rng = np.random.default_rng(SEED)
    c = cat.reindex(items).to_numpy()
    has = np.flatnonzero(~np.isnan(c.astype(float)))
    probe = np.sort(rng.choice(has, 1000, replace=False))
    sc = vecs[probe] @ vecs[has].T
    sc[np.arange(len(probe)), np.searchsorted(has, probe)] = -np.inf
    nn = has[np.argpartition(-sc, 10, axis=1)[:, :10]]
    same_nn = float((c[nn] == c[probe][:, None]).mean())
    cc = pd.Series(c[has]).value_counts(normalize=True)
    same_rand = float((cc ** 2).sum())

    op_snap_a, op_on_a, op_kind_a = np.array(op_snap, np.int16), np.array(op_on, bool), np.array(op_kind, np.int8)
    np.save(out / "vectors.f32.npy", vecs)
    np.savez(out / "stream.npz", item_id=items, views=views, views5=views >= 5, state=S, snap_ts=snaps.astype(np.int64),
             init_item=init_item.astype(np.int64), op_snap=op_snap_a, op_item=np.array(op_item, np.int64),
             op_on=op_on_a, op_kind=op_kind_a, q=Q, q_n=q_n, q_visitor=vis[q_pos], q_ts=ts[q_pos],
             q_item=vv.itemid.to_numpy()[q_pos])
    known = ~np.isnan(S)
    summary = {
        "views": int(len(v)), "sessions": len(sessions), "sessions_len2plus": int(sum(len(s) >= 2 for s in sessions)),
        "item2vec": {"dim": DIM, "window": WINDOW, "negative": NEG, "epochs": EPOCHS, "min_count": MIN_COUNT,
                     "seed": SEED, "workers": 1, "vocab": len(vocab), "train_s": round(train_s, 1),
                     "gensim": __import__("gensim").__version__},
        "catalog_items": int(len(items)), "catalog_views5": int((views >= 5).sum()),
        "snapshots": len(snaps), "first_snapshot": str(pd.to_datetime(snaps[0], unit="ms", utc=True)),
        "last_snapshot": str(pd.to_datetime(snaps[-1], unit="ms", utc=True)),
        "init_live": int(len(init_item)), "init_known": int(known[:, 0].sum()),
        "ops": int(len(op_item)), "offs": int((op_kind_a == 0).sum()), "restocks": int((op_kind_a == 1).sum()),
        "new_on": int((op_kind_a == 2).sum()),
        "ops_per_snapshot": np.bincount(op_snap_a, minlength=len(snaps)).tolist(),
        "unavailable_share": [round(float((S[:, k] == 0).sum() / known[:, k].sum()), 3) for k in range(len(snaps))],
        "live_per_snapshot": [int((S[:, k] == 1).sum()) for k in range(len(snaps))],
        "query_recent_mean": round(float(q_n.mean()), 2),
        "category_check": {"same_category_top10": round(same_nn, 3), "random_pair": round(same_rand, 4)},
        "build_s": round(time.perf_counter() - t0, 1),
    }
    (out / "stream.meta.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


def load(out: Path = OUT) -> tuple[np.ndarray, dict]:
    if not (out / "stream.npz").exists():
        build(out=out)
    return np.load(out / "vectors.f32.npy"), dict(np.load(out / "stream.npz"))


if __name__ == "__main__":
    print(json.dumps(build(), indent=2, ensure_ascii=False))
