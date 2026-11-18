"""Debezium 커넥터 등록 (T-03). 이미 있으면 설정을 갱신한다.

    python -m pipeline.register_connector
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import requests

from .config import CONNECT_URL

CONNECTOR = json.loads((Path(__file__).parent / "connector.json").read_text(encoding="utf-8"))


def wait_connect(timeout_s: float = 180) -> None:
    t_end = time.time() + timeout_s
    while time.time() < t_end:
        try:
            if requests.get(f"{CONNECT_URL}/connectors", timeout=3).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise SystemExit(f"Kafka Connect not reachable at {CONNECT_URL}")


def register(overrides: dict | None = None) -> dict:
    """connector.json 설정으로 등록·갱신. overrides로 일부 값을 바꿔 등록한다 (실험 1: poll.interval.ms)."""
    wait_connect()
    name = CONNECTOR["name"]
    cfg = dict(CONNECTOR["config"], **{k: str(v) for k, v in (overrides or {}).items()})
    cur = requests.get(f"{CONNECT_URL}/connectors/{name}/config", timeout=10)
    if cur.ok and {k: v for k, v in cur.json().items() if k != "name"} == cfg:
        pass  # 같은 설정이면 다시 넣지 않는다 (태스크 재시작 방지)
    else:
        r = requests.put(f"{CONNECT_URL}/connectors/{name}/config", json=cfg, timeout=30)
        r.raise_for_status()
        time.sleep(3)
    r = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10)
    r.raise_for_status()
    # RUNNING 될 때까지 대기
    for _ in range(60):
        st = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10).json()
        tasks = st.get("tasks", [])
        if tasks and all(t.get("state") == "RUNNING" for t in tasks):
            return st
        if any(t.get("state") == "FAILED" for t in tasks):
            print(json.dumps(st, indent=2), file=sys.stderr)
            raise SystemExit("connector task FAILED")
        time.sleep(1)
    raise SystemExit("connector did not reach RUNNING")


if __name__ == "__main__":
    st = register()
    print("connector:", st["name"], st["connector"]["state"], [t["state"] for t in st["tasks"]])
