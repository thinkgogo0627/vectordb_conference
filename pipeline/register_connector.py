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


def register() -> dict:
    wait_connect()
    name = CONNECTOR["name"]
    r = requests.put(f"{CONNECT_URL}/connectors/{name}/config", json=CONNECTOR["config"], timeout=30)
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
