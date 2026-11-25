"""④ CDC: Debezium으로 Postgres bars_1m 변경 → Kafka market.public.bars_1m.

    python -m market.cdc register      # 과거 데이터 채우기가 끝난 뒤에 등록한다 (그 전 행은 흘려보내지 않음)
    python -m market.cdc status
    python -m market.cdc delete

poll.interval.ms = 50 (실험 1: 기본 500ms가 간극의 대부분이었다).
"""
from __future__ import annotations

import argparse
import json
import os
import urllib.request

CONNECT = os.environ.get("MARKET_CONNECT_URL", "http://localhost:8083")
NAME = "market-bars"
TOPIC = "market.public.bars_1m"
CONFIG = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres", "database.port": "5432",
    "database.user": "exp", "database.password": "exp", "database.dbname": "exp",
    "topic.prefix": "market",
    "table.include.list": "public.bars_1m",
    "publication.autocreate.mode": "filtered", "publication.name": "market_pub", "slot.name": "market_slot",
    "snapshot.mode": "no_data",
    "tombstones.on.delete": "false",
    "poll.interval.ms": "50",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false", "value.converter.schemas.enable": "false",
}


def _req(method: str, path: str, body: dict | None = None):
    req = urllib.request.Request(CONNECT + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        txt = r.read().decode()
        return json.loads(txt) if txt else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["register", "status", "delete"])
    a = ap.parse_args()
    if a.action == "register":
        print(_req("PUT", f"/connectors/{NAME}/config", CONFIG)["name"], "등록")
    elif a.action == "status":
        print(json.dumps(_req("GET", f"/connectors/{NAME}/status"), indent=1))
    else:
        _req("DELETE", f"/connectors/{NAME}")
        print(NAME, "삭제 (복제 슬롯 market_slot은 Postgres에 남는다)")


if __name__ == "__main__":
    main()
