#!/usr/bin/env bash
# 서버 첫 준비: 파이썬 가상환경, 패키지, Postgres·Kafka·Debezium, systemd 템플릿.
# 저장소 루트에서 실행:  bash deploy/setup.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo ".env가 없다. Alpaca 키를 먼저 넣는다 (deploy/README.md 2단계)." >&2
  exit 1
fi
chmod 600 .env

python3 -m venv .venv
.venv/bin/pip install -q -U pip
.venv/bin/pip install -q -r deploy/requirements-server.txt

docker compose -f deploy/docker-compose.server.yml up -d --wait

sudo cp deploy/market@.service deploy/market-pgbackup.service deploy/market-pgbackup.timer /etc/systemd/system/
sudo systemctl daemon-reload

echo "준비 끝. 다음: deploy/README.md 4단계 (수집기·bar_builder 시작)"
