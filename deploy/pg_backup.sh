#!/usr/bin/env bash
# Postgres 전체 덤프 → R2 market/pgdump/YYYY-MM-DD.dump, 최근 3개만 남긴다. market-pgbackup.timer가 매일 부른다.
#   bash deploy/pg_backup.sh            # 손으로 한 번
#   복구: pg_restore -U exp -d exp --clean <덤프 파일>
set -euo pipefail
cd "$(dirname "$0")/.."
DAY=$(date -u +%Y-%m-%d)
F="/tmp/market-$DAY.dump"
docker compose -f deploy/docker-compose.server.yml exec -T postgres pg_dump -U exp -d exp -Fc -Z 6 > "$F"
.venv/bin/python -m market.r2 put "market/pgdump/$DAY.dump" "$F"
rm -f "$F"
.venv/bin/python -m market.r2 prune market/pgdump/ --keep 3
