# 서버 배포 (AWS EC2 t4g.large, Ubuntu 24.04 Arm, 시드니)

수집기·bar_builder·Postgres·Kafka·Debezium·벡터 워커를 한 서버에서 돈다. 보안 그룹은 SSH(22, 내 IP)만 연다.
시드니에서 업비트(200)·Alpaca(401, 키 없이 요청)에 닿는 것을 확인했다 (2026-11-26).

## 1. 코드 받기
```bash
git clone -b feat/market-collector https://github.com/thinkgogo0627/vectordb_conference.git
cd vectordb_conference
```

## 2. Alpaca 키 넣기 (서버에서 직접)
```bash
nano .env          # 아래 두 줄을 넣고 저장. R2 등 다른 키는 넣지 않는다
# ALPACA_API_KEY_ID=...
# ALPACA_API_SECRET_KEY=...
```

## 3. 준비 (가상환경, 패키지, Postgres·Kafka·Debezium, systemd 템플릿)
```bash
bash deploy/setup.sh
```

## 4. 수집기와 1분봉 정규화 시작 (24시간)
```bash
sudo systemctl enable --now market@upbit_collector market@alpaca_collector market@bar_builder
journalctl -u market@bar_builder -f        # Ctrl+C로 빠져나온다
```

## 5. 과거 데이터·평소 값 (한 번, tmux 안에서)
업비트 약 12분, Alpaca 약 1시간. 접속이 끊겨도 계속 돌도록 tmux에서 돌린다.
```bash
tmux new -s backfill
.venv/bin/python -m market.backfill upbit --days 7 --top 150 && .venv/bin/python -m market.backfill upbit-gap --hours 2 && .venv/bin/python -m market.backfill alpaca --days 20
# Ctrl+B 다음 D로 빠져나오고, tmux attach -t backfill 로 다시 본다
```

## 6. CDC 등록과 벡터 워커 (과거 데이터를 다 넣은 뒤)
```bash
.venv/bin/python -m market.pairs build                  # 같은 기초자산 쌍 (BTC ↔ IBIT). 출력의 상관 분포를 확인
.venv/bin/python -m market.cdc register
sudo systemctl enable --now market@vector_worker
.venv/bin/python -m market.similar KRW-BTC
```

## 7. R2 보관 (원천 메시지 + Postgres 일일 덤프)
`.env`에 R2 설정 4줄을 더한다 (로컬 `.env`나 Cloudflare 대시보드에서 직접 복사):
```bash
nano .env
# S3_ENDPOINT=https://<계정ID>.r2.cloudflarestorage.com
# S3_BUCKET=...
# S3_ACCESS_KEY=...
# S3_SECRET_KEY=...
```
```bash
git pull && bash deploy/setup.sh                       # 새 패키지(boto3, pyarrow)와 백업 타이머 등록
.venv/bin/python -m market.r2 ls market/                # R2 접속 확인
sudo systemctl enable --now market@archiver market-pgbackup.timer
bash deploy/pg_backup.sh                                # 덤프 한 번 바로 올려 보기
.venv/bin/python -m market.r2 ls market/
```
- 원천 메시지: `market/raw/<토픽>/date=YYYY-MM-DD/p<파티션>-<첫 오프셋>-<끝 오프셋>.parquet`, 10분마다.
  올리기에 성공한 뒤에만 Kafka 오프셋을 커밋하므로 빠지는 메시지는 없고, 중복은 (토픽, 파티션, offset)으로 걸러 낸다.
- Postgres: 매일 22:00 UTC에 `market/pgdump/YYYY-MM-DD.dump`, 최근 3개만 남긴다.

## 운영
- 상태: `systemctl status 'market@*'`, `docker compose -f deploy/docker-compose.server.yml ps`
- 로그: `journalctl -u market@<이름> -f`
- 코드 갱신: `git pull && sudo systemctl restart 'market@*'`
- 서버 시계는 Amazon Time Sync(chrony)로 맞춰진다 (`timedatectl`로 확인). 로컬 PC처럼 시계가 어긋나 CDC가 늦어지는 문제는 없어야 한다.
- 비용: 월 한도 90달러. 발표가 끝나면 인스턴스를 **종료(Terminate)**한다.
