FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collector/ collector/

# 데이터(버퍼, 재개 위치)는 볼륨에 둔다
VOLUME ["/data"]

# 마지막 flush가 15분 넘게 없으면 unhealthy
HEALTHCHECK --interval=60s --timeout=5s --start-period=120s --retries=3 \
  CMD python -c "import time,sys; t=int(open('/data/state/heartbeat').read()); sys.exit(0 if time.time()-t < 900 else 1)"

CMD ["python", "-m", "collector.main"]
