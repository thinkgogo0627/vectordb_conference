"""공통 설정: Kafka 토픽 이름, .env 읽기. 키 값은 어디에도 출력하지 않는다."""
from __future__ import annotations

import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KAFKA = os.environ.get("MARKET_KAFKA", "localhost:9092")

# ① 수집 단계의 출력 (설계 3절). 값 = {"src", "recv_ts"(호스트 epoch ms), "data"(원천 메시지 그대로)}
T_UPBIT_TRADE = "raw.upbit.trade"
T_ALPACA = {                       # Alpaca 메시지 종류(T) → 토픽
    "b": "raw.alpaca.bar",         # 1분봉
    "u": "raw.alpaca.bar_update",  # 1분봉 정정 (늦게 온 체결)
    "s": "raw.alpaca.status",      # 거래 정지·재개
    "t": "raw.alpaca.trade",       # 체결 (최대 30종목)
    "c": "raw.alpaca.trade_fix",   # 체결 정정
    "x": "raw.alpaca.trade_fix",   # 체결 취소
}
T_COLLECTOR = "raw.collector.events"   # 연결·끊김·구독 응답·오류 (끊긴 구간 기록)
SESSION_START = "_session_start"       # 수집기가 (재)연결할 때 데이터 토픽에 넣는 표지. 토픽이 1파티션이라 순서가 보장된다


def env_file_values(path: Path = ROOT / ".env") -> dict[str, str]:
    """.env를 읽는다. 키 이름에 공백이 있는 줄도 건너뛰지 않고 읽는다."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"\s*([^#=\s][^=]*?)\s*=\s*(.*?)\s*$", line)
        if m:
            out[m.group(1)] = m.group(2).strip('"').strip("'")
    return out


def alpaca_keys() -> tuple[str, str]:
    """환경변수 또는 .env에서 이름에 ALPACA/APCA와 KEY(또는 ID)·SECRET이 들어간 값을 찾는다."""
    vals = {**env_file_values(), **os.environ}
    key = secret = None
    for name, val in vals.items():
        n = name.upper()
        if not val or not ("ALPACA" in n or "APCA" in n):
            continue
        if "SECRET" in n:
            secret = val
        elif "KEY" in n or "ID" in n:
            key = val
    if not (key and secret):
        raise SystemExit("Alpaca 키를 찾지 못함: .env에 ALPACA_API_KEY_ID, ALPACA_API_SECRET_KEY를 넣는다")
    return key, secret
