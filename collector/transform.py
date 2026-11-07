"""recentchange 이벤트 → 레이크에 저장할 한 행(row).

남기는 이벤트:
- edit / new          : 문서 생성·수정 (rev_id 있음 → 본문 조회 대상)
- log + delete 계열   : 문서 삭제·복구 (본문 없음)
나머지(categorize, move 등)는 실험에 안 쓰므로 버린다.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

KEEP_LOG_TYPES = {"delete"}  # log_action: delete, restore, revision, ...


def keep_event(evt: dict, wikis: set[str], namespaces: set[int], include_bots: bool) -> bool:
    if evt.get("wiki") not in wikis:
        return False
    if evt.get("namespace") not in namespaces:
        return False
    if not include_bots and evt.get("bot"):
        return False
    t = evt.get("type")
    if t in ("edit", "new"):
        return True
    if t == "log" and evt.get("log_type") in KEEP_LOG_TYPES:
        return True
    return False


def to_row(event_id: str | None, evt: dict) -> dict:
    meta = evt.get("meta") or {}
    rev = evt.get("revision") or {}
    length = evt.get("length") or {}
    t = evt.get("type")
    if t == "new":
        op = "create"
    elif t == "edit":
        op = "update"
    else:
        op = "delete" if evt.get("log_action") == "delete" else f"log_{evt.get('log_action')}"
    return {
        "event_id": meta.get("id"),
        "stream_offset_id": event_id,            # SSE id (재개용 위치)
        "event_dt": meta.get("dt"),              # 이벤트가 스트림에 들어온 시각 (UTC ISO)
        "rc_timestamp": evt.get("timestamp"),    # 위키 기준 변경 시각 (epoch sec)
        "wiki": evt.get("wiki"),
        "domain": meta.get("domain"),
        "op": op,                                # create | update | delete | log_*
        "rc_type": t,
        "log_type": evt.get("log_type"),
        "log_action": evt.get("log_action"),
        "namespace": evt.get("namespace"),
        "page_title": evt.get("title"),
        "rev_id": rev.get("new"),
        "parent_rev_id": rev.get("old"),
        "length_old": length.get("old"),
        "length_new": length.get("new"),
        "is_bot": bool(evt.get("bot")),
        "is_minor": evt.get("minor"),
        "comment": evt.get("comment"),
        # 아래는 Action API 보강 단계에서 채움
        "page_id": None,
        "rev_timestamp": None,
        "sha1": None,
        "rev_size": None,
        "tags": None,
        "content": None,
        "content_status": "pending" if rev.get("new") and t in ("edit", "new") else "none",
        "raw_event": json.dumps(evt, ensure_ascii=False),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
