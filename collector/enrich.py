"""MediaWiki Action API로 리비전 본문·메타 보강.

recentchange 이벤트에는 본문이 없어서, rev_id를 최대 50개씩 묶어 한 번에 조회한다.
- sha1  : 같은 내용으로 돌아간 '되돌림'을 오프라인에서 판별하는 키
- tags  : mw-rollback / mw-undo / mw-manual-revert 등 되돌림 태그
- content: 실험 때 청킹·임베딩할 원문 (wikitext)
"""
from __future__ import annotations

import logging
import time

import requests

log = logging.getLogger(__name__)

RVPROP_META = "ids|timestamp|sha1|size|tags"


class Enricher:
    def __init__(self, user_agent: str, fetch_content: bool = True, session: requests.Session | None = None,
                 stop=lambda: False):
        self.fetch_content = fetch_content
        self.stop = stop                      # 종료 신호가 오면 재시도를 멈추고 'failed'로 남김
        self.s = session or requests.Session()
        self.s.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip"})

    def _api_url(self, domain: str) -> str:
        return f"https://{domain}/w/api.php"

    def fetch(self, domain: str, revids: list[int]) -> dict[int, dict]:
        """revid → {page_id, rev_timestamp, sha1, rev_size, tags, content, content_status}"""
        revids = list(dict.fromkeys(revids))  # 중복 제거(순서 유지)
        if not revids:
            return {}
        params = {
            "action": "query", "format": "json", "formatversion": "2",
            "prop": "revisions", "revids": "|".join(map(str, revids)),
            "rvprop": RVPROP_META + ("|content" if self.fetch_content else ""),
            "rvslots": "main", "maxlag": "5",
        }
        data = self._get_with_retry(self._api_url(domain), params)
        out: dict[int, dict] = {}
        if data is None:
            return out
        q = data.get("query", {})
        for page in q.get("pages", []):
            for r in page.get("revisions", []) or []:
                content = None
                status = "skipped" if not self.fetch_content else "ok"
                if self.fetch_content:
                    slot = (r.get("slots") or {}).get("main") or {}
                    if slot.get("texthidden") or "content" not in slot:
                        status = "hidden"
                    else:
                        content = slot.get("content")
                out[r["revid"]] = {
                    "page_id": page.get("pageid"),
                    "rev_timestamp": r.get("timestamp"),
                    "sha1": r.get("sha1"),
                    "rev_size": r.get("size"),
                    "tags": r.get("tags") or [],
                    "content": content,
                    "content_status": status,
                }
        # 삭제됐거나 숨겨진 리비전은 badrevids로 온다
        for bad in (q.get("badrevids") or {}).keys():
            out[int(bad)] = {"content_status": "missing"}
        return out

    def _get_with_retry(self, url: str, params: dict, tries: int = 5) -> dict | None:
        delay = 2.0
        for i in range(tries):
            if i and self.stop():
                return None
            try:
                r = self.s.get(url, params=params, timeout=30)
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {r.status_code}")
                r.raise_for_status()
                data = r.json()
                err = data.get("error")
                if err and err.get("code") == "maxlag":
                    wait = float(r.headers.get("Retry-After", delay))
                    log.info("maxlag, wait %.0fs", wait)
                    time.sleep(wait)
                    continue
                return data
            except (requests.RequestException, ValueError) as e:
                log.warning("enrich error (%d/%d): %s", i + 1, tries, str(e)[:200])
                end = time.monotonic() + delay
                while time.monotonic() < end and not self.stop():
                    time.sleep(0.5)
                delay = min(delay * 2, 60)
        return None


def apply_enrichment(rows: list[dict], enriched: dict[int, dict]) -> None:
    for row in rows:
        if row.get("content_status") != "pending":
            continue
        info = enriched.get(row["rev_id"])
        if info is None:
            row["content_status"] = "failed"   # API 실패: 나중에 백필 대상
            continue
        row.update(info)
