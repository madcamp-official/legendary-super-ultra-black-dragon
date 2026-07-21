"""crawler: web fallback search, used when the Qdrant score is below threshold.

Ported from camp-18's /root/crawler.py.

⚠️ trafilatura needs lxml_html_clean installed separately. Without it the
import fails and the service never comes up at all.
"""
from __future__ import annotations

from typing import Optional

import trafilatura
import uvicorn
from ddgs import DDGS
from fastapi import FastAPI

app = FastAPI(title="saem-crawler")


@app.post("/crawl")
def crawl(body: dict):
    q = body["question"]
    results = []
    try:
        with DDGS() as ddgs:
            hits = list(ddgs.text(q, max_results=5))
    except Exception as e:
        return {"snippets": [], "error": f"검색 실패: {e}"}
    for h in hits[:4]:
        url = h.get("href") or h.get("url")
        if not url:
            continue
        try:
            html = trafilatura.fetch_url(url)
            text = trafilatura.extract(html) if html else None
            if text:
                results.append({"url": url, "title": h.get("title", ""), "text": text[:2000]})
        except Exception:
            continue
        if len(results) >= 3:
            break
    return {"snippets": results}


def run(port: Optional[int] = None) -> None:
    uvicorn.run(app, host="0.0.0.0", port=port or 9200)
