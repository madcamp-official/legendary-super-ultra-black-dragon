"""retrieval_gateway: search the index, fall back to the web, then ask the LLM.

Ported from camp-59's /root/gateway.py v3 (two-stage fallback).

Branching on the top Qdrant score:
  >= THRESH  -> mode "repo": answer grounded in indexed repo chunks
  <  THRESH  -> ask the crawler; if it returns anything, mode "web"
                otherwise mode "none": plain general-knowledge answer

Unlike the original, the vLLM address is not hardcoded — it comes from
whichever backend head last registered (`saem head register-backend`), so
swapping GPU clusters needs no edit here.
"""
from __future__ import annotations

import json
import urllib.request
from typing import Optional

import uvicorn
from fastapi import FastAPI
from fastembed import TextEmbedding
from qdrant_client import QdrantClient

from saem.common.config import (
    COLLECTION,
    CRAWLER_URL,
    EMBEDDING_MODEL,
    GATEWAY_SEARCH_THRESHOLD,
    LLM_MAX_TOKENS,
    LLM_TIMEOUT,
    QDRANT_URL,
    get_llm_backend,
)

app = FastAPI(title="saem-retrieval-gateway")

model = TextEmbedding(EMBEDDING_MODEL)
client = QdrantClient(url=QDRANT_URL, timeout=60, check_compatibility=False)


def ask_llm(system: str, history: list, q: str) -> str:
    url, model_name = get_llm_backend()
    messages = [{"role": "system", "content": system}] + history + [{"role": "user", "content": q}]
    req = urllib.request.Request(
        url.rstrip("/") + "/v1/chat/completions",
        data=json.dumps(
            {"model": model_name, "messages": messages, "max_tokens": LLM_MAX_TOKENS}
        ).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=LLM_TIMEOUT) as r:
        return json.load(r)["choices"][0]["message"]["content"]


def _answer(collection: str, body: dict) -> dict:
    q = body["question"]
    history = body.get("history", [])
    vec = list(model.embed([q]))[0].tolist()
    hits = client.query_points(collection, query=vec, limit=5).points
    top = hits[0].score if hits else 0.0

    if top >= GATEWAY_SEARCH_THRESHOLD:
        ctx = "\n\n---\n\n".join(
            f"[출처: {h.payload['path']}]\n{h.payload['text']}" for h in hits
        )
        sys = "사용자 저장소에서 검색된 자료다. 이를 근거로 답하고 출처 파일명을 표기하라.\n\n" + ctx
        return {
            "mode": "repo",
            "top_score": round(top, 3),
            "answer": ask_llm(sys, history, q),
            "sources": list({h.payload["path"] for h in hits}),
        }

    try:
        creq = urllib.request.Request(
            CRAWLER_URL,
            data=json.dumps({"question": q}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(creq, timeout=60) as r:
            snippets = json.load(r).get("snippets", [])
    except Exception:
        snippets = []

    if snippets:
        ctx = "\n\n---\n\n".join(f"[출처: {s['url']}]\n{s['text']}" for s in snippets)
        sys = (
            "저장소에 관련 자료가 없어 웹에서 검색한 자료다. 이를 근거로 답하고 출처 URL을 표기하라. "
            "자료가 부족하면 부족하다고 말하라.\n\n" + ctx
        )
        return {
            "mode": "web",
            "top_score": round(top, 3),
            "answer": ask_llm(sys, history, q),
            "sources": [s["url"] for s in snippets],
        }

    return {
        "mode": "none",
        "top_score": round(top, 3),
        "answer": ask_llm("검색 자료 없이 일반 지식으로 답하라.", history, q),
        "sources": [],
    }


@app.post("/ask")
def ask(body: dict):
    return _answer(COLLECTION, body)


@app.post("/vibecutter/ask")
def vibecutter_ask(body: dict):
    """Separate collection shared with the VibeCutter team."""
    return _answer("vibecutter", body)


def run(port: Optional[int] = None) -> None:
    uvicorn.run(app, host="0.0.0.0", port=port or 9000)
