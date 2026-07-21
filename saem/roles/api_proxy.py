"""api_proxy: authenticated external-facing proxy in front of the LLM backend.

Ported from camp-73's /root/api_proxy.py. OpenAI-compatible surface, so
other teams point their existing clients at it unchanged.

This role is the one exception that must be reachable from outside the
internal network, so it binds a port the cloud security group actually
allows (22/443 only — hence the 443 default). Every other role talks over
192.168.0.x exclusively.

Every request/response is appended to API_LOG_FILE. That file is the
fine-tuning corpus and stays valid across model swaps — it is not a
disposable log.
"""
from __future__ import annotations

import json
import time
from typing import Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from saem.common.config import API_KEYS_FILE, API_LOG_FILE, LLM_TIMEOUT, get_llm_backend

app = FastAPI(title="saem-api-proxy")


def load_keys() -> dict:
    """api_keys.txt is one `<key> <team>` per line; edit + restart to rotate."""
    d = {}
    try:
        for line in open(API_KEYS_FILE):
            p = line.split()
            if len(p) >= 2:
                d[p[0]] = p[1]
    except FileNotFoundError:
        pass
    return d


KEYS = load_keys()


def check(request: Request) -> str:
    token = request.headers.get("authorization", "").replace("Bearer ", "").strip()
    team = KEYS.get(token)
    if not team:
        raise HTTPException(401, "invalid or missing API key")
    return team


def log(entry: dict) -> None:
    with open(API_LOG_FILE, "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


@app.get("/health")
async def health():
    """Unauthenticated on purpose, so partners can check reachability."""
    url, _ = get_llm_backend()
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(url.rstrip("/") + "/health", timeout=10)
        return {"proxy": "ok", "upstream": r.status_code}
    except Exception as e:
        return JSONResponse(
            status_code=503, content={"proxy": "ok", "upstream": f"down: {e}"}
        )


@app.get("/v1/models")
async def models(request: Request):
    check(request)
    url, _ = get_llm_backend()
    async with httpx.AsyncClient() as c:
        r = await c.get(url.rstrip("/") + "/v1/models", timeout=30)
    return JSONResponse(status_code=r.status_code, content=r.json())


@app.post("/v1/chat/completions")
async def chat(request: Request):
    team = check(request)
    url, _ = get_llm_backend()
    endpoint = url.rstrip("/") + "/v1/chat/completions"
    body = await request.json()
    t0 = time.time()

    if body.get("stream"):
        log({"ts": time.time(), "team": team, "stream": True, "messages": body.get("messages")})

        async def gen():
            async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
                async with c.stream("POST", endpoint, json=body) as r:
                    async for chunk in r.aiter_bytes():
                        yield chunk

        return StreamingResponse(gen(), media_type="text/event-stream")

    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as c:
        r = await c.post(endpoint, json=body)
    resp = r.json()
    ans = None
    try:
        ans = resp["choices"][0]["message"]["content"]
    except Exception:
        pass
    log(
        {
            "ts": time.time(),
            "team": team,
            "messages": body.get("messages"),
            "answer": ans,
            "latency_s": round(time.time() - t0, 1),
        }
    )
    return JSONResponse(status_code=r.status_code, content=resp)


def run(port: Optional[int] = None) -> None:
    uvicorn.run(app, host="0.0.0.0", port=port or 443)
