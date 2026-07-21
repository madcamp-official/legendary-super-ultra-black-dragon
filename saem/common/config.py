"""Shared config knobs, overridable via environment variables.

Keeping these here (instead of hardcoded per-script, as the original
nohup scripts did) is what lets one role module run correctly on any
node without editing code per-VM.
"""
from __future__ import annotations

import os

AGENT_PORT = int(os.environ.get("SAEM_AGENT_PORT", "9999"))

# Qdrant lives on whichever node holds the qdrant_primary role.
QDRANT_URL = os.environ.get("SAEM_QDRANT_URL", "http://192.168.0.252:6333")
COLLECTION = os.environ.get("SAEM_COLLECTION", "knowledge")

# Web-fallback crawler, i.e. whichever node holds the crawler role.
CRAWLER_URL = os.environ.get("SAEM_CRAWLER_URL", "http://192.168.0.44:9200/crawl")

# 384-dim; e5-large was dropped because it OOM-killed on these 3GB VMs.
EMBEDDING_MODEL = os.environ.get(
    "SAEM_EMBEDDING_MODEL",
    "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
)
EMBEDDING_DIM = int(os.environ.get("SAEM_EMBEDDING_DIM", "384"))

GATEWAY_SEARCH_THRESHOLD = float(os.environ.get("SAEM_SEARCH_THRESHOLD", "0.40"))
LLM_TIMEOUT = int(os.environ.get("SAEM_LLM_TIMEOUT", "600"))
LLM_MAX_TOKENS = int(os.environ.get("SAEM_LLM_MAX_TOKENS", "2000"))

# ingest_coordinator
REPOS_FILE = os.environ.get("SAEM_REPOS_FILE", "/root/repos.txt")
REPOS_DIR = os.environ.get("SAEM_REPOS_DIR", "/root/repos")
INDEX_STATE_FILE = os.environ.get("SAEM_INDEX_STATE", "/root/.index_state.json")
INGEST_INTERVAL_SECONDS = int(os.environ.get("SAEM_INGEST_INTERVAL", "600"))

# api_proxy. api_log.jsonl is the fine-tuning corpus (survives model swaps),
# not a disposable log — see saem-rag-vm-plan.
API_KEYS_FILE = os.environ.get("SAEM_API_KEYS_FILE", "/root/api_keys.txt")
API_LOG_FILE = os.environ.get("SAEM_API_LOG_FILE", "/root/api_log.jsonl")

# qdrant_primary. v1.12.6 is the compatibility ceiling — newer builds need
# GLIBC 2.38 and these VMs have 2.35.
QDRANT_BINARY = os.environ.get("SAEM_QDRANT_BINARY", "/root/qdrant/qdrant")


def get_llm_backend() -> tuple[str, str]:
    """(url, model) of whichever dure GPU-cluster backend head last pushed to
    this node via `saem head register-backend`. Falls back to the env vars
    below (useful for local testing, or the very first node before head has
    registered anything) — but the registry is the source of truth so a new
    GPU head (camp1, a 235B replacement, ...) can be swapped in fleet-wide
    with one `saem head register-backend` call, no code/config edits here.
    """
    from saem.common.state import read_backend

    backend = read_backend()
    if backend:
        return backend["url"], backend["model"]
    return (
        os.environ.get("SAEM_VLLM_URL", "http://192.168.0.228:8000"),
        os.environ.get("SAEM_VLLM_MODEL_NAME", "qwen3-235b"),
    )
