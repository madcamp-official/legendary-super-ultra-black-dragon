"""ingest_coordinator: pull the repos in repos.txt and index changed chunks.

Ported from camp-60's /root/ingest.py. The original ran under a 10-minute
cron with flock; here the loop lives in-process because systemd already
guarantees a single instance, so no lock file is needed.

Incremental by design: a file is re-embedded only when its (mtime, size)
signature differs from INDEX_STATE_FILE, so a normal pass over an unchanged
repo costs one git pull and nothing else.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import uuid
from typing import Optional

from fastembed import TextEmbedding
from qdrant_client import QdrantClient, models

from saem.common.config import (
    COLLECTION,
    EMBEDDING_DIM,
    EMBEDDING_MODEL,
    INDEX_STATE_FILE,
    INGEST_INTERVAL_SECONDS,
    QDRANT_URL,
    REPOS_DIR,
    REPOS_FILE,
)

EXTS = (
    ".py", ".m", ".md", ".txt", ".r", ".cpp", ".h", ".ipynb",
    ".js", ".java", ".ts", ".sh",
)
BATCH = 16


def _sync_repos() -> None:
    if not os.path.exists(REPOS_FILE):
        print(f"no {REPOS_FILE}; nothing to sync", flush=True)
        return
    os.makedirs(REPOS_DIR, exist_ok=True)
    for line in open(REPOS_FILE):
        parts = line.split()
        if not parts:
            continue
        url = parts[0]
        name = parts[1] if len(parts) > 1 else url.split("/")[-1].replace(".git", "")
        d = os.path.join(REPOS_DIR, name)
        if os.path.isdir(d):
            subprocess.run(["git", "-C", d, "pull", "-q"], timeout=120)
        else:
            subprocess.run(["git", "clone", "--depth", "1", "-q", url, d], timeout=300)
        print(f"sync: {name}", flush=True)


def _chunks(path: str, size: int = 800, overlap: int = 100):
    try:
        text = open(path, encoding="utf-8", errors="ignore").read()
    except Exception:
        return
    for i in range(0, max(len(text), 1), size - overlap):
        c = text[i : i + size].strip()
        if c:
            yield i, c


def _changed_files(state: dict) -> list:
    todo = []
    for root, _, files in os.walk(REPOS_DIR):
        if ".git" in root:
            continue
        for f in files:
            if not f.endswith(EXTS):
                continue
            p = os.path.join(root, f)
            st = os.stat(p)
            sig = f"{st.st_mtime}:{st.st_size}"
            if state.get(p) != sig:
                todo.append((p, sig))
    return todo


def ingest_once() -> None:
    _sync_repos()

    state = json.load(open(INDEX_STATE_FILE)) if os.path.exists(INDEX_STATE_FILE) else {}
    client = QdrantClient(url=QDRANT_URL, timeout=60, check_compatibility=False)
    if not client.collection_exists(COLLECTION):
        client.create_collection(
            COLLECTION,
            vectors_config=models.VectorParams(
                size=EMBEDDING_DIM, distance=models.Distance.COSINE
            ),
        )

    todo = _changed_files(state)
    print(f"변경 파일: {len(todo)}개", flush=True)
    if not todo:
        return

    model = TextEmbedding(EMBEDDING_MODEL)
    texts: list = []
    meta: list = []
    total = 0

    def flush():
        nonlocal texts, meta, total
        if not texts:
            return
        vecs = list(model.embed(texts))
        pts = []
        for (path, off, txt), v in zip(meta, vecs):
            key = hashlib.sha256(f"{path}:{off}:{txt}".encode()).hexdigest()
            pts.append(
                models.PointStruct(
                    id=str(uuid.UUID(key[:32])),
                    vector=v.tolist(),
                    payload={
                        "path": path.replace(REPOS_DIR.rstrip("/") + "/", ""),
                        "offset": off,
                        "text": txt,
                    },
                )
            )
        client.upsert(COLLECTION, pts)
        total += len(pts)
        texts, meta = [], []

    for p, sig in todo:
        for off, c in _chunks(p):
            texts.append(c)
            meta.append((p, off, c))
            if len(texts) >= BATCH:
                flush()
        state[p] = sig

    flush()
    json.dump(state, open(INDEX_STATE_FILE, "w"))
    print(f"색인: {total}청크 (전체 {client.count(COLLECTION).count})", flush=True)


def run(port: Optional[int] = None) -> None:
    while True:
        try:
            ingest_once()
        except Exception as e:
            # Keep the loop alive; systemd restarting on every transient
            # network/Qdrant blip would just lose the in-memory model.
            print(f"ingest failed: {e}", flush=True)
        time.sleep(INGEST_INTERVAL_SECONDS)
