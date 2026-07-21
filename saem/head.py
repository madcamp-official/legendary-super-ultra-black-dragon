"""Runs on whichever single node is designated head (`saem head start`).
Not a server itself — `saem head register` just does an HTTP call out to
the target VM's agent and keeps a local copy in head_registry.yaml so
`saem head status` has something to show without re-polling every node.
"""
from __future__ import annotations

import pathlib
from typing import Optional

import httpx

from saem.common.config import AGENT_PORT
from saem.common.state import (
    SAEM_DIR,
    read_backend_registry,
    read_head_registry,
    read_token,
    remove_backend_registry_entry,
    remove_head_registry_entry,
    upsert_backend_registry_entry,
    upsert_head_registry_entry,
)

BACKEND_CONSUMER_ROLES = ("retrieval_gateway", "api_proxy")

HEAD_MARKER = SAEM_DIR / "is_head"


def start(ip: str) -> None:
    SAEM_DIR.mkdir(parents=True, exist_ok=True)
    HEAD_MARKER.write_text(ip, encoding="utf-8")
    # record head itself in the registry so `saem head status` shows the
    # whole cluster (head included), not just the nodes it has assigned
    upsert_head_registry_entry(ip, "head", None)


def is_head() -> bool:
    return HEAD_MARKER.exists()


def get_head_ip() -> Optional[str]:
    if not HEAD_MARKER.exists():
        return None
    return HEAD_MARKER.read_text(encoding="utf-8").strip()


def register(ip: str, role: str, port: Optional[int] = None, timeout: float = 10.0) -> dict:
    token = read_token()
    resp = httpx.post(
        f"http://{ip}:{AGENT_PORT}/role",
        json={"role": role, "port": port},
        headers={"x-saem-token": token},
        timeout=timeout,
    )
    resp.raise_for_status()
    upsert_head_registry_entry(ip, role, port)
    return resp.json()


def unregister(ip: str, timeout: float = 10.0, force: bool = False) -> dict:
    """Drop the node's role and remove it from the registry.

    If the node is unreachable the registry entry is kept, so the cluster
    view doesn't silently lose a node that is merely rebooting — pass
    `force` to drop it anyway (e.g. the VM is gone for good).
    """
    token = read_token()
    node_result: dict = {}
    try:
        resp = httpx.request(
            "DELETE",
            f"http://{ip}:{AGENT_PORT}/role",
            headers={"x-saem-token": token},
            timeout=timeout,
        )
        resp.raise_for_status()
        node_result = resp.json()
    except Exception as e:
        if not force:
            raise RuntimeError(
                f"{ip} unreachable ({e}); registry left intact. "
                f"Re-run with --force to drop it anyway."
            )
        node_result = {"status": "unreachable", "error": str(e)}
    removed = remove_head_registry_entry(ip)
    return {"ip": ip, "removed_from_registry": removed, "node": node_result}


def status() -> list[dict]:
    return read_head_registry()


def register_backend(
    name: str, url: str, model: str, active: bool = True, timeout: float = 10.0
) -> dict:
    """Register a dure GPU-cluster head (e.g. the 235B cluster, or a future
    camp1). dure never installs saem — head just remembers its URL and, if
    `active`, pushes it out to every currently-registered retrieval_gateway
    / api_proxy node so they start calling it."""
    upsert_backend_registry_entry(name, url, model, active=active)
    pushed_to: dict[str, dict] = {}
    if active:
        token = read_token()
        consumers = [e for e in read_head_registry() if e["role"] in BACKEND_CONSUMER_ROLES]
        for c in consumers:
            resp = httpx.post(
                f"http://{c['ip']}:{AGENT_PORT}/backend",
                json={"name": name, "url": url, "model": model},
                headers={"x-saem-token": token},
                timeout=timeout,
            )
            resp.raise_for_status()
            pushed_to[c["ip"]] = resp.json()
    return {"registered": name, "active": active, "pushed_to": pushed_to}


def unregister_backend(name: str, timeout: float = 10.0) -> dict:
    """Forget a backend. If it was the active one, the consumer nodes are
    told to clear it too — otherwise they would keep calling a cluster head
    considers retired."""
    removed = remove_backend_registry_entry(name)
    if removed is None:
        raise RuntimeError(f"no such backend: {name}")
    cleared: dict[str, dict] = {}
    if removed.get("active"):
        token = read_token()
        consumers = [e for e in read_head_registry() if e["role"] in BACKEND_CONSUMER_ROLES]
        for c in consumers:
            try:
                resp = httpx.request(
                    "DELETE",
                    f"http://{c['ip']}:{AGENT_PORT}/backend",
                    headers={"x-saem-token": token},
                    timeout=timeout,
                )
                resp.raise_for_status()
                cleared[c["ip"]] = resp.json()
            except Exception as e:
                cleared[c["ip"]] = {"status": "unreachable", "error": str(e)}
    return {"unregistered": name, "was_active": bool(removed.get("active")), "cleared": cleared}


def backend_status() -> list[dict]:
    return read_backend_registry()
