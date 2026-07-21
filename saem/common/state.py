"""Local on-disk state shared by agent.py and head.py.

Every node keeps its own role assignment at ROLE_FILE. The head node
additionally keeps a registry of everything it has assigned at
HEAD_REGISTRY_FILE. Neither file needs to exist until first written.

LLM backends (GPU clusters running the separate `dure` package, e.g. the
235B cluster or a future camp1 head) are tracked the same way but as a
second, independent registry: BACKEND_REGISTRY_FILE on head lists every
known backend, and BACKEND_FILE on a consuming node (retrieval_gateway /
api_proxy) holds whichever one is currently active there. dure clusters
never need saem installed — head just remembers their URL.
"""
from __future__ import annotations

import datetime
import pathlib
from typing import Optional

import yaml

SAEM_DIR = pathlib.Path("/etc/saem")
ROLE_FILE = SAEM_DIR / "role.yaml"
HEAD_REGISTRY_FILE = SAEM_DIR / "head_registry.yaml"
BACKEND_REGISTRY_FILE = SAEM_DIR / "backend_registry.yaml"
BACKEND_FILE = SAEM_DIR / "backend.yaml"
TOKEN_FILE = SAEM_DIR / "token"

ROLE_CHOICES = [
    "qdrant_primary",
    "retrieval_gateway",
    "ingest_coordinator",
    "crawler",
    "api_proxy",
]


def _ensure_dir() -> None:
    SAEM_DIR.mkdir(parents=True, exist_ok=True)


def read_role() -> Optional[dict]:
    if not ROLE_FILE.exists():
        return None
    return yaml.safe_load(ROLE_FILE.read_text(encoding="utf-8"))


def write_role(role: str, port: Optional[int]) -> dict:
    _ensure_dir()
    data = {
        "role": role,
        "port": port,
        "registered_at": datetime.datetime.utcnow().isoformat() + "Z",
    }
    ROLE_FILE.write_text(yaml.safe_dump(data), encoding="utf-8")
    return data


def read_token() -> str:
    if not TOKEN_FILE.exists():
        raise FileNotFoundError(
            f"{TOKEN_FILE} not found. Copy the shared token to this node before "
            "registering it (see README)."
        )
    return TOKEN_FILE.read_text(encoding="utf-8").strip()


def read_head_registry() -> list[dict]:
    if not HEAD_REGISTRY_FILE.exists():
        return []
    return yaml.safe_load(HEAD_REGISTRY_FILE.read_text(encoding="utf-8")) or []


def write_head_registry(entries: list[dict]) -> None:
    _ensure_dir()
    HEAD_REGISTRY_FILE.write_text(yaml.safe_dump(entries), encoding="utf-8")


def upsert_head_registry_entry(ip: str, role: str, port: Optional[int]) -> None:
    entries = read_head_registry()
    entry = {
        "ip": ip,
        "role": role,
        "port": port,
        "registered_at": datetime.datetime.utcnow().isoformat() + "Z",
    }
    entries = [e for e in entries if e["ip"] != ip]
    entries.append(entry)
    write_head_registry(entries)


def remove_head_registry_entry(ip: str) -> bool:
    entries = read_head_registry()
    remaining = [e for e in entries if e["ip"] != ip]
    if len(remaining) == len(entries):
        return False
    write_head_registry(remaining)
    return True


def clear_role() -> bool:
    if not ROLE_FILE.exists():
        return False
    ROLE_FILE.unlink()
    return True


# --- LLM backend registry (head-side: every known dure cluster) ---


def read_backend_registry() -> list[dict]:
    if not BACKEND_REGISTRY_FILE.exists():
        return []
    return yaml.safe_load(BACKEND_REGISTRY_FILE.read_text(encoding="utf-8")) or []


def write_backend_registry(entries: list[dict]) -> None:
    _ensure_dir()
    BACKEND_REGISTRY_FILE.write_text(yaml.safe_dump(entries), encoding="utf-8")


def upsert_backend_registry_entry(name: str, url: str, model: str, active: bool) -> None:
    entries = read_backend_registry()
    if active:
        for e in entries:
            e["active"] = False
    entries = [e for e in entries if e["name"] != name]
    entries.append(
        {
            "name": name,
            "url": url,
            "model": model,
            "active": active,
            "registered_at": datetime.datetime.utcnow().isoformat() + "Z",
        }
    )
    write_backend_registry(entries)


# --- LLM backend, node-local (the one active backend this node calls) ---


def read_backend() -> Optional[dict]:
    if not BACKEND_FILE.exists():
        return None
    return yaml.safe_load(BACKEND_FILE.read_text(encoding="utf-8"))


def write_backend(name: str, url: str, model: str) -> dict:
    _ensure_dir()
    data = {"name": name, "url": url, "model": model}
    BACKEND_FILE.write_text(yaml.safe_dump(data), encoding="utf-8")
    return data


def clear_backend() -> bool:
    if not BACKEND_FILE.exists():
        return False
    BACKEND_FILE.unlink()
    return True


def remove_backend_registry_entry(name: str) -> Optional[dict]:
    """Returns the removed entry (so head can tell whether it was the active
    one and therefore needs clearing on the consumer nodes), or None."""
    entries = read_backend_registry()
    removed = next((e for e in entries if e["name"] == name), None)
    if removed is None:
        return None
    write_backend_registry([e for e in entries if e["name"] != name])
    return removed
