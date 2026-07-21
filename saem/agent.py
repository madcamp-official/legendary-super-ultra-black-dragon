"""Long-running daemon on every node (`saem agent`). Waits for the head node
to POST a role assignment, then writes it and installs the systemd unit that
actually runs it. This is the only thing head needs network access to.
"""
from __future__ import annotations

from typing import Optional

import uvicorn
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from saem.common.config import AGENT_PORT
from saem.common.state import (
    clear_backend,
    clear_role,
    read_backend,
    read_role,
    read_token,
    write_backend,
    write_role,
)
from saem.roles import ROLE_ENTRYPOINTS
from saem.systemd import install_role_service, remove_role_service, restart_role_service

app = FastAPI(title="saem-agent")


class RoleAssignment(BaseModel):
    role: str
    port: Optional[int] = None


class BackendAssignment(BaseModel):
    name: str
    url: str
    model: str


def _check_token(token: Optional[str]) -> None:
    try:
        expected = read_token()
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    if token != expected:
        raise HTTPException(status_code=403, detail="bad token")


@app.post("/role")
def set_role(assignment: RoleAssignment, x_saem_token: Optional[str] = Header(None)):
    _check_token(x_saem_token)
    if assignment.role not in ROLE_ENTRYPOINTS:
        raise HTTPException(status_code=400, detail=f"unknown role: {assignment.role}")
    data = write_role(assignment.role, assignment.port)
    install_role_service(assignment.role, assignment.port)
    return {"status": "ok", **data}


@app.get("/role")
def get_role():
    return read_role() or {"role": None}


@app.delete("/role")
def delete_role(x_saem_token: Optional[str] = Header(None)):
    """Drop this node's role: stop and remove the unit, forget role.yaml.
    The agent itself keeps running, so head can re-assign a role later
    without anyone SSHing back in."""
    _check_token(x_saem_token)
    previous = read_role()
    remove_role_service()
    clear_role()
    return {"status": "ok", "previous": previous}


@app.post("/backend")
def set_backend(assignment: BackendAssignment, x_saem_token: Optional[str] = Header(None)):
    """head pushes this whenever it registers/switches the active dure backend
    on a node running retrieval_gateway or api_proxy."""
    _check_token(x_saem_token)
    data = write_backend(assignment.name, assignment.url, assignment.model)
    if read_role():
        restart_role_service()  # pick up the new backend without a manual bounce
    return {"status": "ok", **data}


@app.get("/backend")
def get_backend():
    return read_backend() or {"name": None}


@app.delete("/backend")
def delete_backend(x_saem_token: Optional[str] = Header(None)):
    """Forget the active backend. The role keeps running and falls back to
    the env-var default in get_llm_backend()."""
    _check_token(x_saem_token)
    previous = read_backend()
    clear_backend()
    if read_role():
        restart_role_service()
    return {"status": "ok", "previous": previous}


def run() -> None:
    uvicorn.run(app, host="0.0.0.0", port=AGENT_PORT)
