from __future__ import annotations

import os
from datetime import timedelta
from functools import partial
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from dure import __version__

from .db import Base, make_engine, make_session_factory, session_dependency
from .models import (
    Deployment,
    ModelArtifact,
    ModelRelease,
    Node,
    NodeProfileRecord,
    PlacementProfileRecord,
    RuntimeRelease,
    Task,
    TaskType,
    utcnow,
)
from .service import (
    authenticate_node,
    approve_node,
    cancel_task,
    claim_enrollment,
    claim_task,
    create_model_artifact,
    create_model_release,
    create_runtime_release,
    create_enrollment,
    create_tasks,
    extend_task,
    finish_task,
    join_node,
    node_status,
    revoke_node,
    rotate_node_credential,
    save_deployment,
    save_heartbeat,
    add_placement_profile,
    RegistryConflictError,
    transition_model_release,
)
from .recommendation import recommend_deployment


class StrictBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class EnrollmentCreate(BaseModel):
    expires_in_seconds: int = Field(default=3600, ge=60, le=604800)


class EnrollmentClaim(BaseModel):
    token: str
    install_id: str = Field(min_length=8, max_length=64)
    agent_version: str
    profile: dict


class NodeJoin(BaseModel):
    install_id: str = Field(min_length=8, max_length=64)
    agent_version: str
    profile: dict


class Heartbeat(BaseModel):
    state: dict
    profile: dict | None = None
    running_task_id: str | None = None


class DeploymentCreate(BaseModel):
    plan: dict
    accept_model_download: bool = False
    pull_image: bool = False


class TasksCreate(BaseModel):
    node_ids: list[str] = Field(min_length=1)
    type: TaskType
    deployment_id: str | None = None
    options: dict = Field(default_factory=dict)


class TaskComplete(BaseModel):
    result: dict = Field(default_factory=dict)


class TaskFail(BaseModel):
    error: str = Field(min_length=1, max_length=8192)


class ModelArtifactCreate(StrictBody):
    model_id: str
    repository: str
    revision: str
    manifest_digest: str
    quantization: str
    size_mib: int = Field(gt=0)
    default_max_model_len: int = Field(gt=0)
    layer_count: int = Field(gt=0)
    license_id: str


class RuntimeReleaseCreate(StrictBody):
    version: str
    image: str
    vllm_version: str
    cuda_version: str
    gpu_architectures: list[str] = Field(min_length=1)


class ModelReleaseCreate(StrictBody):
    artifact_id: str
    runtime_id: str
    quality_rank: int = Field(gt=0)


class PlacementProfileCreate(StrictBody):
    profile_id: str
    topology: str
    node_count: int = Field(gt=0)
    min_gpu_memory_mib: int = Field(gt=0)
    min_disk_free_mib: int = Field(gt=0)
    pipeline_parallel_size: int = Field(gt=0)
    tensor_parallel_size: int = Field(gt=0)
    requires_network_evidence: bool
    requires_nccl: bool
    min_bandwidth_mbps: int | None = None
    max_rtt_ms: float | None = None
    max_packet_loss_pct: float | None = None
    max_ttft_p95_ms: float = Field(gt=0)
    max_tpot_p95_ms: float = Field(gt=0)
    max_e2e_p95_ms: float = Field(gt=0)
    min_success_rate: float = Field(ge=0, le=1)
    min_vram_headroom_pct: float = Field(ge=0, le=100)
    min_throughput_tps: float = Field(gt=0)


class ModelReleaseTransition(StrictBody):
    status: str


class DeploymentRecommendationCreate(StrictBody):
    node_ids: list[str] = Field(default_factory=list, max_length=256)
    all_online: bool = False
    objective: Literal["quality-first"] = "quality-first"


def _bearer(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "bearer authentication required")
    return authorization[7:]


def _task_dict(task: Task) -> dict:
    return {
        "id": task.id,
        "bulk_id": task.bulk_id,
        "node_id": task.node_id,
        "type": task.type,
        "status": task.status,
        "deployment_id": task.deployment_id,
        "payload": task.payload,
        "attempts": task.attempts,
        "lease_until": task.lease_until,
        "result": task.result,
        "error": task.error,
    }


def _node_dict(node: Node, profile: NodeProfileRecord | None = None) -> dict:
    value = {
        "id": node.id,
        "display_name": node.display_name,
        "hostname": node.hostname,
        "agent_version": node.agent_version,
        "approved": node.approved,
        "connectivity": node_status(node.last_seen),
        "last_seen": node.last_seen,
        "phase": node.observed_phase,
        "role": node.observed_role,
        "deployment_id": node.observed_deployment_id,
        "desired_state": node.desired_state,
    }
    if profile is not None:
        value["profile"] = profile.profile
        value["profile_updated_at"] = profile.updated_at
    return value


def _artifact_dict(record: ModelArtifact) -> dict:
    return {
        "id": record.id,
        "model_id": record.model_id,
        "repository": record.repository,
        "revision": record.revision,
        "manifest_digest": record.manifest_digest,
        "quantization": record.quantization,
        "size_mib": record.size_mib,
        "default_max_model_len": record.default_max_model_len,
        "layer_count": record.layer_count,
        "license_id": record.license_id,
    }


def _runtime_release_dict(record: RuntimeRelease) -> dict:
    return {
        "id": record.id,
        "version": record.version,
        "image": record.image,
        "vllm_version": record.vllm_version,
        "cuda_version": record.cuda_version,
        "gpu_architectures": record.gpu_architectures,
    }


def _placement_dict(record: PlacementProfileRecord) -> dict:
    return {
        key: getattr(record, key)
        for key in (
            "id",
            "release_id",
            "profile_id",
            "topology",
            "node_count",
            "min_gpu_memory_mib",
            "min_disk_free_mib",
            "pipeline_parallel_size",
            "tensor_parallel_size",
            "requires_network_evidence",
            "requires_nccl",
            "min_bandwidth_mbps",
            "max_rtt_ms",
            "max_packet_loss_pct",
            "max_ttft_p95_ms",
            "max_tpot_p95_ms",
            "max_e2e_p95_ms",
            "min_success_rate",
            "min_vram_headroom_pct",
            "min_throughput_tps",
        )
    }


def _model_release_dict(session: Session, release: ModelRelease) -> dict:
    artifact = session.get(ModelArtifact, release.artifact_id)
    runtime = session.get(RuntimeRelease, release.runtime_id)
    placements = list(
        session.scalars(
            select(PlacementProfileRecord)
            .where(PlacementProfileRecord.release_id == release.id)
            .order_by(PlacementProfileRecord.profile_id)
        )
    )
    return {
        "id": release.id,
        "status": release.status,
        "quality_rank": release.quality_rank,
        "artifact": _artifact_dict(artifact),
        "runtime": _runtime_release_dict(runtime),
        "placements": [_placement_dict(item) for item in placements],
    }


def create_app(*, database_url: str | None = None, admin_token: str | None = None, create_schema: bool = False) -> FastAPI:
    engine = make_engine(database_url)
    if create_schema:
        Base.metadata.create_all(engine)
    factory = make_session_factory(engine)
    app = FastAPI(title="Dure Control Plane", version=__version__)
    app.state.session_factory = factory
    expected_admin = admin_token or os.environ.get("DURE_ADMIN_TOKEN")
    get_session = partial(session_dependency, factory)

    def admin_auth(authorization: str | None = Header(default=None)) -> str:
        supplied = _bearer(authorization)
        if not expected_admin or not __import__("hmac").compare_digest(supplied, expected_admin):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid admin credential")
        return "admin"

    def node_auth(
        authorization: str | None = Header(default=None),
        session: Session = Depends(get_session),
    ) -> Node:
        node = authenticate_node(session, _bearer(authorization))
        if node is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid node credential")
        return node

    @app.get("/health")
    def health():
        return {"ok": True, "version": __version__}

    @app.post("/v1/admin/enrollments", dependencies=[Depends(admin_auth)])
    def enrollment_create(body: EnrollmentCreate, session: Session = Depends(get_session)):
        record, raw = create_enrollment(session, timedelta(seconds=body.expires_in_seconds))
        return {"id": record.id, "token": raw, "expires_at": record.expires_at}

    @app.post("/v1/enrollments/claim")
    def enrollment_claim(body: EnrollmentClaim, session: Session = Depends(get_session)):
        try:
            node, credential = claim_enrollment(session, **body.model_dump())
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
        return {"node_id": node.id, "credential": credential}

    @app.post("/v1/nodes/join")
    def node_join(body: NodeJoin, session: Session = Depends(get_session)):
        try:
            node, credential = join_node(session, **body.model_dump())
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
        return {"node_id": node.id, "credential": credential, "status": "pending"}

    @app.post("/v1/agent/heartbeat")
    def heartbeat(body: Heartbeat, node: Node = Depends(node_auth), session: Session = Depends(get_session)):
        save_heartbeat(session, node, body.state, body.profile)
        return {"ok": True, "approved": node.approved}

    @app.post("/v1/agent/tasks/claim")
    def agent_claim(node: Node = Depends(node_auth), session: Session = Depends(get_session)):
        if not node.approved:
            return {"task": None, "status": "pending"}
        task = claim_task(session, node.id)
        return {"task": _task_dict(task) if task else None}

    @app.post("/v1/agent/tasks/{task_id}/heartbeat")
    def agent_task_heartbeat(task_id: str, node: Node = Depends(node_auth), session: Session = Depends(get_session)):
        task = session.get(Task, task_id)
        if task is None or not extend_task(session, task, node.id):
            raise HTTPException(status.HTTP_409_CONFLICT, "task cannot be extended")
        return {"ok": True, "lease_until": task.lease_until}

    @app.post("/v1/agent/tasks/{task_id}/complete")
    def agent_task_complete(task_id: str, body: TaskComplete, node: Node = Depends(node_auth), session: Session = Depends(get_session)):
        task = session.get(Task, task_id)
        if task is None or not finish_task(session, task, node.id, result=body.result, error=None):
            raise HTTPException(status.HTTP_409_CONFLICT, "task cannot be completed")
        return {"ok": True}

    @app.post("/v1/agent/tasks/{task_id}/fail")
    def agent_task_fail(task_id: str, body: TaskFail, node: Node = Depends(node_auth), session: Session = Depends(get_session)):
        task = session.get(Task, task_id)
        if task is None or not finish_task(session, task, node.id, result=None, error=body.error):
            raise HTTPException(status.HTTP_409_CONFLICT, "task cannot be failed")
        return {"ok": True}

    @app.get("/v1/admin/nodes", dependencies=[Depends(admin_auth)])
    def nodes(session: Session = Depends(get_session)):
        return {
            "nodes": [
                _node_dict(node) for node in session.scalars(select(Node).order_by(Node.display_name))
            ]
        }

    @app.get("/v1/admin/inventory", dependencies=[Depends(admin_auth)])
    def inventory(session: Session = Depends(get_session)):
        profiles = {
            profile.node_id: profile for profile in session.scalars(select(NodeProfileRecord))
        }
        return {
            "generated_at": utcnow(),
            "nodes": [
                _node_dict(node, profiles.get(node.id))
                for node in session.scalars(select(Node).order_by(Node.display_name))
            ],
        }

    @app.get("/v1/admin/nodes/{node_id}", dependencies=[Depends(admin_auth)])
    def node_detail(node_id: str, session: Session = Depends(get_session)):
        node = session.get(Node, node_id)
        if node is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "node not found")
        profile = session.get(NodeProfileRecord, node_id)
        value = _node_dict(node, profile)
        if profile is None:
            value["profile"] = None
            value["profile_updated_at"] = None
        return {"node": value}

    @app.post("/v1/admin/nodes/{node_id}/revoke", dependencies=[Depends(admin_auth)])
    def node_revoke(node_id: str, session: Session = Depends(get_session)):
        if not revoke_node(session, node_id):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "node not found")
        return {"ok": True}

    @app.post("/v1/admin/nodes/{node_id}/approve", dependencies=[Depends(admin_auth)])
    def node_approve(node_id: str, session: Session = Depends(get_session)):
        if not approve_node(session, node_id):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "node not found")
        return {"ok": True, "node_id": node_id, "status": "approved"}

    @app.post("/v1/admin/nodes/{node_id}/credential", dependencies=[Depends(admin_auth)])
    def node_credential_rotate(node_id: str, session: Session = Depends(get_session)):
        credential = rotate_node_credential(session, node_id)
        if credential is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "node not found")
        return {"node_id": node_id, "credential": credential}

    @app.post("/v1/admin/model-artifacts", dependencies=[Depends(admin_auth)])
    def model_artifact_create(
        body: ModelArtifactCreate, session: Session = Depends(get_session)
    ):
        try:
            record = create_model_artifact(session, **body.model_dump())
        except RegistryConflictError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
        return {"artifact": _artifact_dict(record)}

    @app.post("/v1/admin/runtime-releases", dependencies=[Depends(admin_auth)])
    def runtime_release_create(
        body: RuntimeReleaseCreate, session: Session = Depends(get_session)
    ):
        try:
            record = create_runtime_release(session, **body.model_dump())
        except RegistryConflictError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
        return {"runtime": _runtime_release_dict(record)}

    @app.post("/v1/admin/model-releases", dependencies=[Depends(admin_auth)])
    def model_release_create(
        body: ModelReleaseCreate, session: Session = Depends(get_session)
    ):
        try:
            record = create_model_release(session, **body.model_dump())
        except RegistryConflictError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
        return {"release": _model_release_dict(session, record)}

    @app.get("/v1/admin/model-releases", dependencies=[Depends(admin_auth)])
    def model_releases(session: Session = Depends(get_session)):
        releases = session.scalars(select(ModelRelease).order_by(ModelRelease.created_at))
        return {"releases": [_model_release_dict(session, item) for item in releases]}

    @app.get("/v1/admin/model-releases/{release_id}", dependencies=[Depends(admin_auth)])
    def model_release_detail(release_id: str, session: Session = Depends(get_session)):
        release = session.get(ModelRelease, release_id)
        if release is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "model release not found")
        return {"release": _model_release_dict(session, release)}

    @app.post(
        "/v1/admin/model-releases/{release_id}/placements",
        dependencies=[Depends(admin_auth)],
    )
    def model_release_placement_create(
        release_id: str,
        body: PlacementProfileCreate,
        session: Session = Depends(get_session),
    ):
        try:
            record = add_placement_profile(
                session, release_id=release_id, **body.model_dump()
            )
        except RegistryConflictError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
        return {"placement": _placement_dict(record)}

    @app.post(
        "/v1/admin/model-releases/{release_id}/transition",
        dependencies=[Depends(admin_auth)],
    )
    def model_release_transition(
        release_id: str,
        body: ModelReleaseTransition,
        session: Session = Depends(get_session),
    ):
        try:
            release = transition_model_release(session, release_id, body.status)
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
        return {"release": _model_release_dict(session, release)}

    @app.post(
        "/v1/admin/deployment-recommendations",
        dependencies=[Depends(admin_auth)],
    )
    def deployment_recommendation_create(
        body: DeploymentRecommendationCreate,
        session: Session = Depends(get_session),
    ):
        try:
            return recommend_deployment(session, **body.model_dump())
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc

    @app.post("/v1/admin/deployments", dependencies=[Depends(admin_auth)])
    def deployment_create(body: DeploymentCreate, session: Session = Depends(get_session)):
        try:
            deployment = save_deployment(
                session,
                body.plan,
                accept_model_download=body.accept_model_download,
                pull_image=body.pull_image,
            )
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
        return {"deployment": {"id": deployment.id, "generation": deployment.generation, "plan": deployment.plan}}

    @app.get("/v1/admin/deployments/{deployment_id}", dependencies=[Depends(admin_auth)])
    def deployment_detail(deployment_id: str, session: Session = Depends(get_session)):
        deployment = session.get(Deployment, deployment_id)
        if deployment is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "deployment not found")
        return {"deployment": {"id": deployment.id, "generation": deployment.generation, "status": deployment.status, "plan": deployment.plan}}

    @app.post("/v1/admin/tasks", dependencies=[Depends(admin_auth)])
    def tasks_create(body: TasksCreate, session: Session = Depends(get_session)):
        try:
            bulk_id, tasks, errors = create_tasks(
                session,
                node_ids=body.node_ids,
                task_type=body.type,
                deployment_id=body.deployment_id,
                options=body.options,
            )
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
        return {"bulk_id": bulk_id, "tasks": [_task_dict(item) for item in tasks], "errors": errors}

    @app.get("/v1/admin/tasks", dependencies=[Depends(admin_auth)])
    def tasks_list(session: Session = Depends(get_session)):
        return {"tasks": [_task_dict(item) for item in session.scalars(select(Task).order_by(Task.created_at.desc()).limit(200))]}

    @app.get("/v1/admin/tasks/{task_id}", dependencies=[Depends(admin_auth)])
    def task_detail(task_id: str, session: Session = Depends(get_session)):
        task = session.get(Task, task_id)
        if task is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "task not found")
        return {"task": _task_dict(task)}

    @app.post("/v1/admin/tasks/{task_id}/cancel", dependencies=[Depends(admin_auth)])
    def task_cancel(task_id: str, session: Session = Depends(get_session)):
        task = session.get(Task, task_id)
        if task is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "task not found")
        if not cancel_task(session, task):
            raise HTTPException(status.HTTP_409_CONFLICT, "only queued tasks can be canceled")
        return {"ok": True}

    return app
