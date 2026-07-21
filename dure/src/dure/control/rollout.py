from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from ..model_cache import MODEL_CACHE_KIND_STAGE
from ..models import DeploymentPlan, VLLM_RAY_PP_BACKEND
from ..pipeline_runtime import (
    pipeline_contract_detail,
    validate_strict_pipeline_plan,
)
from ..task import TaskStatus, TaskType
from .models import (
    AuditEvent,
    Deployment,
    DeploymentOperation,
    DeploymentOperationNode,
    Node,
    Task,
    utcnow,
)
from .qualification import active_profile_qualification_nodes


ROLLOUT_AGENT_VERSION = (0, 3, 12)
STRICT_RAY_AGENT_VERSION = (0, 3, 18)
STAGE_ARTIFACT_AGENT_VERSION = (0, 3, 19)
ROLLBACK_NODE_PHASES = ("STOP_SOURCE", "START_TARGET", "VERIFY_TARGET")
ROLLBACK_API_PHASES = ("START_API", "VERIFY_API")
TERMINAL_TASK_STATUSES = {
    TaskStatus.SUCCEEDED.value,
    TaskStatus.FAILED.value,
    TaskStatus.CANCELED.value,
}
PHASE_TASK_TYPES = {
    "APPLY": TaskType.APPLY_DEPLOYMENT,
    "VERIFY": TaskType.VERIFY,
    "STOP_SOURCE": TaskType.STOP_DEPLOYMENT,
    "START_TARGET": TaskType.START_DEPLOYMENT,
    "VERIFY_TARGET": TaskType.VERIFY,
    "START_API": TaskType.START_DEPLOYMENT,
    "VERIFY_API": TaskType.VERIFY,
}
DEPLOYMENT_MUTATION_TASK_TYPES = {
    TaskType.APPLY_DEPLOYMENT.value,
    TaskType.START_DEPLOYMENT.value,
    TaskType.STOP_DEPLOYMENT.value,
    TaskType.RESTART_DEPLOYMENT.value,
    TaskType.PREPARE_MODEL.value,
    TaskType.PREPARE_IMAGE.value,
    TaskType.QUARANTINE_ARTIFACT_CACHE.value,
}
PHASE_ORDER = {
    phase: index
    for index, phase in enumerate(
        (
            "APPLY",
            "VERIFY",
            "STOP_SOURCE",
            "START_TARGET",
            "VERIFY_TARGET",
            "START_API",
            "VERIFY_API",
            "COMPLETE",
        )
    )
}


class DeploymentRolloutError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "DEPLOYMENT_ROLLOUT_INVALID",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


class DeploymentRolloutNotFoundError(DeploymentRolloutError):
    def __init__(self, message: str = "deployment generation not found") -> None:
        super().__init__(message, code="DEPLOYMENT_GENERATION_NOT_FOUND")


class DeploymentRolloutConflictError(DeploymentRolloutError):
    pass


def _aware(value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _iso(value: datetime | None) -> str | None:
    normalized = _aware(value)
    return normalized.isoformat() if normalized is not None else None


def _canonical_digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _canonical_node_ids(node_ids: list[str]) -> list[str]:
    if type(node_ids) is not list or not node_ids:
        raise DeploymentRolloutError(
            "node_ids must be a non-empty list",
            code="ROLLBACK_NODE_SET_INVALID",
        )
    normalized: list[str] = []
    for node_id in node_ids:
        try:
            if type(node_id) is not str or str(uuid.UUID(node_id)) != node_id:
                raise ValueError
        except (AttributeError, ValueError) as exc:
            raise DeploymentRolloutError(
                "node_ids must contain canonical server UUIDs",
                code="ROLLBACK_NODE_SET_INVALID",
            ) from exc
        normalized.append(node_id)
    if len(normalized) != len(set(normalized)):
        raise DeploymentRolloutError(
            "node_ids must not contain duplicates",
            code="ROLLBACK_NODE_SET_INVALID",
        )
    return sorted(normalized)


def _agent_version(value: str) -> tuple[int, int, int] | None:
    if type(value) is not str:
        return None
    matched = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z.-]+)?", value)
    if matched is None:
        return None
    return tuple(int(part) for part in matched.groups())  # type: ignore[return-value]


def _supports_rollout(node: Node) -> bool:
    version = _agent_version(node.agent_version)
    return version is not None and version >= ROLLOUT_AGENT_VERSION


def _supports_strict_ray(node: Node) -> bool:
    version = _agent_version(node.agent_version)
    return version is not None and version >= STRICT_RAY_AGENT_VERSION


def _supports_stage_artifact(node: Node) -> bool:
    version = _agent_version(node.agent_version)
    return version is not None and version >= STAGE_ARTIFACT_AGENT_VERSION


def _node_is_online(node: Node, now: datetime) -> bool:
    seen = _aware(node.last_seen)
    return seen is not None and now - seen <= timedelta(seconds=30)


def _require_digest_image(plan: dict[str, Any], field: str) -> None:
    image = plan.get("image")
    if (
        type(image) is not str
        or re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}", image
        )
        is None
    ):
        raise DeploymentRolloutError(
            f"{field} image must be OCI digest-pinned",
            code="ROLLBACK_IMAGE_NOT_PINNED",
        )


def _plan_assignments(
    deployment: Deployment,
) -> tuple[list[str], tuple[tuple[Any, ...], ...], tuple[Any, ...]]:
    plan = deployment.plan
    if type(plan) is not dict:
        raise DeploymentRolloutError(
            "deployment plan is invalid", code="ROLLBACK_PLAN_INVALID"
        )
    if (
        plan.get("deployment_id") != deployment.id
        or type(plan.get("generation")) is not int
        or plan.get("generation") != deployment.generation
    ):
        raise DeploymentRolloutError(
            "deployment plan identity does not match its generation",
            code="ROLLBACK_PLAN_IDENTITY_MISMATCH",
        )
    _require_digest_image(plan, "deployment")
    strict_ray = plan.get("execution_backend") == VLLM_RAY_PP_BACKEND
    if "execution_backend" in plan:
        if not strict_ray:
            raise DeploymentRolloutError(
                "deployment execution backend is not supported",
                code="ROLLBACK_PLAN_INVALID",
            )
        try:
            strict_plan = DeploymentPlan.from_dict(plan)
            validate_strict_pipeline_plan(
                strict_plan, require_manifest_cache_path=False
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise DeploymentRolloutError(
                "strict Ray deployment contract is invalid",
                code="ROLLBACK_PLAN_INVALID",
            ) from exc
    assignments = plan.get("assignments")
    if type(assignments) is not list or not assignments:
        raise DeploymentRolloutError(
            "deployment plan assignments are invalid",
            code="ROLLBACK_PLAN_INVALID",
        )
    signature: list[tuple[Any, ...]] = []
    node_ids: list[str] = []
    assignment_topology_fields = (
        "node_id",
        "gpu_index",
        "rank",
        "pipeline_rank",
        "role",
    )
    if strict_ray:
        assignment_topology_fields += (
            "expected_runtime_rank",
            "runtime_address",
        )
    else:
        # Legacy plans use the declared layer range as part of their runtime
        # partition.  The strict vLLM backend validates each generation's
        # model-specific range independently; across generations only the
        # node/rank/runtime placement is topology.
        assignment_topology_fields += (
            "layer_start",
            "layer_end",
        )
    for assignment in assignments:
        if type(assignment) is not dict or any(
            field not in assignment for field in assignment_topology_fields
        ):
            raise DeploymentRolloutError(
                "deployment assignment topology is invalid",
                code="ROLLBACK_PLAN_INVALID",
            )
        node_id = assignment["node_id"]
        if type(node_id) is not str:
            raise DeploymentRolloutError(
                "deployment assignment node identity is invalid",
                code="ROLLBACK_PLAN_INVALID",
            )
        node_ids.append(node_id)
        # Compare only the host/runtime topology across generations.  Model
        # and STAGE identities (for example rank manifest and tensor-key
        # digests) belong to the independently validated target artifact and
        # must be allowed to differ during rollback.
        signature.append(
            (
                node_id,
                json.dumps(
                    {
                        field: assignment[field]
                        for field in assignment_topology_fields
                    }
                    | (
                        {"gpu_uuid": assignment["gpu_uuid"]}
                        if "gpu_uuid" in assignment
                        else {}
                    ),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ),
            )
        )
    if len(node_ids) != len(set(node_ids)):
        raise DeploymentRolloutError(
            "deployment assignments contain duplicate nodes",
            code="ROLLBACK_PLAN_INVALID",
        )
    topology_fields = (
        "pipeline_parallel_size",
        "tensor_parallel_size",
        "ray_head_node_id",
        "ray_head_address",
        "network_interface",
    )
    if strict_ray:
        # Cache delivery kind is an artifact contract, not a runtime topology
        # coordinate.  Each plan validates it independently and rollback gates
        # the target's exact READY cache before any target start.
        topology_fields += (
            "execution_backend",
            "runtime_vllm_version",
        )
    if any(field not in plan for field in topology_fields):
        raise DeploymentRolloutError(
            "deployment topology is incomplete", code="ROLLBACK_PLAN_INVALID"
        )
    topology = tuple(plan[field] for field in topology_fields)
    return sorted(node_ids), tuple(sorted(signature)), topology


def _task_projection(task: Task) -> dict[str, Any]:
    return {
        "id": task.id,
        "type": task.type,
        "status": task.status,
        "node_id": task.node_id,
        "operation_attempt": task.operation_attempt,
        "attempts": task.attempts,
        "created_at": _iso(task.created_at),
        "updated_at": _iso(task.updated_at),
    }


def deployment_operation_detail(
    session: Session, operation: DeploymentOperation
) -> dict[str, Any]:
    operation_nodes = list(
        session.scalars(
            select(DeploymentOperationNode)
            .where(DeploymentOperationNode.operation_id == operation.id)
        )
    )
    operation_nodes.sort(
        key=lambda item: (PHASE_ORDER.get(item.phase, 999), item.node_id)
    )
    tasks_by_node: dict[str, list[Task]] = {}
    node_record_ids = [item.id for item in operation_nodes]
    if node_record_ids:
        for task in session.scalars(
            select(Task)
            .where(Task.operation_node_id.in_(node_record_ids))
            .order_by(Task.created_at, Task.id)
        ):
            tasks_by_node.setdefault(task.operation_node_id or "", []).append(task)
    return {
        "id": operation.id,
        "request_digest": operation.request_digest,
        "lineage_id": operation.lineage_id,
        "deployment_id": operation.deployment_id,
        "rollback_target_id": operation.rollback_target_id,
        "kind": operation.kind,
        "status": operation.status,
        "phase": operation.phase,
        "node_ids": list(operation.node_ids),
        "serve": operation.serve,
        "api": operation.api,
        "active": operation.active_lineage_id is not None,
        "created_at": _iso(operation.created_at),
        "updated_at": _iso(operation.updated_at),
        "completed_at": _iso(operation.completed_at),
        "nodes": [
            {
                "id": item.id,
                "node_id": item.node_id,
                "phase": item.phase,
                "status": item.status,
                "attempt_count": item.attempt_count,
                "failure_code": item.failure_code,
                "created_at": _iso(item.created_at),
                "updated_at": _iso(item.updated_at),
                "completed_at": _iso(item.completed_at),
                "tasks": [
                    _task_projection(task)
                    for task in tasks_by_node.get(item.id, [])
                ],
            }
            for item in operation_nodes
        ],
    }


def deployment_generation_detail(
    session: Session, deployment_id: str
) -> dict[str, Any]:
    deployment = session.get(Deployment, deployment_id)
    if deployment is None:
        raise DeploymentRolloutNotFoundError()
    operations = list(
        session.scalars(
            select(DeploymentOperation)
            .where(
                or_(
                    DeploymentOperation.deployment_id == deployment.id,
                    DeploymentOperation.rollback_target_id == deployment.id,
                )
            )
            .order_by(DeploymentOperation.created_at, DeploymentOperation.id)
        )
    )
    own_operations = [item for item in operations if item.deployment_id == deployment.id]

    def latest(kind: str) -> str:
        matching = [item for item in own_operations if item.kind == kind]
        return matching[-1].status if matching else "NOT_REQUESTED"

    return {
        "id": deployment.id,
        "lineage_id": deployment.lineage_id,
        "generation": deployment.generation,
        "previous_generation_id": deployment.previous_generation_id,
        "source_recommendation_id": deployment.source_recommendation_id,
        "status": deployment.status,
        "verified_at": _iso(deployment.verified_at),
        "rollback_eligible": deployment.verified_at is not None,
        "apply_status": latest("APPLY"),
        "verify_status": latest("VERIFY"),
        "plan": deployment.plan,
        "accept_model_download": deployment.accept_model_download,
        "pull_image": deployment.pull_image,
        "created_at": _iso(deployment.created_at),
        "operations": [
            deployment_operation_detail(session, operation)
            for operation in operations
        ],
    }


def deployment_lineage_generations(
    session: Session, deployment_id: str
) -> list[dict[str, Any]]:
    deployment = session.get(Deployment, deployment_id)
    if deployment is None:
        raise DeploymentRolloutNotFoundError()
    lineage_id = deployment.lineage_id or deployment.id
    generations = list(
        session.scalars(
            select(Deployment)
            .where(Deployment.lineage_id == lineage_id)
            .order_by(Deployment.generation, Deployment.id)
        )
    )
    return [deployment_generation_detail(session, item.id) for item in generations]


def _operation_audit(
    session: Session,
    action: str,
    operation: DeploymentOperation,
    outcome: str,
    **detail: Any,
) -> None:
    session.add(
        AuditEvent(
            actor="admin" if action.endswith("prepare") else "controller",
            action=action,
            target=operation.id,
            outcome=outcome,
            detail=detail,
        )
    )


def _phase_payload(
    session: Session,
    operation: DeploymentOperation,
    phase: str,
    source: Deployment,
    target: Deployment | None,
) -> tuple[Deployment, dict[str, Any]]:
    if target is None or phase in {"APPLY", "VERIFY", "STOP_SOURCE"}:
        deployment = source
    else:
        if target is None:
            raise DeploymentRolloutError(
                "rollback target is missing", code="ROLLBACK_TARGET_NOT_FOUND"
            )
        deployment = target
    plan = deployment.plan
    if deployment.source_recommendation_id is not None:
        from .preparation import effective_deployment_plan

        plan = effective_deployment_plan(
            session,
            deployment,
            require_prepared=phase != "STOP_SOURCE",
        )
    payload: dict[str, Any] = {
        "plan": plan,
        "generation": deployment.generation,
    }
    if phase == "APPLY":
        # API serving is staged on the head only after every Ray node reports
        # successful APPLY completion. This avoids a head racing its workers.
        payload["serve"] = False
        payload["accept_model_download"] = bool(
            deployment.accept_model_download
            if deployment.source_recommendation_id is None
            else False
        )
        payload["pull_image"] = bool(
            deployment.pull_image
            if deployment.source_recommendation_id is None
            else False
        )
    elif phase == "START_TARGET":
        payload["serve"] = False
    elif phase == "START_API":
        payload["serve"] = True
    elif phase == "VERIFY":
        payload["api"] = bool(operation.api)
    elif phase == "VERIFY_TARGET":
        payload["api"] = False
    elif phase == "VERIFY_API":
        payload["api"] = True
    return deployment, payload


def _phase_nodes(
    session: Session,
    operation: DeploymentOperation,
    phase: str | None = None,
    *,
    lock: bool = False,
) -> list[DeploymentOperationNode]:
    statement = select(DeploymentOperationNode).where(
        DeploymentOperationNode.operation_id == operation.id
    )
    if phase is not None:
        statement = statement.where(DeploymentOperationNode.phase == phase)
    statement = statement.order_by(
        DeploymentOperationNode.phase,
        DeploymentOperationNode.node_id,
        DeploymentOperationNode.id,
    )
    if lock:
        statement = statement.with_for_update()
    return list(session.scalars(statement))


def _queue_phase(
    session: Session,
    operation: DeploymentOperation,
    phase: str,
    *,
    retry_failed_only: bool = False,
    update_node_state: bool = False,
) -> list[Task]:
    source = session.get(Deployment, operation.deployment_id)
    target = (
        session.get(Deployment, operation.rollback_target_id)
        if operation.rollback_target_id
        else None
    )
    if source is None:
        raise DeploymentRolloutNotFoundError()
    deployment, payload = _phase_payload(
        session, operation, phase, source, target
    )
    records = _phase_nodes(session, operation, phase, lock=True)
    if retry_failed_only:
        records = [item for item in records if item.status in {"FAILED", "CANCELED"}]
    elif any(item.status != "PENDING" for item in records):
        raise DeploymentRolloutConflictError(
            "operation phase was already queued",
            code="ROLLOUT_PHASE_ALREADY_QUEUED",
        )
    tasks: list[Task] = []
    task_type = PHASE_TASK_TYPES[phase]
    now = utcnow()
    for record in records:
        record.attempt_count += 1
        record.status = "QUEUED"
        record.failure_code = None
        record.updated_at = now
        record.completed_at = None
        task = Task(
            bulk_id=operation.id,
            node_id=record.node_id,
            type=task_type.value,
            deployment_id=deployment.id,
            operation_node_id=record.id,
            operation_attempt=record.attempt_count,
            payload=dict(payload),
        )
        session.add(task)
        tasks.append(task)
        if update_node_state:
            node = session.get(Node, record.node_id)
            if node is not None:
                node.desired_state = task_type.value
    if tasks:
        operation.phase = phase
        operation.status = "QUEUED"
        operation.updated_at = now
    return tasks


def _rollback_request_digest(
    source: Deployment,
    target: Deployment,
    node_ids: list[str],
    *,
    serve: bool,
) -> str:
    return _canonical_digest(
        {
            "kind": "ROLLBACK",
            "lineage_id": source.lineage_id,
            "source": {
                "id": source.id,
                "generation": source.generation,
                "plan": source.plan,
            },
            "target": {
                "id": target.id,
                "generation": target.generation,
                "plan": target.plan,
            },
            "node_ids": node_ids,
            "serve": serve,
            "api": False,
        }
    )


def _validate_rollback_nodes(
    session: Session, node_ids: list[str], *, now: datetime
) -> None:
    for node_id in node_ids:
        # The lineage root serializes operation producers. Nodes are read as a
        # snapshot here so a dry prepare cannot invert the completion path's
        # Node -> Task -> Operation -> Deployment lock order.
        node = session.get(Node, node_id)
        if node is None or not node.approved:
            raise DeploymentRolloutError(
                "rollback requires approved assigned nodes",
                code="ROLLBACK_NODE_NOT_APPROVED",
                details={"node_id": node_id},
            )
        if not _node_is_online(node, now):
            raise DeploymentRolloutError(
                "rollback requires every assigned node to be online",
                code="ROLLBACK_NODE_OFFLINE",
                details={"node_id": node_id},
            )
        if not _supports_rollout(node):
            raise DeploymentRolloutError(
                "rollback requires a generation-aware Agent",
                code="ROLLBACK_AGENT_TOO_OLD",
                details={"node_id": node_id, "agent_version": node.agent_version},
            )


def _validate_rollback(
    session: Session,
    source_id: str,
    node_ids: list[str],
) -> tuple[Deployment, Deployment, str]:
    identity = session.execute(
        select(Deployment.id, Deployment.lineage_id).where(
            Deployment.id == source_id
        )
    ).one_or_none()
    if identity is None:
        raise DeploymentRolloutNotFoundError()
    lineage_id = identity.lineage_id or identity.id
    root = session.scalar(
        select(Deployment)
        .where(Deployment.id == lineage_id)
        .with_for_update()
    )
    if root is None or (root.lineage_id or root.id) != lineage_id:
        raise DeploymentRolloutError(
            "deployment lineage root is invalid",
            code="ROLLBACK_LINEAGE_INVALID",
        )
    generations = list(
        session.scalars(
            select(Deployment)
            .where(Deployment.lineage_id == lineage_id)
            .order_by(Deployment.generation, Deployment.id)
            .with_for_update()
        )
    )
    source = next((item for item in generations if item.id == source_id), None)
    if source is None:
        raise DeploymentRolloutNotFoundError()
    latest = generations[-1] if generations else None
    if latest is None or latest.id != source.id:
        raise DeploymentRolloutConflictError(
            "rollback source must be the latest lineage generation",
            code="ROLLBACK_SOURCE_NOT_LATEST",
            details={"latest_generation_id": latest.id if latest else None},
        )
    if source.previous_generation_id is None:
        raise DeploymentRolloutError(
            "rollback source has no direct previous generation",
            code="ROLLBACK_TARGET_NOT_FOUND",
        )
    target = next(
        (
            item
            for item in generations
            if item.id == source.previous_generation_id
        ),
        None,
    )
    if target is None or target.lineage_id != lineage_id:
        raise DeploymentRolloutError(
            "direct rollback target is not in the source lineage",
            code="ROLLBACK_TARGET_INVALID",
        )
    if target.verified_at is None:
        raise DeploymentRolloutError(
            "rollback target has no full verification evidence",
            code="ROLLBACK_TARGET_NOT_VERIFIED",
        )
    source_nodes, source_assignments, source_topology = _plan_assignments(source)
    target_nodes, target_assignments, target_topology = _plan_assignments(target)
    if (
        source_nodes != target_nodes
        or source_assignments != target_assignments
        or source_topology != target_topology
    ):
        raise DeploymentRolloutError(
            "rollback requires identical assignment and topology",
            code="ROLLBACK_TOPOLOGY_UNSUPPORTED",
        )
    if node_ids != source_nodes:
        raise DeploymentRolloutError(
            "rollback node_ids must exactly match every assigned node",
            code="ROLLBACK_NODE_SET_MISMATCH",
            details={"expected_node_ids": source_nodes},
        )
    _validate_rollback_nodes(session, node_ids, now=utcnow())
    if (
        source.plan.get("execution_backend") == VLLM_RAY_PP_BACKEND
        or target.plan.get("execution_backend") == VLLM_RAY_PP_BACKEND
    ):
        for node_id in node_ids:
            node = session.get(Node, node_id)
            if node is None or not _supports_strict_ray(node):
                raise DeploymentRolloutError(
                    "strict Ray rollback requires Dure Agent 0.3.18 or newer",
                    code="ROLLBACK_AGENT_TOO_OLD",
                    details={
                        "node_id": node_id,
                        "agent_version": node.agent_version if node else None,
                    },
                )
    return source, target, lineage_id


def _activate_operation(
    session: Session, operation: DeploymentOperation
) -> None:
    if operation.active_lineage_id == operation.lineage_id:
        return
    active = next(
        (
            item
            for item in session.scalars(
                select(DeploymentOperation).where(
                    DeploymentOperation.active_lineage_id.is_not(None),
                    DeploymentOperation.id != operation.id,
                )
            )
            if set(item.node_ids).intersection(operation.node_ids)
        ),
        None,
    )
    if active is not None:
        same_lineage = active.lineage_id == operation.lineage_id
        raise DeploymentRolloutConflictError(
            (
                "deployment lineage already has an active operation"
                if same_lineage
                else "deployment nodes already belong to an active operation"
            ),
            code=(
                "DEPLOYMENT_OPERATION_ACTIVE"
                if same_lineage
                else "DEPLOYMENT_NODE_OPERATION_ACTIVE"
            ),
            details={
                "operation_id": active.id,
                "node_ids": sorted(
                    set(active.node_ids).intersection(operation.node_ids)
                ),
            },
        )
    active_task_id = session.scalar(
        select(Task.id)
        .where(
            Task.node_id.in_(operation.node_ids),
            Task.type.in_(DEPLOYMENT_MUTATION_TASK_TYPES),
            Task.status.in_(
                {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
            ),
        )
        .order_by(Task.created_at, Task.id)
        .limit(1)
    )
    if active_task_id is not None:
        raise DeploymentRolloutConflictError(
            "deployment lineage already has a queued or running mutation",
            code="DEPLOYMENT_MUTATION_ACTIVE",
            details={"task_id": active_task_id},
        )
    active_qualifications = active_profile_qualification_nodes(
        session, operation.node_ids
    )
    if active_qualifications:
        overlap = sorted(active_qualifications)
        raise DeploymentRolloutConflictError(
            "deployment nodes belong to active profile qualification runs",
            code="DEPLOYMENT_NODE_QUALIFICATION_ACTIVE",
            details={
                "node_ids": overlap,
                "qualification_run_ids": sorted(
                    {active_qualifications[node_id] for node_id in overlap}
                ),
            },
        )
    operation.active_lineage_id = operation.lineage_id
    operation.updated_at = utcnow()


def prepare_or_apply_rollback(
    session: Session,
    source_id: str,
    node_ids: list[str],
    apply: bool,
    serve: bool,
) -> tuple[DeploymentOperation, list[Task], bool]:
    """Prepare a rollback, or explicitly start/retry its current phase.

    The returned boolean is true only when this call persisted a new operation or
    queued a new task attempt. The request digest intentionally excludes
    ``apply`` so a dry preparation and its later explicit application share one
    immutable operation.
    """
    if type(apply) is not bool or type(serve) is not bool:
        raise DeploymentRolloutError(
            "apply and serve must be strict booleans",
            code="ROLLBACK_REQUEST_INVALID",
        )
    normalized_node_ids = _canonical_node_ids(node_ids)
    if apply:
        # Mutation producers lock the complete node set before the lineage
        # root. Generic task creation follows the same global order.
        list(
            session.scalars(
                select(Node)
                .where(Node.id.in_(normalized_node_ids))
                .order_by(Node.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
    source, target, lineage_id = _validate_rollback(
        session, source_id, normalized_node_ids
    )
    strict_rollback = (
        source.plan.get("execution_backend") == VLLM_RAY_PP_BACKEND
        or target.plan.get("execution_backend") == VLLM_RAY_PP_BACKEND
    )
    if strict_rollback and not serve:
        raise DeploymentRolloutError(
            "strict Ray rollback requires API start and actor attestation",
            code="ROLLBACK_STRICT_API_ATTESTATION_REQUIRED",
        )
    from .preparation import effective_deployment_plan

    # Rollback is deliberately network-free. A recommended target must already
    # have a fully successful exact preparation record; these calls only read
    # immutable evidence and never queue preparation. STOP_SOURCE deliberately
    # retains a revoked source's stored effective plan for exact containment.
    effective_source = effective_deployment_plan(
        session, source, require_prepared=False
    )
    effective_target = effective_deployment_plan(
        session,
        target,
        require_prepared=True,
        lock_ready_caches=apply,
    )
    if any(
        plan.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE
        for plan in (effective_source, effective_target)
    ):
        unsupported = sorted(
            node_id
            for node_id in normalized_node_ids
            if (node := session.get(Node, node_id)) is None
            or not _supports_stage_artifact(node)
        )
        if unsupported:
            raise DeploymentRolloutError(
                "stage artifact rollback requires Dure Agent 0.3.19 or newer",
                code="ROLLBACK_STAGE_AGENT_TOO_OLD",
                details={"node_ids": unsupported},
            )
    digest = _rollback_request_digest(
        source, target, normalized_node_ids, serve=serve
    )
    operation_query = select(DeploymentOperation).where(
        DeploymentOperation.request_digest == digest
    )
    if apply:
        operation_query = operation_query.with_for_update()
    operation = session.scalar(operation_query)
    created = False
    if operation is None:
        if target.status != "VERIFIED":
            raise DeploymentRolloutError(
                "rollback target is not currently verified",
                code="ROLLBACK_TARGET_NOT_VERIFIED",
                details={"target_status": target.status},
            )
        operation = DeploymentOperation(
            id=str(uuid.uuid4()),
            request_digest=digest,
            lineage_id=lineage_id,
            deployment_id=source.id,
            rollback_target_id=target.id,
            kind="ROLLBACK",
            status="PREPARED",
            phase="STOP_SOURCE",
            node_ids=normalized_node_ids,
            serve=serve,
            api=False,
            active_lineage_id=None,
        )
        session.add(operation)
        for phase in ROLLBACK_NODE_PHASES:
            for node_id in normalized_node_ids:
                session.add(
                    DeploymentOperationNode(
                        id=str(uuid.uuid4()),
                        operation_id=operation.id,
                        node_id=node_id,
                        phase=phase,
                        status="PENDING",
                        attempt_count=0,
                    )
                )
        if serve:
            head_node_id = target.plan["ray_head_node_id"]
            for phase in ROLLBACK_API_PHASES:
                session.add(
                    DeploymentOperationNode(
                        id=str(uuid.uuid4()),
                        operation_id=operation.id,
                        node_id=head_node_id,
                        phase=phase,
                        status="PENDING",
                        attempt_count=0,
                    )
                )
        _operation_audit(
            session,
            "deployment_operation.prepare",
            operation,
            "success",
            kind="ROLLBACK",
            source_deployment_id=source.id,
            target_deployment_id=target.id,
            node_ids=normalized_node_ids,
        )
        session.flush()
        created = True
    elif (
        operation.kind != "ROLLBACK"
        or operation.deployment_id != source.id
        or operation.rollback_target_id != target.id
        or operation.node_ids != normalized_node_ids
        or operation.serve is not serve
    ):
        raise DeploymentRolloutConflictError(
            "rollback digest is bound to different immutable inputs",
            code="ROLLBACK_REQUEST_CONFLICT",
        )

    if source.status == "ROLLED_BACK":
        if operation.status == "SUCCEEDED":
            session.commit()
            return operation, [], False
        raise DeploymentRolloutConflictError(
            "rollback source was already rolled back by another request",
            code="ROLLBACK_SOURCE_ALREADY_ROLLED_BACK",
            details={"source_deployment_id": source.id},
        )

    tasks: list[Task] = []
    changed = created
    if apply:
        if operation.status == "PREPARED":
            _activate_operation(session, operation)
            tasks = _queue_phase(
                session,
                operation,
                "STOP_SOURCE",
                update_node_state=True,
            )
        elif operation.status in {"PARTIAL_FAILED", "FAILED"}:
            _activate_operation(session, operation)
            current = _phase_nodes(
                session, operation, operation.phase, lock=True
            )
            if any(item.status in {"QUEUED", "RUNNING"} for item in current):
                raise DeploymentRolloutConflictError(
                    "rollback phase still has in-flight tasks",
                    code="ROLLBACK_PHASE_IN_PROGRESS",
                )
            tasks = _queue_phase(
                session,
                operation,
                operation.phase,
                retry_failed_only=True,
                update_node_state=True,
            )
        if tasks:
            source.status = "ROLLING_BACK"
            target.status = "ROLLBACK_TARGET_PENDING"
            changed = True
    session.commit()
    return operation, tasks, changed


def _generic_operation_digest(
    deployment: Deployment,
    task_type: TaskType,
    tasks: list[Task],
    options: dict[str, Any],
) -> str:
    bulk_ids = sorted({task.bulk_id for task in tasks})
    return _canonical_digest(
        {
            "kind": "APPLY" if task_type == TaskType.APPLY_DEPLOYMENT else "VERIFY",
            "bulk_ids": bulk_ids,
            "deployment_id": deployment.id,
            "generation": deployment.generation,
            "node_ids": sorted(task.node_id for task in tasks),
            "options": options,
        }
    )


def attach_deployment_bulk_operation(
    session: Session,
    *,
    deployment: Deployment,
    task_type: TaskType,
    tasks: list[Task],
    options: dict[str, Any],
) -> DeploymentOperation | None:
    """Attach an existing generic APPLY/VERIFY bulk before its transaction commits."""
    if task_type not in {TaskType.APPLY_DEPLOYMENT, TaskType.VERIFY}:
        return None
    if not tasks:
        return None
    allowed = {"serve"} if task_type == TaskType.APPLY_DEPLOYMENT else {"api"}
    if type(options) is not dict or set(options) - allowed:
        raise DeploymentRolloutError(
            "deployment operation options are not in the closed schema",
            code="DEPLOYMENT_OPERATION_OPTIONS_INVALID",
        )
    normalized_options = {field: False for field in allowed}
    for field, value in options.items():
        if type(value) is not bool:
            raise DeploymentRolloutError(
                "deployment operation options must be strict booleans",
                code="DEPLOYMENT_OPERATION_OPTIONS_INVALID",
            )
        normalized_options[field] = value
    node_ids = sorted(task.node_id for task in tasks)
    if len(node_ids) != len(set(node_ids)):
        raise DeploymentRolloutError(
            "deployment operation tasks must have unique nodes",
            code="DEPLOYMENT_OPERATION_NODE_DUPLICATE",
        )
    lineage_id = deployment.lineage_id or deployment.id
    root = session.scalar(
        select(Deployment)
        .where(Deployment.id == lineage_id)
        .with_for_update()
    )
    if root is None or (root.lineage_id or root.id) != lineage_id:
        raise DeploymentRolloutError(
            "deployment lineage root is invalid",
            code="DEPLOYMENT_LINEAGE_INVALID",
        )
    locked_deployment = session.scalar(
        select(Deployment)
        .where(Deployment.id == deployment.id)
        .with_for_update()
    )
    if locked_deployment is None or locked_deployment.lineage_id != lineage_id:
        raise DeploymentRolloutError(
            "deployment generation is not in its lineage",
            code="DEPLOYMENT_LINEAGE_INVALID",
        )
    deployment = locked_deployment
    expected_nodes, _, _ = _plan_assignments(deployment)
    if deployment.plan.get("execution_backend") == VLLM_RAY_PP_BACKEND:
        required_option = "serve" if task_type == TaskType.APPLY_DEPLOYMENT else "api"
        if normalized_options.get(required_option) is not True:
            raise DeploymentRolloutError(
                "strict Ray operations require live vLLM API verification",
                code="DEPLOYMENT_STRICT_RUNTIME_ATTESTATION_REQUIRED",
            )
    if any(node_id not in expected_nodes for node_id in node_ids):
        raise DeploymentRolloutError(
            "deployment operation contains an unassigned node",
            code="DEPLOYMENT_OPERATION_NODE_INVALID",
        )
    if normalized_options.get("serve") and node_ids != expected_nodes:
        raise DeploymentRolloutError(
            "serving apply requires the complete assigned node set",
            code="DEPLOYMENT_OPERATION_NODE_SET_MISMATCH",
            details={"expected_node_ids": expected_nodes},
        )
    digest = _generic_operation_digest(
        deployment, task_type, tasks, normalized_options
    )
    existing = session.scalar(
        select(DeploymentOperation).where(
            DeploymentOperation.request_digest == digest
        )
    )
    if existing is not None:
        return existing
    active = session.scalar(
        select(DeploymentOperation)
        .where(DeploymentOperation.active_lineage_id == lineage_id)
    )
    if active is not None:
        raise DeploymentRolloutConflictError(
            "deployment lineage already has an active mutation",
            code="DEPLOYMENT_OPERATION_ACTIVE",
            details={"operation_id": active.id},
        )
    kind = "APPLY" if task_type == TaskType.APPLY_DEPLOYMENT else "VERIFY"
    phase = kind
    operation = DeploymentOperation(
        id=str(uuid.uuid4()),
        request_digest=digest,
        lineage_id=lineage_id,
        deployment_id=deployment.id,
        rollback_target_id=None,
        kind=kind,
        status="QUEUED",
        phase=phase,
        node_ids=node_ids,
        serve=normalized_options.get("serve", False),
        api=normalized_options.get("api", False),
        active_lineage_id=lineage_id,
    )
    session.add(operation)
    task_by_node = {task.node_id: task for task in tasks}
    _, payload = _phase_payload(
        session, operation, phase, deployment, None
    )
    for node_id in node_ids:
        record = DeploymentOperationNode(
            id=str(uuid.uuid4()),
            operation_id=operation.id,
            node_id=node_id,
            phase=phase,
            status="QUEUED",
            attempt_count=1,
        )
        session.add(record)
        task = task_by_node[node_id]
        if task.type != task_type.value or task.deployment_id != deployment.id:
            raise DeploymentRolloutError(
                "generic task does not match its deployment operation",
                code="DEPLOYMENT_OPERATION_TASK_INVALID",
            )
        task.operation_node_id = record.id
        task.operation_attempt = 1
        task.payload = dict(payload)
    if kind == "APPLY" and operation.serve:
        head_node_id = deployment.plan["ray_head_node_id"]
        for api_phase in ROLLBACK_API_PHASES:
            session.add(
                DeploymentOperationNode(
                    id=str(uuid.uuid4()),
                    operation_id=operation.id,
                    node_id=head_node_id,
                    phase=api_phase,
                    status="PENDING",
                    attempt_count=0,
                )
            )
    deployment.status = "APPLYING" if kind == "APPLY" else "VERIFYING"
    if kind == "APPLY":
        deployment.verified_at = None
    _operation_audit(
        session,
        "deployment_operation.prepare",
        operation,
        "success",
        kind=kind,
        deployment_id=deployment.id,
        node_ids=node_ids,
    )
    return operation


# A descriptive alias for callers that do not expose the old bulk terminology.
attach_generic_deployment_operation = attach_deployment_bulk_operation


def _locked_operation_binding(
    session: Session, task: Task, node_id: str
) -> tuple[Task, DeploymentOperationNode, DeploymentOperation] | None:
    # Every operation hook takes the shared Operation row before its ordered
    # node records. Concurrent completions therefore serialize without each
    # holding a different record while waiting on the shared operation.
    with session.no_autoflush:
        identity = session.execute(
            select(
                Task.operation_node_id,
                Task.operation_attempt,
                Task.node_id,
            ).where(Task.id == task.id)
        ).one_or_none()
        if (
            identity is None
            or identity.operation_node_id is None
            or identity.operation_attempt is None
            or identity.node_id != node_id
        ):
            return None
        operation_id = session.scalar(
            select(DeploymentOperationNode.operation_id).where(
                DeploymentOperationNode.id == identity.operation_node_id
            )
        )
        if operation_id is None:
            return None
        operation = session.scalar(
            select(DeploymentOperation)
            .where(DeploymentOperation.id == operation_id)
            .with_for_update()
        )
        if operation is None:
            return None
        records = _phase_nodes(session, operation, lock=True)
        record = next(
            (
                item
                for item in records
                if item.id == identity.operation_node_id
            ),
            None,
        )
        if record is None:
            return None
        locked_task = session.scalar(
            select(Task).where(Task.id == task.id).with_for_update()
        )
        if locked_task is None or locked_task.node_id != node_id:
            return None
    return locked_task, record, operation


def _binding_is_current(
    task: Task, record: DeploymentOperationNode, operation: DeploymentOperation
) -> bool:
    return (
        task.operation_attempt == record.attempt_count
        and task.node_id == record.node_id
        and task.type == PHASE_TASK_TYPES[record.phase].value
        and record.phase == operation.phase
    )


def claim_operation_task(session: Session, task: Task, node_id: str) -> bool:
    """Update operation progress after the generic claim path marks a task RUNNING.

    This hook deliberately does not commit; ``claim_task`` must call it before
    its existing transaction commit.
    """
    binding = _locked_operation_binding(session, task, node_id)
    if binding is None:
        return task.operation_node_id is None
    locked_task, record, operation = binding
    if not _binding_is_current(locked_task, record, operation):
        return False
    if locked_task.status != TaskStatus.RUNNING.value:
        return False
    if record.status == "RUNNING":
        return True
    if record.status != "QUEUED":
        return False
    record.status = "RUNNING"
    record.updated_at = utcnow()
    operation.status = "RUNNING"
    operation.updated_at = utcnow()
    return True


def _valid_operation_success_result(
    task: Task, operation: DeploymentOperation, result: dict[str, Any] | None
) -> bool:
    return valid_deployment_task_success_result(
        task,
        result,
        operation_kind=operation.kind,
        operation_phase=operation.phase,
    )


def valid_deployment_task_success_result(
    task: Task,
    result: dict[str, Any] | None,
    *,
    operation_kind: str | None = None,
    operation_phase: str | None = None,
) -> bool:
    """Validate a deployment task result against its persisted plan.

    Direct START/RESTART/STOP tasks and rollout-bound tasks use the same
    strict backend schema. Legacy APPLY keeps its historical unstructured
    result compatibility only outside the strict backend.
    """
    if type(result) is not dict:
        return False
    plan = task.payload.get("plan") if type(task.payload) is dict else None
    strict_pipeline = (
        type(plan) is dict
        and plan.get("execution_backend") == VLLM_RAY_PP_BACKEND
    )
    if strict_pipeline and operation_kind is None:
        if (
            task.type
            in {
                TaskType.APPLY_DEPLOYMENT.value,
                TaskType.START_DEPLOYMENT.value,
                TaskType.RESTART_DEPLOYMENT.value,
            }
            and task.payload.get("serve") is not True
        ) or (
            task.type == TaskType.VERIFY.value
            and task.payload.get("api") is not True
        ):
            return False
    if operation_kind != "ROLLBACK" and task.type == TaskType.APPLY_DEPLOYMENT.value:
        # Preserve arbitrary legacy success dictionaries. New Agent results that
        # contain checks still receive the rollout-aware blocking validation.
        if "checks" not in result:
            return not strict_pipeline
    expected = {"checks", "ok"} if task.type == TaskType.VERIFY.value else {"checks"}
    if set(result) != expected or ("ok" in result and result["ok"] is not True):
        return False
    checks = result.get("checks")
    if type(checks) is not list or not checks or len(checks) > 16:
        return False
    check_names: set[str] = set()
    checks_by_name: dict[str, dict[str, Any]] = {}
    for check in checks:
        if (
            type(check) is not dict
            or set(check) != {"name", "ok", "detail", "blocking"}
            or type(check["name"]) is not str
            or type(check["ok"]) is not bool
            or type(check["detail"]) is not str
            or type(check["blocking"]) is not bool
            or not 1 <= len(check["name"]) <= 128
            or len(check["detail"]) > 8192
        ):
            return False
        if check["name"] in check_names:
            return False
        check_names.add(check["name"])
        checks_by_name[check["name"]] = check
        if not check["ok"]:
            if strict_pipeline:
                return False
            allows_nonblocking_wait = (
                task.type == TaskType.APPLY_DEPLOYMENT.value
                or (
                    task.type == TaskType.START_DEPLOYMENT.value
                    and operation_phase == "START_TARGET"
                )
            )
            if not allows_nonblocking_wait or check["blocking"]:
                return False
    required = {
        "host-gpu",
        "container-gpu",
        "pipeline-rank-contract" if strict_pipeline else "ray-cluster",
    }
    if task.type == TaskType.STOP_DEPLOYMENT.value:
        required = {"deployment-stop"}
    elif task.type in {
        TaskType.APPLY_DEPLOYMENT.value,
        TaskType.START_DEPLOYMENT.value,
        TaskType.RESTART_DEPLOYMENT.value,
    }:
        model_check = (
            "stage-cache"
            if strict_pipeline
            and plan.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE
            else "model"
        )
        required.update(
            {
                "node-profile",
                "deployment-plan",
                model_check,
                "container-image",
                "ray-container",
            }
        )
        is_head = (
            type(plan) is dict
            and plan.get("ray_head_node_id") == task.node_id
        )
        if task.payload.get("serve") is True and is_head:
            required.update({"vllm-api-start", "vllm-api"})
    elif task.type == TaskType.VERIFY.value:
        is_head = (
            type(plan) is dict
            and plan.get("ray_head_node_id") == task.node_id
        )
        if task.payload.get("api") is True and is_head:
            required.add("vllm-api")
    else:
        return False
    if strict_pipeline:
        if check_names != required:
            return False
    elif not required.issubset(check_names):
        return False
    if (
        strict_pipeline
        and task.type != TaskType.STOP_DEPLOYMENT.value
    ):
        contract = checks_by_name.get("pipeline-rank-contract")
        if (
            contract is None
            or contract["ok"] is not True
            or contract["blocking"] is not True
            or not _valid_pipeline_rank_contract(task, plan, contract["detail"])
        ):
            return False
    return True


def _valid_pipeline_rank_contract(
    task: Task,
    plan: dict[str, Any],
    raw_detail: str,
) -> bool:
    """Match the agent attestation to the exact persisted node contract.

    The shared canonical encoder is intentionally used for both FULL and STAGE
    plans.  A STAGE attestation therefore binds the selected variant, the
    rank-local manifest/tensor set, and the derived cache identity in addition
    to the existing topology fields; extra, missing, duplicate, or reordered
    JSON fields cannot compare equal.
    """
    try:
        typed_plan = DeploymentPlan.from_dict(plan)
        assignment = typed_plan.assignment_for(task.node_id)
        if assignment is None:
            return False
        expected = pipeline_contract_detail(
            typed_plan,
            assignment,
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return False
    return type(raw_detail) is str and raw_detail == expected


def _operation_full_node_set(
    operation: DeploymentOperation, deployment: Deployment
) -> bool:
    try:
        expected, _, _ = _plan_assignments(deployment)
    except DeploymentRolloutError:
        return False
    return list(operation.node_ids) == expected


def _all_operation_nodes_support_rollout(
    session: Session, operation: DeploymentOperation
) -> bool:
    for node_id in operation.node_ids:
        node = session.get(Node, node_id)
        if node is None or not _supports_rollout(node):
            return False
    return True


def _finish_generic_operation(
    session: Session, operation: DeploymentOperation, deployment: Deployment
) -> None:
    if operation.kind == "APPLY" and operation.serve:
        if operation.phase == "APPLY":
            _queue_phase(session, operation, "START_API")
            return
        if operation.phase == "START_API":
            _queue_phase(session, operation, "VERIFY_API")
            return
        if operation.phase != "VERIFY_API":
            raise DeploymentRolloutConflictError(
                "apply serving phase cannot advance",
                code="DEPLOYMENT_OPERATION_PHASE_INVALID",
            )
    now = utcnow()
    operation.status = "SUCCEEDED"
    operation.phase = "COMPLETE"
    operation.active_lineage_id = None
    operation.updated_at = now
    operation.completed_at = now
    if operation.kind == "APPLY":
        deployment.status = (
            "APPLIED"
            if _operation_full_node_set(operation, deployment)
            else "PARTIALLY_APPLIED"
        )
    else:
        qualifies = _operation_full_node_set(
            operation, deployment
        ) and _all_operation_nodes_support_rollout(session, operation)
        if qualifies:
            deployment.status = "VERIFIED"
            deployment.verified_at = now
        else:
            deployment.status = "PARTIALLY_VERIFIED"
    _operation_audit(
        session,
        "deployment_operation.complete",
        operation,
        "success",
        kind=operation.kind,
        deployment_id=deployment.id,
    )


def _advance_rollback(
    session: Session, operation: DeploymentOperation
) -> list[Task]:
    source = session.get(Deployment, operation.deployment_id)
    target = session.get(Deployment, operation.rollback_target_id)
    if source is None or target is None:
        raise DeploymentRolloutNotFoundError()
    if operation.phase == "STOP_SOURCE":
        source.status = "ROLLING_BACK"
        target.status = "ROLLBACK_TARGET_PENDING"
        # The target may have become missing/corrupt/quarantined while the
        # source was stopping. Recheck the exact persisted cache immediately
        # before START_TARGET and never repair or download on the rollback path.
        from .preparation import (
            ArtifactPreparationError,
            effective_deployment_plan,
        )

        try:
            effective_deployment_plan(
                session,
                target,
                require_prepared=True,
                lock_ready_caches=True,
            )
        except ArtifactPreparationError as exc:
            now = utcnow()
            operation.phase = "START_TARGET"
            operation.status = "FAILED"
            operation.updated_at = now
            source.status = "ROLLBACK_FAILED"
            target.status = "ROLLBACK_FAILED"
            for record in _phase_nodes(
                session, operation, "START_TARGET", lock=True
            ):
                record.status = "FAILED"
                record.failure_code = "ROLLBACK_TARGET_CACHE_NOT_READY"
                record.updated_at = now
                record.completed_at = now
            _operation_audit(
                session,
                "deployment_operation.rollback.cache_gate",
                operation,
                "failed",
                failure_code="ROLLBACK_TARGET_CACHE_NOT_READY",
                cache_failure_code=getattr(exc, "code", None),
                target_deployment_id=target.id,
            )
            return []
        return _queue_phase(session, operation, "START_TARGET")
    if operation.phase == "START_TARGET":
        return _queue_phase(session, operation, "VERIFY_TARGET")
    if operation.phase == "VERIFY_TARGET" and operation.serve:
        return _queue_phase(session, operation, "START_API")
    if operation.phase == "START_API":
        return _queue_phase(session, operation, "VERIFY_API")
    if operation.phase not in {"VERIFY_TARGET", "VERIFY_API"}:
        raise DeploymentRolloutConflictError(
            "rollback phase cannot advance", code="ROLLBACK_PHASE_INVALID"
        )
    now = utcnow()
    operation.status = "SUCCEEDED"
    operation.phase = "COMPLETE"
    operation.active_lineage_id = None
    operation.updated_at = now
    operation.completed_at = now
    source.status = "ROLLED_BACK"
    source.verified_at = None
    target.status = "VERIFIED"
    target.verified_at = now
    _operation_audit(
        session,
        "deployment_operation.complete",
        operation,
        "success",
        kind="ROLLBACK",
        source_deployment_id=source.id,
        target_deployment_id=target.id,
    )
    return []


def _update_failed_operation(
    session: Session, operation: DeploymentOperation
) -> None:
    records = _phase_nodes(session, operation, operation.phase, lock=True)
    failed = [item for item in records if item.status in {"FAILED", "CANCELED"}]
    if not failed:
        return
    operation.status = (
        "FAILED" if len(failed) == len(records) else "PARTIAL_FAILED"
    )
    operation.updated_at = utcnow()
    source = session.get(Deployment, operation.deployment_id)
    target = (
        session.get(Deployment, operation.rollback_target_id)
        if operation.rollback_target_id
        else None
    )
    if operation.kind == "ROLLBACK":
        if source is not None:
            source.status = "ROLLBACK_FAILED"
        if target is not None:
            target.status = "ROLLBACK_FAILED"
        return
    # Generic bulk failures are terminal once every node in the phase reports.
    if operation.kind == "VERIFY" and source is not None:
        source.verified_at = None
    if all(item.status in {"SUCCEEDED", "FAILED", "CANCELED"} for item in records):
        was_incomplete = operation.completed_at is None
        operation.active_lineage_id = None
        operation.completed_at = utcnow()
        if source is not None:
            prefix = "APPLY" if operation.kind == "APPLY" else "VERIFY"
            source.status = (
                f"{prefix}_FAILED"
                if operation.status == "FAILED"
                else f"{prefix}_PARTIAL_FAILED"
            )
        if was_incomplete:
            _operation_audit(
                session,
                "deployment_operation.complete",
                operation,
                "failed",
                kind=operation.kind,
                deployment_id=operation.deployment_id,
            )


def finish_operation_task(
    session: Session,
    task: Task,
    node_id: str,
    *,
    result: dict[str, Any] | None,
    error: str | None,
) -> bool:
    """Finish an operation-bound task and atomically stage the next phase.

    This hook does not commit. The Agent terminal endpoint must use it instead
    of the legacy mutation and then commit the task, operation and any newly
    queued phase together.
    """
    binding = _locked_operation_binding(session, task, node_id)
    if binding is None:
        return False
    locked_task, record, operation = binding
    if locked_task.status in TERMINAL_TASK_STATUSES:
        expected_status = (
            TaskStatus.FAILED.value if error is not None else TaskStatus.SUCCEEDED.value
        )
        return (
            locked_task.status == expected_status
            and locked_task.result == result
            and locked_task.error == error
        )
    if not _binding_is_current(locked_task, record, operation):
        return False
    if locked_task.status != TaskStatus.RUNNING.value or record.status != "RUNNING":
        return False
    invalid_result = error is None and not _valid_operation_success_result(
        locked_task, operation, result
    )
    terminal_error = "TASK_RESULT_INVALID" if invalid_result else error
    now = utcnow()
    locked_task.status = (
        TaskStatus.FAILED.value
        if terminal_error is not None
        else TaskStatus.SUCCEEDED.value
    )
    locked_task.result = result if terminal_error is None else None
    locked_task.error = terminal_error
    locked_task.lease_until = None
    record.status = "FAILED" if terminal_error is not None else "SUCCEEDED"
    if invalid_result:
        record.failure_code = "TASK_RESULT_INVALID"
    elif error == "TASK_LEASE_EXPIRED":
        record.failure_code = "TASK_LEASE_EXPIRED"
    elif error is not None:
        record.failure_code = "TASK_FAILED"
    else:
        record.failure_code = None
    record.updated_at = now
    record.completed_at = now
    node = session.get(Node, node_id)
    if node is not None:
        node.desired_state = None
    if terminal_error is not None:
        if operation.phase in {"VERIFY", "VERIFY_TARGET"}:
            verification_deployment_id = (
                operation.rollback_target_id
                if operation.phase == "VERIFY_TARGET"
                else operation.deployment_id
            )
            verification_deployment = (
                session.get(Deployment, verification_deployment_id)
                if verification_deployment_id is not None
                else None
            )
            if verification_deployment is not None:
                from .preparation import (
                    record_deployment_cache_verification_failure,
                )

                record_deployment_cache_verification_failure(
                    session,
                    verification_deployment,
                    node_id=node_id,
                    task_id=locked_task.id,
                )
        _update_failed_operation(session, operation)
        return True

    current_records = _phase_nodes(
        session, operation, operation.phase, lock=True
    )
    if any(item.status in {"FAILED", "CANCELED"} for item in current_records):
        _update_failed_operation(session, operation)
        return True
    if not all(item.status == "SUCCEEDED" for item in current_records):
        operation.status = "RUNNING"
        operation.updated_at = now
        return True
    if operation.kind == "ROLLBACK":
        _advance_rollback(session, operation)
    else:
        deployment = session.get(Deployment, operation.deployment_id)
        if deployment is None:
            raise DeploymentRolloutNotFoundError()
        _finish_generic_operation(session, operation, deployment)
    return True


def cancel_operation_task(session: Session, task: Task) -> bool:
    """Project a legacy task cancellation into operation progress without committing."""
    binding = _locked_operation_binding(session, task, task.node_id)
    if binding is None:
        return False
    locked_task, record, operation = binding
    if (
        locked_task.status == TaskStatus.CANCELED.value
        and record.status == "CANCELED"
    ):
        return True
    if not _binding_is_current(locked_task, record, operation):
        return False
    if (
        locked_task.status != TaskStatus.QUEUED.value
        or record.status != "QUEUED"
    ):
        return False
    now = utcnow()
    locked_task.status = TaskStatus.CANCELED.value
    locked_task.lease_until = None
    record.status = "CANCELED"
    record.failure_code = "TASK_CANCELED"
    record.updated_at = now
    record.completed_at = now
    node = session.get(Node, locked_task.node_id)
    if node is not None:
        node.desired_state = None
    _update_failed_operation(session, operation)
    return True


# Alternative hook-oriented names for integration call sites.
operation_task_claimed = claim_operation_task
operation_task_terminal = finish_operation_task
operation_task_canceled = cancel_operation_task


__all__ = [
    "DeploymentRolloutConflictError",
    "DeploymentRolloutError",
    "DeploymentRolloutNotFoundError",
    "attach_deployment_bulk_operation",
    "attach_generic_deployment_operation",
    "cancel_operation_task",
    "claim_operation_task",
    "deployment_generation_detail",
    "deployment_lineage_generations",
    "deployment_operation_detail",
    "finish_operation_task",
    "operation_task_canceled",
    "operation_task_claimed",
    "operation_task_terminal",
    "prepare_or_apply_rollback",
]
