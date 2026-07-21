from __future__ import annotations

import re
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dure.task import TaskType

from .models import (
    ArtifactPreparation,
    ArtifactPreparationNode,
    Deployment,
    DeploymentOperation,
    DeploymentOperationNode,
    FleetDeploymentRuntime,
    FleetRecord,
    utcnow,
)
from .resource_reservation import lock_fleet_reservation_gate


FLEET_PREPARATION_NAMESPACE = uuid.UUID(
    "ca033f0f-d615-401e-a5c6-b06080469b34"
)
FLEET_RUNTIME_STATUSES = frozenset(
    {
        "ACCEPTED",
        "PREPARING",
        "PREPARED",
        "PREPARE_FAILED",
        "APPLYING",
        "VERIFYING",
        "ACTIVE",
        "APPLY_FAILED",
        "VERIFY_FAILED",
    }
)
FLEET_RUNTIME_FAILURE_STATUSES = frozenset(
    {"PREPARE_FAILED", "APPLY_FAILED", "VERIFY_FAILED"}
)
_FAILURE_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}")


class FleetRuntimeError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}

    def to_detail(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), **self.details}


class FleetRuntimeNotFoundError(FleetRuntimeError):
    def __init__(self, fleet_id: str) -> None:
        super().__init__(
            "Fleet runtime not found",
            code="FLEET_NOT_FOUND",
            details={"fleet_id": fleet_id},
        )


def _closed_failure_code(value: Any, fallback: str) -> str:
    return value if type(value) is str and _FAILURE_CODE.fullmatch(value) else fallback


def _runtime_rows(
    session: Session,
    fleet_id: str,
    *,
    lock: bool = False,
    refresh: bool = False,
) -> list[FleetDeploymentRuntime]:
    statement = (
        select(FleetDeploymentRuntime)
        .where(FleetDeploymentRuntime.fleet_id == fleet_id)
        .order_by(FleetDeploymentRuntime.deployment_id)
    )
    if lock:
        statement = statement.with_for_update()
    if refresh:
        statement = statement.execution_options(populate_existing=True)
    return list(session.scalars(statement))


def _aggregate_status(statuses: list[str]) -> str:
    if not statuses or any(status not in FLEET_RUNTIME_STATUSES for status in statuses):
        raise FleetRuntimeError(
            "Fleet runtime state is incomplete",
            code="FLEET_RUNTIME_RECORD_INVALID",
        )
    if all(status == "ACTIVE" for status in statuses):
        return "ACTIVE"
    if "APPLYING" in statuses:
        return "APPLYING"
    if "VERIFYING" in statuses:
        return "VERIFYING"
    failures = [status for status in statuses if status in FLEET_RUNTIME_FAILURE_STATUSES]
    if failures:
        return "FAILED" if len(failures) == len(statuses) else "PARTIAL_FAILED"
    if all(status == "PREPARED" for status in statuses):
        return "PREPARED"
    if any(status in {"ACTIVE", "PREPARED"} for status in statuses):
        return "APPLYING" if "ACTIVE" in statuses else "PREPARING"
    if any(status in {"PREPARING", "PREPARED"} for status in statuses):
        return "PREPARING"
    return "ACCEPTED"


def recompute_fleet_runtime_status(session: Session, fleet_id: str) -> str:
    # Serialize aggregate projection through the Fleet row.  Two deployments
    # may complete concurrently while holding different runtime rows; taking
    # this shared row before the final read prevents the last committer from
    # publishing an aggregate that omitted the other committed transition.
    fleet = session.scalar(
        select(FleetRecord)
        .where(FleetRecord.id == fleet_id)
        .with_for_update()
    )
    if fleet is None:
        raise FleetRuntimeNotFoundError(fleet_id)
    rows = _runtime_rows(session, fleet_id, refresh=True)
    status = _aggregate_status([row.status for row in rows])
    fleet.status = status
    fleet.updated_at = utcnow()
    return status


def _preparation_failure_code(
    session: Session, preparation_id: str
) -> str:
    records = list(
        session.scalars(
            select(ArtifactPreparationNode)
            .where(
                ArtifactPreparationNode.preparation_id == preparation_id
            )
            .order_by(ArtifactPreparationNode.node_id)
        )
    )
    for record in records:
        value = record.model_failure_code or record.image_failure_code
        if value is not None:
            return _closed_failure_code(value, "FLEET_PREPARATION_FAILED")
    return "FLEET_PREPARATION_FAILED"


def _operation_failure_code(
    session: Session, operation: DeploymentOperation
) -> str:
    records = list(
        session.scalars(
            select(DeploymentOperationNode)
            .where(
                DeploymentOperationNode.operation_id == operation.id,
                DeploymentOperationNode.phase == operation.phase,
                DeploymentOperationNode.status.in_({"FAILED", "CANCELED"}),
            )
            .order_by(
                DeploymentOperationNode.node_id,
                DeploymentOperationNode.id,
            )
        )
    )
    for record in records:
        if record.failure_code is not None:
            return _closed_failure_code(
                record.failure_code, "FLEET_OPERATION_FAILED"
            )
    return "FLEET_OPERATION_FAILED"


def bind_fleet_preparation(
    session: Session,
    *,
    runtime_id: str,
    deployment: Deployment,
    preparation: ArtifactPreparation,
) -> FleetDeploymentRuntime:
    runtime = session.scalar(
        select(FleetDeploymentRuntime)
        .where(FleetDeploymentRuntime.id == runtime_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        runtime is None
        or deployment.fleet_id is None
        or runtime.fleet_id != deployment.fleet_id
        or runtime.deployment_id != deployment.id
        or preparation.deployment_id != deployment.id
        or (
            runtime.preparation_id is not None
            and runtime.preparation_id != preparation.id
        )
    ):
        raise FleetRuntimeError(
            "Fleet preparation does not match its reserved deployment",
            code="FLEET_RUNTIME_BINDING_INVALID",
            details={"deployment_id": deployment.id},
        )
    if runtime.status in {
        "APPLYING",
        "VERIFYING",
        "ACTIVE",
        "APPLY_FAILED",
        "VERIFY_FAILED",
    }:
        # A late or replayed preparation completion cannot move a deployment
        # backward after its explicit APPLY lifecycle has started.
        if runtime.preparation_id != preparation.id:
            raise FleetRuntimeError(
                "Fleet operation lost its exact preparation binding",
                code="FLEET_RUNTIME_BINDING_INVALID",
                details={"deployment_id": deployment.id},
            )
        return runtime
    runtime.preparation_id = preparation.id
    if preparation.status == "SUCCEEDED":
        runtime.status = "PREPARED"
    elif preparation.status in {"PARTIAL_FAILED", "FAILED"}:
        failure_code = _preparation_failure_code(session, preparation.id)
        runtime.status = "PREPARE_FAILED"
        runtime.failure_phase = "PREPARE"
        runtime.failure_code = failure_code
    else:
        runtime.status = "PREPARING"
    if runtime.status not in FLEET_RUNTIME_FAILURE_STATUSES:
        runtime.failure_phase = None
        runtime.failure_code = None
    runtime.updated_at = utcnow()
    recompute_fleet_runtime_status(session, runtime.fleet_id)
    return runtime


def sync_fleet_preparation_status(
    session: Session, preparation: ArtifactPreparation
) -> bool:
    runtime = session.scalar(
        select(FleetDeploymentRuntime)
        .where(
            FleetDeploymentRuntime.preparation_id == preparation.id
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if runtime is None:
        return False
    deployment = session.get(Deployment, runtime.deployment_id)
    if deployment is None:
        raise FleetRuntimeError(
            "Fleet preparation deployment is missing",
            code="FLEET_RUNTIME_RECORD_INVALID",
        )
    bind_fleet_preparation(
        session,
        runtime_id=runtime.id,
        deployment=deployment,
        preparation=preparation,
    )
    return True


def bind_fleet_operation(
    session: Session,
    *,
    runtime_id: str,
    deployment: Deployment,
    operation: DeploymentOperation,
) -> FleetDeploymentRuntime:
    runtime = session.scalar(
        select(FleetDeploymentRuntime)
        .where(FleetDeploymentRuntime.id == runtime_id)
        .with_for_update()
    )
    preparation = (
        session.get(ArtifactPreparation, runtime.preparation_id)
        if runtime is not None and runtime.preparation_id is not None
        else None
    )
    if (
        runtime is None
        or deployment.fleet_id is None
        or runtime.fleet_id != deployment.fleet_id
        or runtime.deployment_id != deployment.id
        or operation.deployment_id != deployment.id
        or operation.kind != "APPLY"
        or preparation is None
        or preparation.status != "SUCCEEDED"
        or runtime.status
        not in {"PREPARED", "APPLY_FAILED", "VERIFY_FAILED", "APPLYING"}
        or (
            runtime.current_operation_id is not None
            and runtime.current_operation_id != operation.id
            and runtime.status == "APPLYING"
        )
    ):
        raise FleetRuntimeError(
            "Fleet operation does not match a prepared reserved deployment",
            code="FLEET_RUNTIME_BINDING_INVALID",
            details={"deployment_id": deployment.id},
        )

    verify_records = list(
        session.scalars(
            select(DeploymentOperationNode)
            .where(
                DeploymentOperationNode.operation_id == operation.id,
                DeploymentOperationNode.phase == "VERIFY",
            )
            .order_by(DeploymentOperationNode.node_id)
        )
    )
    expected_nodes = sorted(operation.node_ids)
    if not verify_records:
        for node_id in expected_nodes:
            session.add(
                DeploymentOperationNode(
                    id=str(uuid.uuid4()),
                    operation_id=operation.id,
                    node_id=node_id,
                    phase="VERIFY",
                    status="PENDING",
                    attempt_count=0,
                )
            )
    elif [record.node_id for record in verify_records] != expected_nodes or any(
        record.status != "PENDING" or record.attempt_count != 0
        for record in verify_records
    ):
        raise FleetRuntimeError(
            "Fleet verification phase is not pristine",
            code="FLEET_RUNTIME_OPERATION_INVALID",
            details={"operation_id": operation.id},
        )
    operation.api = True
    runtime.current_operation_id = operation.id
    runtime.status = "APPLYING"
    runtime.failure_phase = None
    runtime.failure_code = None
    runtime.updated_at = utcnow()
    recompute_fleet_runtime_status(session, runtime.fleet_id)
    return runtime


def fleet_operation_is_current(
    session: Session,
    operation: DeploymentOperation,
    *,
    lock: bool = False,
) -> bool:
    deployment = session.get(Deployment, operation.deployment_id)
    if deployment is None or deployment.fleet_id is None:
        return True
    statement = select(FleetDeploymentRuntime).where(
        FleetDeploymentRuntime.fleet_id == deployment.fleet_id,
        FleetDeploymentRuntime.deployment_id == deployment.id,
    )
    if lock:
        statement = statement.with_for_update()
    runtime = session.scalar(statement)
    return runtime is not None and runtime.current_operation_id == operation.id


def sync_fleet_operation_status(
    session: Session, operation: DeploymentOperation
) -> bool:
    runtime = session.scalar(
        select(FleetDeploymentRuntime)
        .where(
            FleetDeploymentRuntime.current_operation_id == operation.id
        )
        .with_for_update()
    )
    if runtime is None:
        return False
    if operation.status in {"FAILED", "PARTIAL_FAILED"}:
        verify_failure = operation.phase in {"VERIFY", "VERIFY_API"}
        failure_code = _operation_failure_code(session, operation)
        runtime.status = "VERIFY_FAILED" if verify_failure else "APPLY_FAILED"
        runtime.failure_phase = "VERIFY" if verify_failure else "APPLY"
        runtime.failure_code = failure_code
    elif operation.status == "SUCCEEDED" and operation.phase == "COMPLETE":
        deployment = session.get(Deployment, runtime.deployment_id)
        if (
            deployment is None
            or deployment.status != "VERIFIED"
            or deployment.verified_at is None
        ):
            raise FleetRuntimeError(
                "Fleet operation completed without verification evidence",
                code="FLEET_RUNTIME_VERIFICATION_INCOMPLETE",
                details={"operation_id": operation.id},
            )
        runtime.status = "ACTIVE"
        runtime.failure_phase = None
        runtime.failure_code = None
    elif operation.phase in {"VERIFY", "VERIFY_API"}:
        runtime.status = "VERIFYING"
        runtime.failure_phase = None
        runtime.failure_code = None
    else:
        runtime.status = "APPLYING"
        runtime.failure_phase = None
        runtime.failure_code = None
    runtime.updated_at = utcnow()
    recompute_fleet_runtime_status(session, runtime.fleet_id)
    return True


def _record_runtime_failure(
    session: Session,
    *,
    runtime_id: str,
    phase: str,
    code: str,
    expected: tuple[str, str | None, str | None],
) -> tuple[bool, dict[str, Any]]:
    # Failure recording is a second transaction because the failed producer
    # has already rolled back. Re-enter the producer gate and use a small CAS
    # so a successful concurrent request cannot be overwritten afterward.
    lock_fleet_reservation_gate(session)
    runtime = session.scalar(
        select(FleetDeploymentRuntime)
        .where(FleetDeploymentRuntime.id == runtime_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if runtime is None:
        raise FleetRuntimeError(
            "Fleet deployment runtime disappeared",
            code="FLEET_RUNTIME_RECORD_INVALID",
        )
    current = (
        runtime.status,
        runtime.preparation_id,
        runtime.current_operation_id,
    )
    if current != expected:
        projection = runtime.to_dict()
        session.rollback()
        return False, projection
    runtime.status = f"{phase}_FAILED"
    runtime.failure_phase = phase
    runtime.failure_code = _closed_failure_code(
        code, f"FLEET_{phase}_FAILED"
    )
    runtime.updated_at = utcnow()
    recompute_fleet_runtime_status(session, runtime.fleet_id)
    projection = runtime.to_dict()
    session.commit()
    return True, projection


def fleet_runtime_projection(
    session: Session, fleet_id: str
) -> list[dict[str, Any]]:
    return [
        row.to_dict()
        for row in _runtime_rows(session, fleet_id, refresh=True)
    ]


def _validate_fleet_record(session: Session, fleet_id: str) -> None:
    # Refuse to mutate only a surviving subset if DB corruption removed or
    # rebound a deployment, reservation, runtime row, or source candidate.
    from .fleet_acceptance import show_fleet

    show_fleet(session, fleet_id)


def prepare_fleet(session: Session, fleet_id: str) -> dict[str, Any]:
    fleet = session.get(FleetRecord, fleet_id)
    if fleet is None:
        raise FleetRuntimeNotFoundError(fleet_id)
    _validate_fleet_record(session, fleet_id)
    rows = _runtime_rows(session, fleet_id)
    if not rows:
        raise FleetRuntimeError(
            "Fleet has no deployment runtime records",
            code="FLEET_RUNTIME_RECORD_INVALID",
            details={"fleet_id": fleet_id},
        )
    actions: list[dict[str, Any]] = []
    from .preparation import (
        ArtifactPreparationError,
        prepare_deployment_artifacts,
    )

    for runtime_id in [row.id for row in rows]:
        # Refresh after the producer gate, but do not lock the runtime before
        # preparation takes its ordered Node rows. Agent completion follows
        # Node -> preparation -> runtime; taking runtime -> Node here would
        # create a PostgreSQL deadlock. The later bind is authoritative.
        lock_fleet_reservation_gate(session)
        runtime = session.scalar(
            select(FleetDeploymentRuntime)
            .where(FleetDeploymentRuntime.id == runtime_id)
            .execution_options(populate_existing=True)
        )
        if runtime is None:
            raise FleetRuntimeError(
                "Fleet deployment runtime disappeared",
                code="FLEET_RUNTIME_RECORD_INVALID",
            )
        if runtime.status in {
            "APPLYING",
            "VERIFYING",
            "ACTIVE",
            "APPLY_FAILED",
            "VERIFY_FAILED",
        }:
            actions.append(
                {
                    "deployment_id": runtime.deployment_id,
                    "changed": False,
                    "status": runtime.status,
                    "reason": "PREPARATION_ALREADY_TERMINAL",
                }
            )
            session.rollback()
            continue
        request_id = str(
            uuid.uuid5(
                FLEET_PREPARATION_NAMESPACE,
                f"fleet-preparation:{runtime.fleet_id}:{runtime.deployment_id}",
            )
        )
        deployment_id = runtime.deployment_id
        expected_runtime = (
            runtime.status,
            runtime.preparation_id,
            runtime.current_operation_id,
        )
        try:
            preparation, tasks, changed = prepare_deployment_artifacts(
                session,
                runtime.deployment_id,
                request_id=request_id,
                apply=True,
                _fleet_runtime_id=runtime.id,
            )
            current = session.get(FleetDeploymentRuntime, runtime.id)
            actions.append(
                {
                    "deployment_id": runtime.deployment_id,
                    "preparation_id": preparation.id,
                    "task_ids": sorted(task.id for task in tasks),
                    "changed": changed,
                    "status": current.status if current is not None else None,
                }
            )
        except ArtifactPreparationError as exc:
            session.rollback()
            failure_recorded, projection = _record_runtime_failure(
                session,
                runtime_id=runtime_id,
                phase="PREPARE",
                code=exc.code,
                expected=expected_runtime,
            )
            action = {
                "deployment_id": deployment_id,
                "changed": False,
                "status": projection["status"],
            }
            if failure_recorded:
                action["failure_code"] = _closed_failure_code(
                    exc.code, "FLEET_PREPARE_FAILED"
                )
            else:
                action["reason"] = "RUNTIME_ADVANCED"
                if projection.get("failure_code") is not None:
                    action["failure_code"] = projection["failure_code"]
            actions.append(action)
    return {"fleet_id": fleet_id, "actions": actions}


def apply_fleet(session: Session, fleet_id: str) -> dict[str, Any]:
    fleet = session.get(FleetRecord, fleet_id)
    if fleet is None:
        raise FleetRuntimeNotFoundError(fleet_id)
    _validate_fleet_record(session, fleet_id)
    rows = _runtime_rows(session, fleet_id)
    if not rows:
        raise FleetRuntimeError(
            "Fleet has no deployment runtime records",
            code="FLEET_RUNTIME_RECORD_INVALID",
            details={"fleet_id": fleet_id},
        )
    actions: list[dict[str, Any]] = []
    from .preparation import ArtifactPreparationError
    from .rollout import DeploymentRolloutError
    from .service import create_tasks

    for runtime_id in [row.id for row in rows]:
        # Serialize every producer through the same reservation gate used by
        # task creation, then force-refresh the identity-map entry.  Without
        # this ordering a concurrent, already-committed APPLY can be mistaken
        # for a new conflict and incorrectly downgrade its runtime to FAILED.
        lock_fleet_reservation_gate(session)
        runtime = session.scalar(
            select(FleetDeploymentRuntime)
            .where(FleetDeploymentRuntime.id == runtime_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if runtime is None:
            raise FleetRuntimeError(
                "Fleet deployment runtime disappeared",
                code="FLEET_RUNTIME_RECORD_INVALID",
            )
        if runtime.status == "ACTIVE":
            actions.append(
                {
                    "deployment_id": runtime.deployment_id,
                    "changed": False,
                    "status": "ACTIVE",
                    "reason": "ALREADY_ACTIVE",
                }
            )
            session.rollback()
            continue
        if runtime.status in {"APPLYING", "VERIFYING"}:
            actions.append(
                {
                    "deployment_id": runtime.deployment_id,
                    "changed": False,
                    "status": runtime.status,
                    "reason": "OPERATION_ALREADY_ACTIVE",
                }
            )
            session.rollback()
            continue
        if runtime.status not in {"PREPARED", "APPLY_FAILED", "VERIFY_FAILED"}:
            actions.append(
                {
                    "deployment_id": runtime.deployment_id,
                    "changed": False,
                    "status": runtime.status,
                    "reason": "DEPLOYMENT_NOT_PREPARED",
                }
            )
            session.rollback()
            continue
        current_operation = (
            session.get(DeploymentOperation, runtime.current_operation_id)
            if runtime.current_operation_id is not None
            else None
        )
        if (
            current_operation is not None
            and current_operation.completed_at is None
            and current_operation.status
            in {"PREPARED", "QUEUED", "RUNNING", "PARTIAL_FAILED", "FAILED"}
        ):
            actions.append(
                {
                    "deployment_id": runtime.deployment_id,
                    "operation_id": current_operation.id,
                    "changed": False,
                    "status": runtime.status,
                    "reason": "OPERATION_STILL_ACTIVE",
                }
            )
            session.rollback()
            continue
        deployment = session.get(Deployment, runtime.deployment_id)
        if deployment is None:
            raise FleetRuntimeError(
                "Fleet deployment is missing",
                code="FLEET_RUNTIME_RECORD_INVALID",
            )
        node_ids = sorted(
            item.get("node_id")
            for item in deployment.plan.get("assignments", [])
            if isinstance(item, dict) and isinstance(item.get("node_id"), str)
        )
        deployment_id = runtime.deployment_id
        expected_runtime = (
            runtime.status,
            runtime.preparation_id,
            runtime.current_operation_id,
        )
        try:
            bulk_id, tasks, errors = create_tasks(
                session,
                node_ids=node_ids,
                task_type=TaskType.APPLY_DEPLOYMENT,
                deployment_id=deployment.id,
                options={"serve": True},
                _fleet_runtime_id=runtime.id,
            )
            if errors or len(tasks) != len(node_ids):
                raise FleetRuntimeError(
                    "Fleet apply did not queue every reserved node",
                    code="FLEET_APPLY_INCOMPLETE",
                    details={"errors": errors},
                )
            current = session.get(FleetDeploymentRuntime, runtime.id)
            actions.append(
                {
                    "deployment_id": runtime.deployment_id,
                    "operation_id": (
                        current.current_operation_id if current else None
                    ),
                    "bulk_id": bulk_id,
                    "task_ids": sorted(task.id for task in tasks),
                    "changed": True,
                    "status": current.status if current else None,
                }
            )
        except (ArtifactPreparationError, DeploymentRolloutError, FleetRuntimeError) as exc:
            session.rollback()
            failure_code = getattr(exc, "code", "FLEET_APPLY_FAILED")
            failure_recorded, projection = _record_runtime_failure(
                session,
                runtime_id=runtime_id,
                phase="APPLY",
                code=failure_code,
                expected=expected_runtime,
            )
            action = {
                "deployment_id": deployment_id,
                "changed": False,
                "status": projection["status"],
            }
            if failure_recorded:
                action["failure_code"] = _closed_failure_code(
                    failure_code, "FLEET_APPLY_FAILED"
                )
            else:
                action["reason"] = "RUNTIME_ADVANCED"
                action["operation_id"] = projection.get(
                    "current_operation_id"
                )
                if projection.get("failure_code") is not None:
                    action["failure_code"] = projection["failure_code"]
            actions.append(action)
    return {"fleet_id": fleet_id, "actions": actions}


__all__ = [
    "FleetRuntimeError",
    "FleetRuntimeNotFoundError",
    "apply_fleet",
    "bind_fleet_operation",
    "bind_fleet_preparation",
    "fleet_operation_is_current",
    "fleet_runtime_projection",
    "prepare_fleet",
    "recompute_fleet_runtime_status",
    "sync_fleet_operation_status",
    "sync_fleet_preparation_status",
]
