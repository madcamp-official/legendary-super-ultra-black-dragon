from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import re
import uuid
from datetime import timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dure.artifact_prepare import validate_digest_pinned_runtime_image
from dure.models import NodeProfile
from dure.profile_generator import AUTO_PROFILE_ORIGIN
from dure.resource_pool import build_gpu_pool_snapshot
from dure.selector import InventoryNode, _gpu_architecture

from .models import (
    AuditEvent,
    Deployment,
    DeploymentOperation,
    ModelArtifact,
    ModelRelease,
    Node,
    NodeProfileRecord,
    PlacementProfileRecord,
    ProfileQualificationBinding,
    ProfileQualificationEvidence,
    ProfileQualificationRun,
    RuntimeRelease,
    Task,
    TaskStatus,
    utcnow,
)
from .resource_reservation import (
    active_fleet_reservations,
    lock_fleet_reservation_gate,
)


QUALIFICATION_POLICY_VERSION = "profile-qualification-v2"
QUALIFICATION_SUITE_ID = "dure-profile-qualification-v2"
QUALIFICATION_PURPOSES = frozenset({"PRIMARY", "SUPPLEMENTARY"})
QUALIFICATION_PROFILE_MAX_AGE = timedelta(seconds=90)
QUALIFICATION_STEPS = (
    "STATIC_COMPATIBILITY",
    "CAPACITY_ESTIMATE",
    "ARTIFACT_READY",
    "NETWORK_NCCL",
    "MODEL_LOAD",
    "SHORT_INFERENCE",
    "CONTEXT_CONCURRENCY",
    "RESTART_STABILITY",
)
QUALIFICATION_FAILURE_BY_STEP = {
    step: failure
    for step, failure in zip(
        QUALIFICATION_STEPS,
        (
            "STATIC_COMPATIBILITY_FAILED",
            "CAPACITY_ESTIMATE_FAILED",
            "ARTIFACT_NOT_READY",
            "NETWORK_NCCL_FAILED",
            "MODEL_LOAD_FAILED",
            "SHORT_INFERENCE_FAILED",
            "CONTEXT_CONCURRENCY_FAILED",
            "RESTART_STABILITY_FAILED",
        ),
    )
}


class ProfileQualificationError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "PROFILE_QUALIFICATION_BLOCKED",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


def _aware(value):
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def _canonical_uuid(value: str, *, field: str) -> str:
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a canonical UUID") from exc
    return value


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _node_online(node: Node, now) -> bool:
    last_seen = _aware(node.last_seen)
    return (
        last_seen is not None
        and timedelta(0) <= now - last_seen <= QUALIFICATION_PROFILE_MAX_AGE
    )


def _load_inventory(
    session: Session, node_ids: list[str], *, now
) -> list[InventoryNode]:
    ordered_ids = sorted(node_ids)
    nodes = {
        node.id: node
        for node in session.scalars(
            select(Node)
            .where(Node.id.in_(ordered_ids))
            .order_by(Node.id)
            .with_for_update()
        )
    }
    profiles = {
        record.node_id: record
        for record in session.scalars(
            select(NodeProfileRecord)
            .where(NodeProfileRecord.node_id.in_(ordered_ids))
            .order_by(NodeProfileRecord.node_id)
            .with_for_update()
        )
    }
    missing = sorted(set(ordered_ids) - set(nodes))
    if missing:
        raise ProfileQualificationError(
            "qualification node does not exist",
            code="QUALIFICATION_NODE_NOT_FOUND",
            details={"node_ids": missing},
        )
    inventory = []
    for node_id in ordered_ids:
        node = nodes[node_id]
        record = profiles.get(node_id)
        profile = None
        profile_error = None
        fresh = False
        if record is None:
            profile_error = "missing"
        else:
            try:
                profile = NodeProfile.from_dict(record.profile)
                profile.node_id = node_id
                age = now - _aware(record.updated_at)
                fresh = timedelta(0) <= age <= QUALIFICATION_PROFILE_MAX_AGE
            except (KeyError, TypeError, ValueError):
                profile_error = "invalid"
        inventory.append(
            InventoryNode(
                node_id=node_id,
                profile=profile,
                approved=node.approved,
                online=_node_online(node, now),
                profile_fresh=fresh,
                network_verified=False,
                profile_error=profile_error,
                agent_version=node.agent_version,
            )
        )
    return inventory


def _qualification_occupancy(
    session: Session,
    inventory: list[InventoryNode],
    *,
    exclude_run_id: str | None = None,
    exclude_fleet_id: str | None = None,
) -> dict[str, str]:
    requested = {item.node_id for item in inventory}
    reasons: dict[str, str] = {}
    gpu_nodes: dict[str, set[str]] = {}
    for item in inventory:
        if item.profile is None:
            continue
        for gpu in item.profile.gpus:
            gpu_nodes.setdefault(gpu.uuid, set()).add(item.node_id)
    for reservation in active_fleet_reservations(
        session,
        node_ids=requested,
        gpu_uuids=gpu_nodes,
    ):
        if reservation.fleet_id == exclude_fleet_id:
            continue
        affected = set(gpu_nodes.get(reservation.gpu_uuid, set()))
        if reservation.node_id in requested:
            affected.add(reservation.node_id)
        for node_id in sorted(affected):
            reasons.setdefault(
                node_id,
                "ACTIVE_FLEET_RESERVATION:"
                f"{reservation.fleet_id}:{reservation.deployment_id}",
            )
    active_runs = select(ProfileQualificationRun).where(
        ProfileQualificationRun.status == "QUALIFYING"
    )
    if exclude_run_id is not None:
        active_runs = active_runs.where(
            ProfileQualificationRun.id != exclude_run_id
        )
    for run in session.scalars(active_runs.order_by(ProfileQualificationRun.id)):
        affected = set(run.node_ids)
        for binding in run.gpu_bindings or []:
            if isinstance(binding, dict):
                affected.update(
                    gpu_nodes.get(binding.get("gpu_uuid"), set())
                )
        for affected_node_id in sorted(affected):
            if affected_node_id in requested:
                reasons.setdefault(
                    affected_node_id,
                    f"ACTIVE_PROFILE_QUALIFICATION:{run.id}",
                )
    for node_id, task_id, deployment_id in session.execute(
        select(Task.node_id, Task.id, Task.deployment_id).where(
            Task.status.in_(
                {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
            ),
        )
    ):
        affected = {node_id}
        deployment = (
            session.get(Deployment, deployment_id)
            if deployment_id is not None
            else None
        )
        if deployment is not None and isinstance(deployment.plan, dict):
            for assignment in deployment.plan.get("assignments", []):
                if isinstance(assignment, dict):
                    affected.update(
                        gpu_nodes.get(assignment.get("gpu_uuid"), set())
                    )
        for affected_node_id in sorted(affected):
            if affected_node_id in requested:
                reasons.setdefault(
                    affected_node_id, f"ACTIVE_TASK:{task_id}"
                )
    for operation in session.scalars(
        select(DeploymentOperation).where(
            DeploymentOperation.active_lineage_id.is_not(None)
        )
    ):
        affected = set(operation.node_ids)
        deployment = session.get(Deployment, operation.deployment_id)
        if deployment is not None and isinstance(deployment.plan, dict):
            for assignment in deployment.plan.get("assignments", []):
                if isinstance(assignment, dict):
                    affected.update(
                        gpu_nodes.get(assignment.get("gpu_uuid"), set())
                    )
        for node_id in affected:
            if node_id in requested:
                reasons.setdefault(
                    node_id, f"ACTIVE_DEPLOYMENT_OPERATION:{operation.id}"
                )
    for item in inventory:
        if item.profile is not None and item.profile.workloads:
            reasons.setdefault(item.node_id, "OBSERVED_RUNNING_WORKLOAD")
    return reasons


def active_profile_qualification_nodes(
    session: Session,
    node_ids: set[str] | list[str] | tuple[str, ...],
    *,
    exclude_run_id: str | None = None,
) -> dict[str, str]:
    """Return exact nodes reserved by currently active qualification runs."""

    requested = set(node_ids)
    if not requested:
        return {}
    statement = (
        select(
            ProfileQualificationBinding.node_id,
            ProfileQualificationBinding.run_id,
        )
        .join(
            ProfileQualificationRun,
            ProfileQualificationRun.id == ProfileQualificationBinding.run_id,
        )
        .where(
            ProfileQualificationBinding.node_id.in_(requested),
            ProfileQualificationRun.status == "QUALIFYING",
        )
        .order_by(
            ProfileQualificationBinding.node_id,
            ProfileQualificationBinding.run_id,
        )
    )
    if exclude_run_id is not None:
        statement = statement.where(
            ProfileQualificationBinding.run_id != exclude_run_id
        )
    return {
        node_id: run_id
        for node_id, run_id in session.execute(statement)
    }


def _workload_contract(
    *,
    placement: PlacementProfileRecord,
    release: ModelRelease,
    artifact: ModelArtifact,
    runtime: RuntimeRelease,
    qualification_purpose: str | None,
) -> dict[str, Any]:
    output_tokens = min(32, max(1, placement.max_model_len // 16))
    input_tokens = placement.max_model_len - output_tokens
    contract = {
        "policy_version": QUALIFICATION_POLICY_VERSION,
        "suite_id": QUALIFICATION_SUITE_ID,
        "release_id": release.id,
        "placement_id": placement.id,
        "profile_spec_digest": placement.spec_digest,
        "artifact_revision": artifact.revision,
        "artifact_manifest_digest": artifact.manifest_digest,
        "runtime_image": runtime.image,
        "runtime_vllm_version": runtime.vllm_version,
        "tensor_parallel_size": placement.tensor_parallel_size,
        "pipeline_parallel_size": placement.pipeline_parallel_size,
        "node_count": placement.node_count,
        "max_model_len": placement.max_model_len,
        "max_concurrency": placement.max_concurrency,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "warmup_requests": max(1, min(8, placement.max_concurrency)),
        "minimum_request_count": max(2, placement.max_concurrency),
        "steps": list(QUALIFICATION_STEPS),
    }
    if qualification_purpose is not None:
        contract["qualification_purpose"] = qualification_purpose
    return contract


def _qualification_purpose(run: ProfileQualificationRun) -> str:
    if type(run.workload) is not dict:
        raise ProfileQualificationError(
            "qualification run has an invalid workload",
            code="QUALIFICATION_PURPOSE_INVALID",
        )
    purpose = run.workload.get("qualification_purpose", "PRIMARY")
    if type(purpose) is not str or purpose not in QUALIFICATION_PURPOSES:
        raise ProfileQualificationError(
            "qualification run has an invalid purpose",
            code="QUALIFICATION_PURPOSE_INVALID",
        )
    return purpose


def _stored_bindings(
    session: Session, run_id: str
) -> list[dict[str, Any]]:
    return [
        {
            "rank": binding.rank,
            "node_id": binding.node_id,
            "gpu_index": binding.gpu_index,
            "gpu_uuid": binding.gpu_uuid,
            "memory_mib": binding.memory_mib,
            "compute_capability": binding.compute_capability,
        }
        for binding in session.scalars(
            select(ProfileQualificationBinding)
            .where(ProfileQualificationBinding.run_id == run_id)
            .order_by(ProfileQualificationBinding.rank)
        )
    ]


def _qualification_context(
    session: Session,
    *,
    placement: PlacementProfileRecord,
    node_ids: list[str],
    now,
    qualification_run_id: str | None = None,
    qualification_purpose: str | None = "PRIMARY",
    reservation_fleet_id: str | None = None,
) -> dict[str, Any]:
    if placement.origin != AUTO_PROFILE_ORIGIN or placement.spec_digest is None:
        raise ProfileQualificationError(
            "only generated profiles can use automatic qualification",
            code="QUALIFICATION_PROFILE_UNSUPPORTED",
        )
    if placement.tensor_parallel_size != 1:
        raise ProfileQualificationError(
            "automatic qualification requires TP=1",
            code="QUALIFICATION_TP_UNSUPPORTED",
        )
    if placement.max_model_len < 2:
        raise ProfileQualificationError(
            "automatic qualification requires at least two context tokens",
            code="QUALIFICATION_WORKLOAD_INVALID",
        )
    if len(node_ids) != placement.node_count or len(node_ids) != len(set(node_ids)):
        raise ProfileQualificationError(
            "qualification nodes must exactly match the profile node count",
            code="QUALIFICATION_NODE_COUNT",
        )
    release = session.get(ModelRelease, placement.release_id)
    if release is None:
        raise ProfileQualificationError(
            "qualification model release is missing",
            code="QUALIFICATION_RELEASE_MISSING",
        )
    if release.status == "REVOKED":
        raise ProfileQualificationError(
            "a revoked model release cannot be qualified",
            code="QUALIFICATION_RELEASE_STATE",
        )
    artifact = session.get(ModelArtifact, release.artifact_id)
    runtime = session.get(RuntimeRelease, release.runtime_id)
    if artifact is None or runtime is None:
        raise ProfileQualificationError(
            "qualification registry identity is incomplete",
            code="QUALIFICATION_REGISTRY_INCOMPLETE",
        )
    inventory = _load_inventory(session, node_ids, now=now)
    occupancy = _qualification_occupancy(
        session,
        inventory,
        exclude_run_id=qualification_run_id,
        exclude_fleet_id=reservation_fleet_id,
    )
    snapshot = build_gpu_pool_snapshot(
        inventory,
        occupied_node_ids=occupancy,
        occupancy_reasons=occupancy,
    )
    unavailable = [
        {
            "node_id": node.node_id,
            "reason": node.unavailable_reason,
        }
        for node in snapshot.nodes
        if node.selected_gpu is None
    ]
    if unavailable:
        raise ProfileQualificationError(
            "qualification node is not currently eligible",
            code="QUALIFICATION_NODE_INELIGIBLE",
            details={"nodes": unavailable},
        )
    by_node = {slot.node_id: slot for slot in snapshot.selected_slots}
    profile_by_node = {item.node_id: item.profile for item in inventory}
    rejections = []
    private_addresses: dict[str, str] = {}
    for node_id in sorted(node_ids):
        slot = by_node[node_id]
        profile = profile_by_node[node_id]
        reasons = []
        if slot.memory_mib < placement.min_gpu_memory_mib:
            reasons.append("GPU_MEMORY_INSUFFICIENT")
        architecture = _gpu_architecture(slot.compute_capability)
        if architecture is None:
            reasons.append("GPU_ARCHITECTURE_UNKNOWN")
        elif architecture not in runtime.gpu_architectures:
            reasons.append("GPU_ARCHITECTURE_UNSUPPORTED")
        if profile.disk_free_mib < placement.min_disk_free_mib:
            reasons.append("DISK_FREE_INSUFFICIENT")
        if placement.node_count > 1:
            addresses = []
            for value in profile.network.default_interface_addresses:
                try:
                    parsed = ipaddress.ip_address(value)
                except ValueError:
                    continue
                if parsed.version == 4 and parsed.is_private:
                    addresses.append(str(parsed))
            if len(set(addresses)) != 1:
                reasons.append("PRIVATE_NETWORK_IDENTITY_REQUIRED")
            else:
                private_addresses[node_id] = addresses[0]
        if reasons:
            rejections.append({"node_id": node_id, "reasons": reasons})
    if len(set(private_addresses.values())) != len(private_addresses):
        rejections.append(
            {"node_id": None, "reasons": ["PRIVATE_NETWORK_ADDRESS_DUPLICATE"]}
        )
    if rejections:
        raise ProfileQualificationError(
            "qualification nodes do not satisfy the generated profile",
            code="QUALIFICATION_STATIC_GATE_FAILED",
            details={"nodes": rejections},
        )
    if placement.node_count > 1:
        head_node_id = sorted(node_ids)[0]
        rank_node_ids = [
            head_node_id,
            *sorted(
                (node_id for node_id in node_ids if node_id != head_node_id),
                key=lambda node_id: ipaddress.ip_address(
                    private_addresses[node_id]
                ),
            ),
        ]
    else:
        rank_node_ids = sorted(node_ids)
    gpu_bindings = [
        {
            "rank": rank,
            "node_id": node_id,
            "gpu_index": by_node[node_id].gpu_index,
            "gpu_uuid": by_node[node_id].gpu_uuid,
            "memory_mib": by_node[node_id].memory_mib,
            "compute_capability": by_node[node_id].compute_capability,
        }
        for rank, node_id in enumerate(rank_node_ids)
    ]
    workload = _workload_contract(
        placement=placement,
        release=release,
        artifact=artifact,
        runtime=runtime,
        qualification_purpose=qualification_purpose,
    )
    return {
        "release": release,
        "artifact": artifact,
        "runtime": runtime,
        "inventory_fingerprint": snapshot.inventory_fingerprint,
        "rank_node_ids": rank_node_ids,
        "gpu_bindings": gpu_bindings,
        "workload": workload,
        "workload_digest": _digest(workload),
    }


def _qualification_evidence_digest(
    run: ProfileQualificationRun,
    *,
    steps: list[dict[str, Any]],
    metrics: dict[str, Any],
    executor_image: str,
    dure_commit: str,
) -> str:
    return _digest(
        {
            "policy_version": run.policy_version,
            "suite_id": run.suite_id,
            "required_steps": run.required_steps,
            "workload": run.workload,
            "workload_digest": run.workload_digest,
            "run_id": run.id,
            "release_id": run.release_id,
            "placement_id": run.placement_id,
            "node_ids": run.node_ids,
            "rank_node_ids": run.rank_node_ids,
            "gpu_bindings": run.gpu_bindings,
            "inventory_fingerprint": run.inventory_fingerprint,
            "profile_spec_digest": run.profile_spec_digest,
            "artifact_revision": run.artifact_revision,
            "artifact_manifest_digest": run.artifact_manifest_digest,
            "runtime_image": run.runtime_image,
            "runtime_vllm_version": run.runtime_vllm_version,
            "steps": steps,
            "metrics": metrics,
            "executor_image": executor_image,
            "dure_commit": dure_commit,
        }
    )


def validate_profile_qualification_evidence(
    session: Session,
    *,
    placement: PlacementProfileRecord,
    evidence: ProfileQualificationEvidence,
    run: ProfileQualificationRun,
    now=None,
    require_primary: bool = True,
    reservation_fleet_id: str | None = None,
) -> dict[str, Any]:
    purpose = _qualification_purpose(run)
    if (
        placement.origin != AUTO_PROFILE_ORIGIN
        or (require_primary and purpose != "PRIMARY")
        or (
            require_primary
            and placement.qualification_evidence_id != evidence.id
        )
        or evidence.status != "PASSED"
        or run.status != "PASSED"
        or run.failure_code is not None
        or run.evidence_id != evidence.id
        or evidence.run_id != run.id
        or run.placement_id != placement.id
        or run.release_id != placement.release_id
        or run.profile_spec_digest != placement.spec_digest
        or evidence.policy_version != run.policy_version
        or evidence.suite_id != run.suite_id
        or evidence.workload_digest != run.workload_digest
        or run.policy_version != QUALIFICATION_POLICY_VERSION
        or run.suite_id != QUALIFICATION_SUITE_ID
        or list(run.required_steps) != list(QUALIFICATION_STEPS)
        or _digest(run.workload) != run.workload_digest
        or placement.max_model_len != run.max_model_len
        or placement.max_concurrency != run.max_concurrency
        or _stored_bindings(session, run.id) != run.gpu_bindings
        or _qualification_evidence_digest(
            run,
            steps=evidence.steps,
            metrics=evidence.metrics,
            executor_image=evidence.executor_image,
            dure_commit=evidence.dure_commit,
        )
        != evidence.evidence_digest
    ):
        raise ProfileQualificationError(
            "qualification evidence binding is invalid",
            code="QUALIFICATION_EVIDENCE_INVALID",
        )
    checked_at = now or utcnow()
    context = _qualification_context(
        session,
        placement=placement,
        node_ids=list(run.node_ids),
        now=checked_at,
        qualification_purpose=run.workload.get("qualification_purpose"),
        reservation_fleet_id=reservation_fleet_id,
    )
    if (
        context["gpu_bindings"] != run.gpu_bindings
        or context["rank_node_ids"] != run.rank_node_ids
        or context["workload"] != run.workload
        or context["workload_digest"] != run.workload_digest
        or context["artifact"].revision != run.artifact_revision
        or context["artifact"].manifest_digest
        != run.artifact_manifest_digest
        or context["runtime"].image != run.runtime_image
        or context["runtime"].vllm_version != run.runtime_vllm_version
    ):
        raise ProfileQualificationError(
            "qualification evidence is stale for the current identity",
            code="QUALIFICATION_EVIDENCE_STALE",
        )
    return context


def qualification_run_dict(run: ProfileQualificationRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "release_id": run.release_id,
        "placement_id": run.placement_id,
        "status": run.status,
        "purpose": _qualification_purpose(run),
        "node_ids": list(run.node_ids),
        "rank_node_ids": list(run.rank_node_ids),
        "gpu_bindings": list(run.gpu_bindings),
        "inventory_fingerprint": run.inventory_fingerprint,
        "profile_spec_digest": run.profile_spec_digest,
        "policy_version": run.policy_version,
        "suite_id": run.suite_id,
        "workload": dict(run.workload),
        "workload_digest": run.workload_digest,
        "max_model_len": run.max_model_len,
        "max_concurrency": run.max_concurrency,
        "artifact_revision": run.artifact_revision,
        "artifact_manifest_digest": run.artifact_manifest_digest,
        "runtime_image": run.runtime_image,
        "runtime_vllm_version": run.runtime_vllm_version,
        "required_steps": list(run.required_steps),
        "evidence_id": run.evidence_id,
        "failure_code": run.failure_code,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
    }


def prepare_profile_qualification(
    session: Session,
    *,
    request_id: str,
    placement_id: str,
    node_ids: list[str],
    apply: bool,
    purpose: str = "PRIMARY",
) -> tuple[dict[str, Any], bool]:
    request_id = _canonical_uuid(request_id, field="request_id")
    _canonical_uuid(placement_id, field="placement_id")
    if type(apply) is not bool:
        raise ValueError("apply must be a boolean")
    if type(purpose) is not str or purpose not in QUALIFICATION_PURPOSES:
        raise ValueError("unsupported qualification purpose")
    for node_id in node_ids:
        _canonical_uuid(node_id, field="node_id")
    if apply:
        lock_fleet_reservation_gate(session)
    placement = session.scalar(
        select(PlacementProfileRecord)
        .where(PlacementProfileRecord.id == placement_id)
        .with_for_update()
    )
    if placement is None:
        raise ProfileQualificationError(
            "placement profile does not exist",
            code="QUALIFICATION_PROFILE_NOT_FOUND",
        )
    existing = session.get(ProfileQualificationRun, request_id)
    if existing is not None:
        if (
            existing.placement_id != placement.id
            or list(existing.node_ids) != sorted(node_ids)
            or existing.release_id != placement.release_id
            or _qualification_purpose(existing) != purpose
            or _stored_bindings(session, existing.id) != existing.gpu_bindings
        ):
            raise ProfileQualificationError(
                "qualification request identity conflicts with the stored run",
                code="QUALIFICATION_REQUEST_CONFLICT",
            )
        return qualification_run_dict(existing), False
    now = utcnow()
    if purpose == "PRIMARY":
        if (
            placement.status != "DRAFT"
            or placement.qualification_evidence_id is not None
        ):
            raise ProfileQualificationError(
                "primary qualification can start only from an unevidenced DRAFT profile",
                code="QUALIFICATION_PROFILE_STATE",
                details={"status": placement.status},
            )
    elif (
        placement.status not in {"VALIDATED", "ACTIVE"}
        or placement.qualification_evidence_id is None
    ):
        raise ProfileQualificationError(
            "supplementary qualification requires an evidenced VALIDATED or ACTIVE profile",
            code="QUALIFICATION_PROFILE_STATE",
            details={"status": placement.status},
        )
    context = _qualification_context(
        session,
        placement=placement,
        node_ids=node_ids,
        now=now,
        qualification_purpose=purpose,
    )
    planned = {
        "id": request_id,
        "release_id": placement.release_id,
        "placement_id": placement.id,
        "status": "QUALIFYING",
        "purpose": purpose,
        "node_ids": sorted(node_ids),
        "rank_node_ids": context["rank_node_ids"],
        "gpu_bindings": context["gpu_bindings"],
        "inventory_fingerprint": context["inventory_fingerprint"],
        "profile_spec_digest": placement.spec_digest,
        "policy_version": QUALIFICATION_POLICY_VERSION,
        "suite_id": QUALIFICATION_SUITE_ID,
        "workload": context["workload"],
        "workload_digest": context["workload_digest"],
        "max_model_len": placement.max_model_len,
        "max_concurrency": placement.max_concurrency,
        "artifact_revision": context["artifact"].revision,
        "artifact_manifest_digest": context["artifact"].manifest_digest,
        "runtime_image": context["runtime"].image,
        "runtime_vllm_version": context["runtime"].vllm_version,
        "required_steps": list(QUALIFICATION_STEPS),
        "evidence_id": None,
        "failure_code": None,
    }
    if not apply:
        return planned, False
    run = ProfileQualificationRun(
        id=request_id,
        release_id=placement.release_id,
        placement_id=placement.id,
        status="QUALIFYING",
        node_ids=planned["node_ids"],
        rank_node_ids=planned["rank_node_ids"],
        gpu_bindings=planned["gpu_bindings"],
        inventory_fingerprint=planned["inventory_fingerprint"],
        profile_spec_digest=planned["profile_spec_digest"],
        policy_version=planned["policy_version"],
        suite_id=planned["suite_id"],
        required_steps=planned["required_steps"],
        workload=planned["workload"],
        workload_digest=planned["workload_digest"],
        max_model_len=planned["max_model_len"],
        max_concurrency=planned["max_concurrency"],
        artifact_revision=planned["artifact_revision"],
        artifact_manifest_digest=planned["artifact_manifest_digest"],
        runtime_image=planned["runtime_image"],
        runtime_vllm_version=planned["runtime_vllm_version"],
        created_at=now,
        updated_at=now,
    )
    session.add(run)
    session.flush()
    session.add_all(
        [
            ProfileQualificationBinding(
                run_id=run.id,
                rank=binding["rank"],
                node_id=binding["node_id"],
                gpu_index=binding["gpu_index"],
                gpu_uuid=binding["gpu_uuid"],
                memory_mib=binding["memory_mib"],
                compute_capability=binding["compute_capability"],
            )
            for binding in planned["gpu_bindings"]
        ]
    )
    if purpose == "PRIMARY":
        placement.status = "QUALIFYING"
    session.add(
        AuditEvent(
            actor="admin",
            action="placement_profile.qualification.start",
            target=placement.id,
            outcome="success",
            detail={
                "run_id": run.id,
                "node_ids": planned["node_ids"],
                "purpose": purpose,
            },
        )
    )
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise ProfileQualificationError(
            "qualification run identity already exists",
            code="QUALIFICATION_REQUEST_CONFLICT",
        ) from exc
    return qualification_run_dict(run), True


def _validate_steps(steps: list[dict[str, Any]]) -> tuple[list[dict], list[str]]:
    if type(steps) is not list or len(steps) != len(QUALIFICATION_STEPS):
        raise ValueError("qualification evidence requires all closed steps")
    normalized = []
    failure_codes = []
    for expected, step in zip(QUALIFICATION_STEPS, steps):
        if type(step) is not dict or set(step) != {
            "step_id",
            "status",
            "failure_code",
        }:
            raise ValueError("qualification step shape is invalid")
        if step["step_id"] != expected or step["status"] not in {
            "PASSED",
            "FAILED",
        }:
            raise ValueError("qualification steps must use canonical order and status")
        failure_code = step["failure_code"]
        if step["status"] == "PASSED":
            if failure_code is not None:
                raise ValueError("passing qualification step cannot have failure_code")
        elif failure_code != QUALIFICATION_FAILURE_BY_STEP[expected]:
            raise ValueError(
                "failed qualification step requires its canonical failure_code"
            )
        if failure_code is not None:
            failure_codes.append(failure_code)
        normalized.append(dict(step))
    return normalized, failure_codes


def _finite_metric(metrics: dict[str, Any], name: str) -> float:
    value = metrics.get(name)
    if type(value) not in {int, float} or not math.isfinite(value):
        raise ValueError(f"qualification metric {name} must be finite")
    return float(value)


def _metric_failures(
    placement: PlacementProfileRecord,
    run: ProfileQualificationRun,
    metrics: dict[str, Any],
) -> list[str]:
    expected = {
        "model_load_seconds",
        "request_count",
        "restart_count",
        "max_model_len",
        "concurrency",
        "input_tokens",
        "output_tokens",
        "warmup_requests",
        "ttft_p95_ms",
        "tpot_p95_ms",
        "e2e_p95_ms",
        "throughput_tps",
        "success_rate",
        "vram_headroom_pct",
        "network_bandwidth_mbps",
        "network_rtt_ms",
        "packet_loss_pct",
        "nccl_all_reduce_ok",
    }
    if type(metrics) is not dict or set(metrics) != expected:
        raise ValueError("qualification metrics do not match the closed schema")
    failures = []
    if _finite_metric(metrics, "model_load_seconds") <= 0:
        failures.append("MODEL_LOAD_FAILED")
    request_count = metrics["request_count"]
    restart_count = metrics["restart_count"]
    if type(request_count) is not int or request_count <= 0:
        raise ValueError("qualification request_count must be positive")
    if type(restart_count) is not int or restart_count < 1:
        failures.append("RESTART_STABILITY_FAILED")
    integer_contract = {
        "max_model_len": run.max_model_len,
        "concurrency": run.max_concurrency,
        "input_tokens": run.workload["input_tokens"],
        "output_tokens": run.workload["output_tokens"],
        "warmup_requests": run.workload["warmup_requests"],
    }
    for name, required in integer_contract.items():
        if type(metrics[name]) is not int or metrics[name] != required:
            raise ValueError(
                f"qualification metric {name} does not match the frozen workload"
            )
    if request_count < run.workload["minimum_request_count"]:
        failures.append("CONTEXT_CONCURRENCY_FAILED")
    bounds = (
        ("ttft_p95_ms", placement.max_ttft_p95_ms, "CONTEXT_CONCURRENCY_FAILED"),
        ("tpot_p95_ms", placement.max_tpot_p95_ms, "CONTEXT_CONCURRENCY_FAILED"),
        ("e2e_p95_ms", placement.max_e2e_p95_ms, "CONTEXT_CONCURRENCY_FAILED"),
    )
    for name, maximum, code in bounds:
        if _finite_metric(metrics, name) <= 0 or metrics[name] > maximum:
            failures.append(code)
    if _finite_metric(metrics, "throughput_tps") < placement.min_throughput_tps:
        failures.append("CONTEXT_CONCURRENCY_FAILED")
    success_rate = _finite_metric(metrics, "success_rate")
    headroom = _finite_metric(metrics, "vram_headroom_pct")
    if not 0 <= success_rate <= 1 or success_rate < placement.min_success_rate:
        failures.append("SHORT_INFERENCE_FAILED")
    if not 0 <= headroom <= 100 or headroom < placement.min_vram_headroom_pct:
        failures.append("CAPACITY_ESTIMATE_FAILED")
    network_names = (
        "network_bandwidth_mbps",
        "network_rtt_ms",
        "packet_loss_pct",
    )
    if placement.node_count > 1:
        for name in network_names:
            _finite_metric(metrics, name)
        if (
            metrics["network_bandwidth_mbps"] < placement.min_bandwidth_mbps
            or metrics["network_rtt_ms"] > placement.max_rtt_ms
            or metrics["packet_loss_pct"] > placement.max_packet_loss_pct
            or metrics["nccl_all_reduce_ok"] is not True
        ):
            failures.append("NETWORK_NCCL_FAILED")
    elif any(metrics[name] is not None for name in network_names) or metrics[
        "nccl_all_reduce_ok"
    ] is not None:
        raise ValueError("single-node qualification network metrics must be null")
    return sorted(set(failures))


def register_profile_qualification_evidence(
    session: Session,
    *,
    run_id: str,
    steps: list[dict[str, Any]],
    metrics: dict[str, Any],
    executor_image: str,
    dure_commit: str,
) -> tuple[ProfileQualificationEvidence, ProfileQualificationRun, bool]:
    _canonical_uuid(run_id, field="run_id")
    validate_digest_pinned_runtime_image(executor_image)
    if re.fullmatch(r"[0-9a-f]{40,64}", dure_commit) is None:
        raise ValueError("dure_commit must be an immutable commit identity")
    normalized_steps, step_failures = _validate_steps(steps)
    run = session.scalar(
        select(ProfileQualificationRun)
        .where(ProfileQualificationRun.id == run_id)
        .with_for_update()
    )
    if run is None:
        raise ProfileQualificationError(
            "qualification run does not exist",
            code="QUALIFICATION_RUN_NOT_FOUND",
        )
    placement = session.scalar(
        select(PlacementProfileRecord)
        .where(PlacementProfileRecord.id == run.placement_id)
        .with_for_update()
    )
    if placement is None:
        raise ProfileQualificationError(
            "qualification profile does not exist",
            code="QUALIFICATION_PROFILE_NOT_FOUND",
        )
    metric_failures = _metric_failures(placement, run, metrics)
    evidence_digest = _qualification_evidence_digest(
        run,
        steps=normalized_steps,
        metrics=metrics,
        executor_image=executor_image,
        dure_commit=dure_commit,
    )
    existing = session.scalar(
        select(ProfileQualificationEvidence).where(
            ProfileQualificationEvidence.run_id == run.id
        )
    )
    if existing is not None:
        if existing.evidence_digest != evidence_digest:
            raise ProfileQualificationError(
                "qualification run already has different evidence",
                code="QUALIFICATION_EVIDENCE_CONFLICT",
            )
        return existing, run, False
    purpose = _qualification_purpose(run)
    valid_profile_state = (
        placement.status == "QUALIFYING"
        if purpose == "PRIMARY"
        else placement.status in {"VALIDATED", "ACTIVE"}
        and placement.qualification_evidence_id is not None
    )
    if run.status != "QUALIFYING" or not valid_profile_state:
        raise ProfileQualificationError(
            "qualification run is not accepting evidence",
            code="QUALIFICATION_RUN_STATE",
        )
    if (
        run.policy_version != QUALIFICATION_POLICY_VERSION
        or run.suite_id != QUALIFICATION_SUITE_ID
        or list(run.required_steps) != list(QUALIFICATION_STEPS)
        or _digest(run.workload) != run.workload_digest
    ):
        raise ProfileQualificationError(
            "qualification run uses a stale policy or workload contract",
            code="QUALIFICATION_POLICY_STALE",
        )
    if _stored_bindings(session, run.id) != run.gpu_bindings:
        raise ProfileQualificationError(
            "qualification GPU binding records are inconsistent",
            code="QUALIFICATION_BINDING_INVALID",
        )
    now = utcnow()
    context = _qualification_context(
        session,
        placement=placement,
        node_ids=list(run.node_ids),
        now=now,
        qualification_run_id=run.id,
        qualification_purpose=run.workload.get("qualification_purpose"),
    )
    if (
        context["gpu_bindings"] != run.gpu_bindings
        or context["rank_node_ids"] != run.rank_node_ids
        or context["workload"] != run.workload
        or context["workload_digest"] != run.workload_digest
        or placement.max_model_len != run.max_model_len
        or placement.max_concurrency != run.max_concurrency
        or placement.spec_digest != run.profile_spec_digest
        or context["artifact"].revision != run.artifact_revision
        or context["artifact"].manifest_digest != run.artifact_manifest_digest
        or context["runtime"].image != run.runtime_image
        or context["runtime"].vllm_version != run.runtime_vllm_version
    ):
        raise ProfileQualificationError(
            "qualification inventory or registry identity changed",
            code="QUALIFICATION_EVIDENCE_STALE",
        )
    failures = sorted(set(step_failures + metric_failures))
    status = "FAILED" if failures else "PASSED"
    evidence = ProfileQualificationEvidence(
        id=str(uuid.uuid5(uuid.NAMESPACE_URL, evidence_digest)),
        run_id=run.id,
        status=status,
        steps=normalized_steps,
        metrics=metrics,
        policy_version=run.policy_version,
        suite_id=run.suite_id,
        workload_digest=run.workload_digest,
        executor_image=executor_image,
        dure_commit=dure_commit,
        evidence_digest=evidence_digest,
        created_at=now,
    )
    session.add(evidence)
    session.flush()
    run.status = status
    run.evidence_id = evidence.id
    run.failure_code = failures[0] if failures else None
    run.updated_at = now
    if status == "PASSED" and purpose == "PRIMARY":
        placement.status = "VALIDATED"
        placement.qualification_evidence_id = evidence.id
        placement.qualified_at = now
    elif status == "FAILED" and purpose == "PRIMARY":
        placement.status = "DRAFT"
    session.add(
        AuditEvent(
            actor="admin",
            action="placement_profile.qualification.finish",
            target=placement.id,
            outcome="success" if status == "PASSED" else "failure",
            detail={
                "run_id": run.id,
                "evidence_id": evidence.id,
                "failure_codes": failures,
                "purpose": purpose,
            },
        )
    )
    session.commit()
    return evidence, run, True


def qualification_evidence_dict(
    evidence: ProfileQualificationEvidence,
) -> dict[str, Any]:
    return {
        "id": evidence.id,
        "run_id": evidence.run_id,
        "status": evidence.status,
        "steps": evidence.steps,
        "metrics": evidence.metrics,
        "policy_version": evidence.policy_version,
        "suite_id": evidence.suite_id,
        "workload_digest": evidence.workload_digest,
        "executor_image": evidence.executor_image,
        "dure_commit": evidence.dure_commit,
        "evidence_digest": evidence.evidence_digest,
        "created_at": evidence.created_at,
    }


def activate_validated_profile(
    session: Session, placement_id: str
) -> tuple[PlacementProfileRecord, bool]:
    _canonical_uuid(placement_id, field="placement_id")
    placement = session.scalar(
        select(PlacementProfileRecord)
        .where(PlacementProfileRecord.id == placement_id)
        .with_for_update()
    )
    if placement is None:
        raise ProfileQualificationError(
            "placement profile does not exist",
            code="QUALIFICATION_PROFILE_NOT_FOUND",
        )
    if placement.origin != AUTO_PROFILE_ORIGIN:
        raise ProfileQualificationError(
            "manual placement profiles do not use qualification activation",
            code="QUALIFICATION_PROFILE_UNSUPPORTED",
        )
    if placement.status == "ACTIVE":
        return placement, False
    if placement.status != "VALIDATED" or not placement.qualification_evidence_id:
        raise ProfileQualificationError(
            "only an evidenced VALIDATED profile can be activated",
            code="QUALIFICATION_PROFILE_STATE",
            details={"status": placement.status},
        )
    evidence = session.get(
        ProfileQualificationEvidence, placement.qualification_evidence_id
    )
    run = (
        session.get(ProfileQualificationRun, evidence.run_id)
        if evidence is not None
        else None
    )
    if evidence is None or run is None:
        raise ProfileQualificationError(
            "qualification evidence binding is invalid",
            code="QUALIFICATION_EVIDENCE_INVALID",
        )
    now = utcnow()
    validate_profile_qualification_evidence(
        session,
        placement=placement,
        evidence=evidence,
        run=run,
        now=now,
    )
    placement.status = "ACTIVE"
    placement.activated_at = now
    session.add(
        AuditEvent(
            actor="admin",
            action="placement_profile.activate",
            target=placement.id,
            outcome="success",
            detail={"evidence_id": evidence.id, "run_id": run.id},
        )
    )
    session.commit()
    return placement, True


def cancel_profile_qualification(
    session: Session, run_id: str
) -> tuple[ProfileQualificationRun, bool]:
    _canonical_uuid(run_id, field="run_id")
    run = session.scalar(
        select(ProfileQualificationRun)
        .where(ProfileQualificationRun.id == run_id)
        .with_for_update()
    )
    if run is None:
        raise ProfileQualificationError(
            "qualification run does not exist",
            code="QUALIFICATION_RUN_NOT_FOUND",
        )
    if run.status == "CANCELED":
        return run, False
    if run.status != "QUALIFYING":
        raise ProfileQualificationError(
            "only a qualifying run can be canceled",
            code="QUALIFICATION_RUN_STATE",
        )
    placement = session.scalar(
        select(PlacementProfileRecord)
        .where(PlacementProfileRecord.id == run.placement_id)
        .with_for_update()
    )
    purpose = _qualification_purpose(run)
    valid_profile_state = (
        placement is not None
        and (
            placement.status == "QUALIFYING"
            if purpose == "PRIMARY"
            else placement.status in {"VALIDATED", "ACTIVE"}
            and placement.qualification_evidence_id is not None
        )
    )
    if not valid_profile_state:
        raise ProfileQualificationError(
            "qualification profile state is inconsistent",
            code="QUALIFICATION_PROFILE_STATE",
        )
    run.status = "CANCELED"
    run.failure_code = "QUALIFICATION_CANCELED"
    run.updated_at = utcnow()
    if purpose == "PRIMARY":
        placement.status = "DRAFT"
    session.add(
        AuditEvent(
            actor="admin",
            action="placement_profile.qualification.cancel",
            target=placement.id,
            outcome="success",
            detail={"run_id": run.id, "purpose": purpose},
        )
    )
    session.commit()
    return run, True
