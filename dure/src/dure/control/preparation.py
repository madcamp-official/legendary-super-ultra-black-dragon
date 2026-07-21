from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dure.artifact_prepare import (
    PREPARATION_FAILURE_CODES,
    PREPARE_IMAGE_TASK,
    PREPARE_MODEL_TASK,
    validate_digest_pinned_runtime_image,
    validate_preparation_result,
)
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_VERIFICATION_VERSION,
)
from dure.models import DeploymentPlan, NodeProfile
from dure.stage_cache import (
    STAGE_CACHE_VERIFICATION_VERSION,
    StageCacheError,
    StageCacheIdentity,
)
from dure.task import TaskStatus, TaskType

from .cache_lifecycle import (
    ArtifactCacheIdentity,
    ArtifactCacheLifecycleError,
    record_preparation_success,
    record_verification_failure,
    require_ready_cache,
)
from .models import (
    ArtifactFileChunk,
    ArtifactManifest,
    ArtifactManifestFile,
    ArtifactPreparation,
    ArtifactPreparationAttempt,
    ArtifactPreparationNode,
    AuditEvent,
    Deployment,
    DeploymentOperation,
    DeploymentRecommendationRecord,
    ModelArtifact,
    ModelRelease,
    Node,
    NodeProfileRecord,
    RuntimeRelease,
    Task,
    utcnow,
)
from .recommendation import RecommendationError, evaluate_deployment_recommendation
from .stage_artifacts import (
    StageArtifactConflictError,
    StageArtifactNotFoundError,
    validated_stage_artifact_projection,
)


PROFILE_FRESH_FOR = timedelta(seconds=90)
NODE_ONLINE_FOR = timedelta(seconds=30)
MINIMUM_PREPARATION_AGENT = (0, 3, 16)
MINIMUM_STAGE_PREPARATION_AGENT = (0, 3, 19)

PREPARATION_TASK_TYPES = frozenset(
    {TaskType.PREPARE_MODEL.value, TaskType.PREPARE_IMAGE.value}
)
PREPARATION_TERMINAL_FAILURE_CODES = frozenset(
    {
        *PREPARATION_FAILURE_CODES,
        "PREPARATION_LEASE_EXPIRED",
        "PREPARATION_TASK_CANCELED",
        "PREPARATION_NODE_REVOKED",
        "PREPARATION_RESULT_REJECTED",
    }
)

# Terminal verified bytes and best-effort download high-water have different
# trust semantics.  Keep both sources explicit so API consumers never mistake
# progress telemetry for completed immutable-manifest verification.
PREPARATION_PROGRESS_BYTES_SOURCE = "COMPLETED_MODEL_VERIFICATION"
PREPARATION_DOWNLOAD_BYTES_SOURCE = "MODEL_PREPARATION_HIGH_WATER"
PREPARATION_DOWNLOAD_BYTES_DERIVED_SOURCE = (
    "DERIVED_FROM_COMPLETED_MODEL_VERIFICATION"
)
PREPARATION_DOWNLOAD_BYTES_NOT_STARTED_SOURCE = "NOT_STARTED"
PREPARATION_DOWNLOAD_BYTES_UNAVAILABLE_SOURCE = "UNAVAILABLE"
PREPARATION_DOWNLOAD_BYTES_MIXED_SOURCE = "MIXED"
PREPARATION_PROGRESS_STAGES = frozenset(
    {"MODEL", "IMAGE", "COMPLETE", "FAILED"}
)

_SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z.-]+)?$")


class ArtifactPreparationError(ValueError):
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
        return {
            "code": self.code,
            "message": str(self),
            "details": self.details,
        }


class ArtifactPreparationNotFoundError(ArtifactPreparationError):
    def __init__(
        self,
        message: str = "artifact preparation not found",
        *,
        code: str = "ARTIFACT_PREPARATION_NOT_FOUND",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code=code, details=details)


def _aware(value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _iso(value: datetime | None) -> str | None:
    normalized = _aware(value)
    return normalized.isoformat() if normalized is not None else None


def _canonical_uuid(value: str, field: str) -> str:
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except (AttributeError, TypeError, ValueError) as exc:
        raise ArtifactPreparationError(
            f"{field} must be a canonical UUID",
            code="PREPARATION_REQUEST_INVALID",
        ) from exc
    return value


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _agent_version_supported(
    value: str,
    *,
    minimum: tuple[int, int, int] = MINIMUM_PREPARATION_AGENT,
) -> bool:
    match = _SEMVER.fullmatch(value)
    return bool(match and tuple(int(item) for item in match.groups()) >= minimum)


def _node_online(node: Node, now: datetime) -> bool:
    last_seen = _aware(node.last_seen)
    return last_seen is not None and now - last_seen <= NODE_ONLINE_FOR


def _profile_fresh(record: NodeProfileRecord, now: datetime) -> bool:
    updated_at = _aware(record.updated_at)
    return updated_at is not None and now - updated_at <= PROFILE_FRESH_FOR


def _workload_is_active(profile: NodeProfile) -> bool:
    inactive = {"created", "exited", "stopped", "dead", "removed"}
    return any(item.status.strip().lower() not in inactive for item in profile.workloads)


def _stable_profile_identity(value: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "hostname",
        "os_name",
        "os_version",
        "kernel",
        "architecture",
        "virtualization",
        "cpu_model",
        "cpu_count",
        "memory_mib",
        "swap_mib",
        "disk_total_mib",
        "gpus",
        "network",
        "runtime",
    )
    def canonical(item: Any) -> Any:
        if isinstance(item, dict):
            return {key: canonical(item[key]) for key in sorted(item)}
        if isinstance(item, list):
            normalized = [canonical(child) for child in item]
            return sorted(
                normalized,
                key=lambda child: json.dumps(
                    child, sort_keys=True, separators=(",", ":")
                ),
            )
        return item

    return {field: canonical(value.get(field)) for field in fields}


def _lock_model_release_transitions(session: Session) -> None:
    """Keep a release status stable without taking a release row lock.

    Preparation serializes host mutations by locking nodes first.  Existing
    registry and benchmark paths lock model releases before nodes, so taking a
    release row lock here would invert that order.  PostgreSQL's table SHARE
    lock blocks status writes while remaining compatible with the ROW SHARE
    table lock acquired by ``SELECT ... FOR UPDATE``.  SQLite serializes the
    unit-test writes itself and has no equivalent table-lock statement.
    """

    if session.get_bind().dialect.name == "postgresql":
        session.execute(text("LOCK TABLE model_releases IN SHARE MODE"))


def _stage_preparation_projection(
    session: Session,
    deployment: Deployment,
    selected: dict[str, Any],
    artifact: ModelArtifact,
    *,
    artifact_set_digest: str | None,
) -> dict[str, Any] | None:
    """Revalidate the delivery mode already fixed by the accepted generation.

    The optional request digest is only an equality assertion for compatibility
    with the PR7 CLI.  It can no longer change a FULL generation into STAGE or
    select a different STAGE variant after recommendation acceptance.
    """

    plan = deployment.plan
    plan_cache_kind = plan.get("model_cache_kind")
    plan_stage = plan.get("stage_artifact")
    if plan_cache_kind != MODEL_CACHE_KIND_STAGE:
        if artifact_set_digest is not None:
            raise ArtifactPreparationError(
                "the accepted generation does not select a stage artifact",
                code="PREPARATION_STAGE_VARIANT_MISMATCH",
                details={"deployment_id": deployment.id},
            )
        return None
    selected_digest = (
        plan_stage.get("artifact_set_digest")
        if type(plan_stage) is dict
        else None
    )
    if artifact_set_digest is not None and artifact_set_digest != selected_digest:
        raise ArtifactPreparationError(
            "the requested stage artifact differs from the accepted generation",
            code="PREPARATION_STAGE_VARIANT_MISMATCH",
            details={
                "deployment_id": deployment.id,
                "artifact_set_digest": artifact_set_digest,
            },
        )
    artifact_set_digest = selected_digest
    if (
        type(artifact_set_digest) is not str
        or re.fullmatch(r"sha256:[0-9a-f]{64}", artifact_set_digest) is None
    ):
        raise ArtifactPreparationError(
            "stage artifact set digest is invalid",
            code="PREPARATION_STAGE_VARIANT_INVALID",
            details={"deployment_id": deployment.id},
        )
    try:
        projection = validated_stage_artifact_projection(
            session, artifact_set_digest
        )
    except StageArtifactNotFoundError as exc:
        raise ArtifactPreparationError(
            "requested validated stage artifact variant is unavailable",
            code="PREPARATION_STAGE_VARIANT_UNAVAILABLE",
            details={"artifact_set_digest": artifact_set_digest},
        ) from exc
    except (StageArtifactConflictError, ValueError) as exc:
        raise ArtifactPreparationError(
            "requested stage artifact variant failed immutable validation",
            code="PREPARATION_STAGE_VARIANT_INVALID",
            details={"artifact_set_digest": artifact_set_digest},
        ) from exc

    assignments = plan.get("assignments")
    expected_contract = {
        "artifact_set_digest": artifact_set_digest,
        "source_manifest_digest": artifact.manifest_digest,
        "runtime_image": selected.get("runtime_image"),
        "vllm_version": plan.get("runtime_vllm_version"),
        "quantization": artifact.quantization,
        "tensor_parallel_size": plan.get("tensor_parallel_size"),
        "pipeline_parallel_size": plan.get("pipeline_parallel_size"),
        "loader_format": "VLLM_SHARDED_STATE_V1",
    }
    if (
        type(projection) is not dict
        or any(projection.get(key) != value for key, value in expected_contract.items())
        or type(plan_stage) is not dict
        or any(
            plan_stage.get(key) != projection.get(key)
            for key in (
                "artifact_set_digest",
                "contract_identity_digest",
                "source_manifest_digest",
                "runtime_image",
                "vllm_version",
                "exporter_build_digest",
                "architecture",
                "quantization",
                "tensor_parallel_size",
                "pipeline_parallel_size",
                "loader_format",
            )
        )
        or selected.get("model_cache_kind") != MODEL_CACHE_KIND_STAGE
        or selected.get("stage_artifact") != plan_stage
        or projection.get("architecture") != "Qwen2ForCausalLM"
        or plan.get("execution_backend") != "VLLM_RAY_PP_V1"
        or type(assignments) is not list
        or len(assignments) != plan.get("pipeline_parallel_size")
    ):
        raise ArtifactPreparationError(
            "stage artifact variant does not match the accepted generation",
            code="PREPARATION_STAGE_VARIANT_MISMATCH",
            details={"artifact_set_digest": artifact_set_digest},
        )

    assignment_by_pipeline_rank: dict[int, dict[str, Any]] = {}
    for assignment in assignments:
        if type(assignment) is not dict:
            raise ArtifactPreparationError(
                "accepted generation has an invalid stage assignment",
                code="PREPARATION_STAGE_VARIANT_MISMATCH",
            )
        pipeline_rank = assignment.get("pipeline_rank")
        if type(pipeline_rank) is not int or pipeline_rank in assignment_by_pipeline_rank:
            raise ArtifactPreparationError(
                "accepted generation has duplicate or invalid pipeline ranks",
                code="PREPARATION_STAGE_VARIANT_MISMATCH",
            )
        assignment_by_pipeline_rank[pipeline_rank] = assignment

    stages = projection.get("ranks")
    if type(stages) is not list or len(stages) != len(assignments):
        raise ArtifactPreparationError(
            "stage artifact rank set is incomplete",
            code="PREPARATION_STAGE_VARIANT_MISMATCH",
            details={"artifact_set_digest": artifact_set_digest},
        )
    bindings: list[dict[str, Any]] = []
    seen_nodes: set[str] = set()
    seen_manifests: set[str] = set()
    for expected_rank, stage in enumerate(stages):
        if type(stage) is not dict:
            raise ArtifactPreparationError(
                "stage artifact rank binding is invalid",
                code="PREPARATION_STAGE_VARIANT_MISMATCH",
            )
        pipeline_rank = stage.get("pipeline_rank")
        tensor_rank = stage.get("tensor_rank")
        assignment = assignment_by_pipeline_rank.get(pipeline_rank)
        node_id = assignment.get("node_id") if assignment is not None else None
        manifest_digest = stage.get("manifest_digest")
        if (
            stage.get("rank") != expected_rank
            or tensor_rank != 0
            or assignment is None
            or assignment.get("expected_runtime_rank") != expected_rank
            or assignment.get("stage_manifest_digest") != manifest_digest
            or assignment.get("stage_tensor_keys_digest")
            != stage.get("tensor_keys_digest")
            or type(node_id) is not str
            or node_id in seen_nodes
            or type(manifest_digest) is not str
            or manifest_digest in seen_manifests
            or re.fullmatch(r"sha256:[0-9a-f]{64}", manifest_digest) is None
            or type(stage.get("tensor_keys_digest")) is not str
            or re.fullmatch(
                r"sha256:[0-9a-f]{64}", stage["tensor_keys_digest"]
            )
            is None
            or type(stage.get("total_size_bytes")) is not int
            or stage["total_size_bytes"] <= 0
            or type(stage.get("file_count")) is not int
            or stage["file_count"] <= 0
        ):
            raise ArtifactPreparationError(
                "stage artifact rank does not match the accepted node assignment",
                code="PREPARATION_STAGE_VARIANT_MISMATCH",
                details={"artifact_set_digest": artifact_set_digest},
            )
        seen_nodes.add(node_id)
        seen_manifests.add(manifest_digest)
        bindings.append(
            {
                "node_id": node_id,
                "rank": expected_rank,
                "pipeline_rank": pipeline_rank,
                "tensor_rank": tensor_rank,
                "manifest_digest": manifest_digest,
                "tensor_key_count": stage.get("tensor_key_count"),
                "tensor_keys_digest": stage["tensor_keys_digest"],
                "weight_size_bytes": stage.get("weight_size_bytes"),
                "total_size_bytes": stage["total_size_bytes"],
                "file_count": stage["file_count"],
            }
        )
    if set(assignment_by_pipeline_rank) != set(range(len(bindings))):
        raise ArtifactPreparationError(
            "stage artifact ranks do not cover the accepted topology",
            code="PREPARATION_STAGE_VARIANT_MISMATCH",
        )
    return {
        **{
            key: projection[key]
            for key in (
                "artifact_set_digest",
                "contract_identity_digest",
                "source_manifest_digest",
                "runtime_image",
                "vllm_version",
                "exporter_build_digest",
                "architecture",
                "quantization",
                "tensor_parallel_size",
                "pipeline_parallel_size",
                "loader_format",
            )
        },
        "node_bindings": bindings,
    }


def _selected_context(
    session: Session,
    deployment: Deployment,
    *,
    now: datetime,
    stage_artifact_set_digest: str | None = None,
    preparation_id: str | None = None,
    revalidate_inventory: bool = True,
    disk_node_ids: set[str] | None = None,
) -> tuple[
    DeploymentRecommendationRecord,
    dict[str, Any],
    ModelArtifact,
    ArtifactManifest,
    list[Node],
    dict[str, Any] | None,
]:
    if deployment.source_recommendation_id is None:
        raise ArtifactPreparationError(
            "only an accepted recommendation generation can be prepared",
            code="PREPARATION_RECOMMENDATION_REQUIRED",
            details={"deployment_id": deployment.id},
        )
    recommendation = session.scalar(
        select(DeploymentRecommendationRecord)
        .where(
            DeploymentRecommendationRecord.id
            == deployment.source_recommendation_id
        )
    )
    if recommendation is None:
        raise ArtifactPreparationError(
            "accepted recommendation snapshot is missing",
            code="PREPARATION_RECOMMENDATION_MISSING",
            details={"deployment_id": deployment.id},
        )
    selected = recommendation.recommendation_snapshot.get("selected")
    if not isinstance(selected, dict):
        raise ArtifactPreparationError(
            "accepted recommendation has no selected artifact",
            code="PREPARATION_RECOMMENDATION_INVALID",
            details={"deployment_id": deployment.id},
        )
    node_ids = selected.get("node_ids")
    assignment_ids = [
        item.get("node_id")
        for item in deployment.plan.get("assignments", [])
        if isinstance(item, dict)
    ]
    if (
        not isinstance(node_ids, list)
        or not node_ids
        or node_ids != sorted(set(node_ids))
        or sorted(assignment_ids) != node_ids
        or len(assignment_ids) != len(node_ids)
    ):
        raise ArtifactPreparationError(
            "accepted node assignment is invalid",
            code="PREPARATION_ASSIGNMENT_INVALID",
            details={"deployment_id": deployment.id},
        )

    nodes = list(
        session.scalars(
            select(Node)
            .where(Node.id.in_(node_ids))
            .order_by(Node.id)
            .with_for_update()
        )
    )
    if [node.id for node in nodes] != node_ids:
        raise ArtifactPreparationError(
            "an assigned node no longer exists",
            code="PREPARATION_NODE_MISSING",
            details={"node_ids": node_ids},
        )

    # Nodes are the first mutation lock everywhere in the preparation path.
    # Stabilize release transitions without adding a Node -> ModelRelease row
    # lock edge that could deadlock with benchmark and promotion paths.
    _lock_model_release_transitions(session)
    release = session.scalar(
        select(ModelRelease)
        .where(ModelRelease.id == selected.get("model_release_id"))
        .execution_options(populate_existing=True)
    )
    if release is None or release.status != "ACTIVE":
        raise ArtifactPreparationError(
            "accepted recommendation release is no longer active",
            code="PREPARATION_RECOMMENDATION_STALE",
            details={
                "deployment_id": deployment.id,
                "release_status": release.status if release is not None else None,
            },
        )
    profile_records = {
        item.node_id: item
        for item in session.scalars(
            select(NodeProfileRecord)
            .where(NodeProfileRecord.node_id.in_(node_ids))
            .with_for_update()
        )
    }
    stored_inventory = {
        item.get("node_id"): item
        for item in recommendation.inventory_snapshot
        if isinstance(item, dict) and isinstance(item.get("node_id"), str)
    }
    for node in nodes:
        if not node.approved:
            raise ArtifactPreparationError(
                "an assigned node is not approved",
                code="PREPARATION_NODE_UNAPPROVED",
                details={"node_id": node.id},
            )
        if not _node_online(node, now):
            raise ArtifactPreparationError(
                "an assigned node is offline",
                code="PREPARATION_NODE_OFFLINE",
                details={"node_id": node.id},
            )
        if not _agent_version_supported(node.agent_version):
            raise ArtifactPreparationError(
                "an assigned node Agent cannot execute preparation tasks",
                code="PREPARATION_AGENT_UNSUPPORTED",
                details={"node_id": node.id, "minimum_version": "0.3.16"},
            )
        profile_record = profile_records.get(node.id)
        if profile_record is None or not _profile_fresh(profile_record, now):
            raise ArtifactPreparationError(
                "an assigned node profile is missing or stale",
                code="PREPARATION_PROFILE_STALE",
                details={"node_id": node.id},
            )
        try:
            profile = NodeProfile.from_dict(profile_record.profile)
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactPreparationError(
                "an assigned node profile is invalid",
                code="PREPARATION_PROFILE_INVALID",
                details={"node_id": node.id},
            ) from exc
        if not revalidate_inventory:
            expected = stored_inventory.get(node.id)
            expected_profile = (
                expected.get("profile") if isinstance(expected, dict) else None
            )
            if (
                not isinstance(expected_profile, dict)
                or _stable_profile_identity(profile_record.profile)
                != _stable_profile_identity(expected_profile)
            ):
                raise ArtifactPreparationError(
                    "an assigned node hardware identity has changed",
                    code="PREPARATION_INVENTORY_STALE",
                    details={"node_id": node.id},
                )
        if _workload_is_active(profile):
            raise ArtifactPreparationError(
                "an assigned node has an active workload",
                code="PREPARATION_WORKLOAD_ACTIVE",
                details={"node_id": node.id},
            )
        if not profile.runtime.engine_ready:
            raise ArtifactPreparationError(
                "an assigned node container runtime is unavailable",
                code="PREPARATION_RUNTIME_UNAVAILABLE",
                details={"node_id": node.id},
            )

    active_statement = select(Task.id, Task.node_id).where(
        Task.node_id.in_(node_ids),
        Task.status.in_({TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}),
    )
    if preparation_id is not None:
        active_statement = active_statement.where(Task.bulk_id != preparation_id)
    active_task = session.execute(
        active_statement.order_by(Task.created_at, Task.id).limit(1)
    ).one_or_none()
    if active_task is not None:
        raise ArtifactPreparationError(
            "an assigned node already has an active task",
            code="PREPARATION_NODE_BUSY",
            details={"task_id": active_task.id, "node_id": active_task.node_id},
        )

    requested_nodes = set(node_ids)
    for operation in session.scalars(
        select(DeploymentOperation).where(
            DeploymentOperation.active_lineage_id.is_not(None)
        )
    ):
        overlap = sorted(requested_nodes.intersection(operation.node_ids))
        if overlap:
            raise ArtifactPreparationError(
                "assigned nodes belong to an active deployment operation",
                code="PREPARATION_NODE_BUSY",
                details={
                    "operation_id": operation.id,
                    "node_ids": overlap,
                },
            )

    if revalidate_inventory:
        try:
            current, current_inventory = evaluate_deployment_recommendation(
                session,
                node_ids=(
                    list(recommendation.requested_node_ids)
                    if recommendation.selection_mode == "explicit_nodes"
                    else []
                ),
                all_online=recommendation.selection_mode == "all_online",
                objective=recommendation.objective,
                now=now,
            )
        except (RecommendationError, ValueError) as exc:
            raise ArtifactPreparationError(
                "accepted recommendation inventory cannot be revalidated",
                code="PREPARATION_INVENTORY_STALE",
                details={"deployment_id": deployment.id},
            ) from exc
        if (
            current.get("recommendation")
            != recommendation.recommendation_snapshot
            or current_inventory != recommendation.inventory_snapshot
        ):
            raise ArtifactPreparationError(
                "accepted recommendation inventory has changed",
                code="PREPARATION_INVENTORY_STALE",
                details={
                    "deployment_id": deployment.id,
                    "expected_inventory_fingerprint": recommendation.inventory_fingerprint,
                    "current_inventory_fingerprint": current.get(
                        "recommendation", {}
                    ).get("inventory_fingerprint"),
                },
            )

    artifact_id = selected.get("artifact_id")
    artifact = session.get(ModelArtifact, artifact_id)
    if (
        artifact is None
        or artifact.manifest_digest != selected.get("artifact_manifest_digest")
        or artifact.repository != selected.get("artifact_repository")
        or artifact.revision != selected.get("artifact_revision")
        or artifact.quantization != selected.get("quantization")
        or artifact.model_id != selected.get("model_id")
    ):
        raise ArtifactPreparationError(
            "selected model artifact identity has changed",
            code="PREPARATION_ARTIFACT_STALE",
            details={"deployment_id": deployment.id},
        )
    manifest = session.get(ArtifactManifest, artifact.manifest_digest)
    if (
        manifest is None
        or manifest.model_artifact_id != artifact.id
        or manifest.total_size_bytes <= 0
        or manifest.file_count <= 0
    ):
        raise ArtifactPreparationError(
            "selected model artifact has no verified manifest",
            code="PREPARATION_MANIFEST_REQUIRED",
            details={"artifact_id": artifact.id},
        )
    # Reconstructing the registered manifest verifies every normalized row and
    # its canonical digest before a task can refer to it.
    try:
        from .service import artifact_manifest_dict

        artifact_manifest_dict(session, manifest)
    except ValueError as exc:
        raise ArtifactPreparationError(
            "selected model artifact manifest is inconsistent",
            code="PREPARATION_MANIFEST_INVALID",
            details={"artifact_id": artifact.id},
        ) from exc

    runtime_image = selected.get("runtime_image")
    runtime = session.get(RuntimeRelease, selected.get("runtime_release_id"))
    try:
        validated_runtime_image, _ = validate_digest_pinned_runtime_image(
            runtime_image
        )
    except ValueError:
        validated_runtime_image = None
    if (
        validated_runtime_image is None
        or runtime is None
        or runtime.image != validated_runtime_image
    ):
        raise ArtifactPreparationError(
            "selected runtime image is not an immutable registered digest",
            code="PREPARATION_IMAGE_INVALID",
            details={"deployment_id": deployment.id},
        )
    stage_projection = _stage_preparation_projection(
        session,
        deployment,
        selected,
        artifact,
        artifact_set_digest=stage_artifact_set_digest,
    )
    if stage_projection is not None:
        unsupported = [
            node.id
            for node in nodes
            if not _agent_version_supported(
                node.agent_version,
                minimum=MINIMUM_STAGE_PREPARATION_AGENT,
            )
        ]
        if unsupported:
            raise ArtifactPreparationError(
                "an assigned node Agent cannot prepare stage artifacts",
                code="PREPARATION_AGENT_UNSUPPORTED",
                details={
                    "node_ids": unsupported,
                    "minimum_version": "0.3.19",
                },
            )
    # The controller cannot know which CAS chunks already exist or whether the
    # chunk store and assembled model share a filesystem. Reserve the safe
    # same-filesystem worst case: all chunks + one assembled snapshot + PR3's
    # fixed disk reserve. The Agent performs the authoritative per-filesystem
    # check again immediately before writing.
    stage_by_node = (
        {
            item["node_id"]: item
            for item in stage_projection["node_bindings"]
        }
        if stage_projection is not None
        else {}
    )
    checked_disk_nodes = set(node_ids) if disk_node_ids is None else disk_node_ids
    for node in nodes:
        if node.id not in checked_disk_nodes:
            continue
        selected_size = (
            stage_by_node[node.id]["total_size_bytes"]
            if stage_projection is not None
            else manifest.total_size_bytes
        )
        required_bytes = selected_size * 2 + 64 * 1024 * 1024
        required_mib = math.ceil(required_bytes / (1024 * 1024))
        profile = NodeProfile.from_dict(profile_records[node.id].profile)
        if profile.disk_free_mib < required_mib:
            raise ArtifactPreparationError(
                "an assigned node has insufficient free disk",
                code="PREPARATION_DISK_INSUFFICIENT",
                details={
                    "node_id": node.id,
                    "required_mib": required_mib,
                    "available_mib": profile.disk_free_mib,
                },
            )
    return recommendation, selected, artifact, manifest, nodes, stage_projection


def _request_identity(
    deployment: Deployment,
    *,
    stage_artifact_set_digest: str | None,
) -> dict[str, Any]:
    plan_cache_kind = deployment.plan.get("model_cache_kind")
    plan_stage = deployment.plan.get("stage_artifact")
    selected_stage_digest = (
        plan_stage.get("artifact_set_digest")
        if plan_cache_kind == MODEL_CACHE_KIND_STAGE and type(plan_stage) is dict
        else None
    )
    if stage_artifact_set_digest is not None and (
        selected_stage_digest is None
        or stage_artifact_set_digest != selected_stage_digest
    ):
        raise ArtifactPreparationError(
            "the requested stage artifact differs from the accepted generation",
            code="PREPARATION_STAGE_VARIANT_MISMATCH",
            details={"deployment_id": deployment.id},
        )
    value = {
        "schema_version": 1,
        "deployment_id": deployment.id,
        "generation": deployment.generation,
        "source_recommendation_id": deployment.source_recommendation_id,
        "plan": deployment.plan,
        "cache_kind": (
            MODEL_CACHE_KIND_STAGE
            if selected_stage_digest is not None
            else MODEL_CACHE_KIND_FULL_SNAPSHOT
        ),
    }
    if selected_stage_digest is not None:
        value["stage_artifact_set_digest"] = selected_stage_digest
    return value


def _plan_snapshot(
    deployment: Deployment,
    recommendation: DeploymentRecommendationRecord,
    selected: dict[str, Any],
    artifact: ModelArtifact,
    manifest: ArtifactManifest,
    stage_projection: dict[str, Any] | None,
) -> dict[str, Any]:
    effective_plan = copy.deepcopy(deployment.plan)
    if stage_projection is None:
        effective_plan["model_path"] = (
            "/var/lib/dure/models/sha256-"
            + manifest.digest.removeprefix("sha256:")
        )
        cache_kind = MODEL_CACHE_KIND_FULL_SNAPSHOT
    else:
        effective_plan["model_path"] = "/var/lib/dure/models/stages"
        effective_plan["model_cache_kind"] = MODEL_CACHE_KIND_STAGE
        effective_plan["stage_artifact"] = {
            key: stage_projection[key]
            for key in (
                "artifact_set_digest",
                "contract_identity_digest",
                "source_manifest_digest",
                "runtime_image",
                "vllm_version",
                "exporter_build_digest",
                "architecture",
                "quantization",
                "tensor_parallel_size",
                "pipeline_parallel_size",
                "loader_format",
            )
        }
        binding_by_node = {
            item["node_id"]: item
            for item in stage_projection["node_bindings"]
        }
        for assignment in effective_plan.get("assignments", []):
            binding = binding_by_node.get(assignment.get("node_id"))
            if binding is None:
                raise ArtifactPreparationError(
                    "stage plan is missing a node rank binding",
                    code="PREPARATION_STAGE_VARIANT_MISMATCH",
                )
            assignment["stage_manifest_digest"] = binding["manifest_digest"]
            assignment["stage_tensor_keys_digest"] = binding[
                "tensor_keys_digest"
            ]
        cache_kind = MODEL_CACHE_KIND_STAGE
    value = {
        "schema_version": 1,
        "deployment_id": deployment.id,
        "generation": deployment.generation,
        "source_recommendation_id": deployment.source_recommendation_id,
        "model_release_id": selected["model_release_id"],
        "inventory_fingerprint": recommendation.inventory_fingerprint,
        "artifact": {
            "id": artifact.id,
            "model_id": artifact.model_id,
            "repository": artifact.repository,
            "revision": artifact.revision,
            "manifest_digest": manifest.digest,
            "quantization": artifact.quantization,
            "total_size_bytes": manifest.total_size_bytes,
            "file_count": manifest.file_count,
            "cache_kind": cache_kind,
        },
        "runtime_image": selected["runtime_image"],
        "node_ids": list(selected["node_ids"]),
        "effective_plan": effective_plan,
    }
    if stage_projection is not None:
        value["stage_artifact"] = copy.deepcopy(stage_projection)
    return value


def _artifact_cache_identity(
    snapshot: dict[str, Any], node_id: str
) -> ArtifactCacheIdentity:
    """Derive the exact central cache identity from an immutable preparation."""

    artifact = snapshot.get("artifact")
    stage_artifact = snapshot.get("stage_artifact")
    if type(artifact) is not dict:
        raise ArtifactPreparationError(
            "preparation artifact identity is invalid",
            code="DEPLOYMENT_PREPARATION_INVALID",
        )
    if stage_artifact is None:
        manifest_digest = artifact.get("manifest_digest")
        return ArtifactCacheIdentity(
            cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
            cache_identity_digest=manifest_digest,
            manifest_digest=manifest_digest,
            source_manifest_digest=manifest_digest,
            verification_version=MODEL_CACHE_VERIFICATION_VERSION,
        )
    if type(stage_artifact) is not dict:
        raise ArtifactPreparationError(
            "preparation STAGE identity is invalid",
            code="DEPLOYMENT_PREPARATION_INVALID",
        )
    bindings = stage_artifact.get("node_bindings")
    binding = next(
        (
            item
            for item in bindings
            if type(item) is dict and item.get("node_id") == node_id
        ),
        None,
    ) if type(bindings) is list else None
    if binding is None:
        raise ArtifactPreparationError(
            "preparation STAGE rank binding is missing",
            code="DEPLOYMENT_PREPARATION_INVALID",
            details={"node_id": node_id},
        )
    try:
        physical_identity = StageCacheIdentity(
            repository=artifact["repository"],
            revision=artifact["revision"],
            manifest_digest=binding["manifest_digest"],
            quantization=artifact["quantization"],
            artifact_set_digest=stage_artifact["artifact_set_digest"],
            contract_identity_digest=stage_artifact[
                "contract_identity_digest"
            ],
            source_manifest_digest=stage_artifact["source_manifest_digest"],
            runtime_image=stage_artifact["runtime_image"],
            vllm_version=stage_artifact["vllm_version"],
            exporter_build_digest=stage_artifact["exporter_build_digest"],
            architecture=stage_artifact["architecture"],
            loader_format=stage_artifact["loader_format"],
            tensor_parallel_size=stage_artifact["tensor_parallel_size"],
            pipeline_parallel_size=stage_artifact["pipeline_parallel_size"],
            pipeline_rank=binding["pipeline_rank"],
            tensor_rank=binding["tensor_rank"],
            tensor_keys_digest=binding["tensor_keys_digest"],
        )
    except (KeyError, TypeError, ValueError, StageCacheError) as exc:
        raise ArtifactPreparationError(
            "preparation STAGE cache identity is invalid",
            code="DEPLOYMENT_PREPARATION_INVALID",
            details={"node_id": node_id},
        ) from exc
    return ArtifactCacheIdentity(
        cache_kind=MODEL_CACHE_KIND_STAGE,
        cache_identity_digest=physical_identity.cache_identity_digest,
        manifest_digest=physical_identity.manifest_digest,
        source_manifest_digest=physical_identity.source_manifest_digest,
        verification_version=STAGE_CACHE_VERIFICATION_VERSION,
        artifact_set_digest=physical_identity.artifact_set_digest,
        pipeline_rank=physical_identity.pipeline_rank,
        tensor_rank=physical_identity.tensor_rank,
        tensor_parallel_size=physical_identity.tensor_parallel_size,
        pipeline_parallel_size=physical_identity.pipeline_parallel_size,
        tensor_keys_digest=physical_identity.tensor_keys_digest,
    )


def record_deployment_cache_verification_failure(
    session: Session,
    deployment: Deployment,
    *,
    node_id: str,
    task_id: str,
) -> bool:
    """Fail closed for a node-local deployment verification failure.

    Legacy deployments have no controller-owned preparation/cache contract and
    are intentionally left unchanged. A missing cache record must not hide the
    original rollout failure, so callers receive ``False`` instead.
    """

    if deployment.source_recommendation_id is None:
        return False
    preparation = session.scalar(
        select(ArtifactPreparation).where(
            ArtifactPreparation.deployment_id == deployment.id
        )
    )
    if preparation is None or type(preparation.plan_snapshot) is not dict:
        return False
    try:
        identity = _artifact_cache_identity(preparation.plan_snapshot, node_id)
        record_verification_failure(
            session,
            node_id=node_id,
            identity=identity,
            source_id=task_id,
            source_task_id=task_id,
        )
    except (ArtifactCacheLifecycleError, ArtifactPreparationError):
        return False
    return True


def _active_attempts(session: Session, preparation_id: str) -> bool:
    return (
        session.scalar(
            select(ArtifactPreparationAttempt.id)
            .join(
                ArtifactPreparationNode,
                ArtifactPreparationNode.id
                == ArtifactPreparationAttempt.preparation_node_id,
            )
            .where(
                ArtifactPreparationNode.preparation_id == preparation_id,
                ArtifactPreparationAttempt.status.in_({"QUEUED", "RUNNING"}),
            )
            .limit(1)
        )
        is not None
    )


def _reconcile_expired_attempts(
    session: Session,
    preparation: ArtifactPreparation,
    *,
    now: datetime,
) -> int:
    expired_tasks = list(
        session.scalars(
            select(Task)
            .join(
                ArtifactPreparationAttempt,
                ArtifactPreparationAttempt.task_id == Task.id,
            )
            .join(
                ArtifactPreparationNode,
                ArtifactPreparationNode.id
                == ArtifactPreparationAttempt.preparation_node_id,
            )
            .where(
                ArtifactPreparationNode.preparation_id == preparation.id,
                Task.status == TaskStatus.RUNNING.value,
                or_(Task.lease_until.is_(None), Task.lease_until < now),
            )
            .order_by(Task.node_id, Task.id)
            .with_for_update()
        )
    )
    for task in expired_tasks:
        if not expire_preparation_task(session, task, task.node_id):
            raise ArtifactPreparationError(
                "expired preparation attempt could not be fenced",
                code="PREPARATION_ATTEMPT_CONFLICT",
                details={"task_id": task.id},
            )
    return len(expired_tasks)


def _task_payload(
    preparation: ArtifactPreparation,
    record: ArtifactPreparationNode,
    attempt: ArtifactPreparationAttempt,
    *,
    stage: str,
) -> dict[str, Any]:
    snapshot = preparation.plan_snapshot
    artifact = snapshot["artifact"]
    payload: dict[str, Any] = {
        "preparation_id": preparation.id,
        "preparation_node_id": record.id,
        "attempt_id": attempt.id,
        "attempt_no": attempt.attempt_no,
        "deployment_id": preparation.deployment_id,
        "generation": snapshot["generation"],
        "node_id": record.node_id,
        "apply": True,
    }
    if stage == "MODEL":
        stage_artifact = snapshot.get("stage_artifact")
        if stage_artifact is None:
            payload.update(
                model_id=artifact["model_id"],
                repository=artifact["repository"],
                revision=artifact["revision"],
                manifest_digest=artifact["manifest_digest"],
                quantization=artifact["quantization"],
                cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
            )
        else:
            bindings = stage_artifact.get("node_bindings")
            binding = next(
                (
                    item
                    for item in bindings
                    if type(item) is dict
                    and item.get("node_id") == record.node_id
                ),
                None,
            ) if type(bindings) is list else None
            if binding is None:
                raise ArtifactPreparationError(
                    "stage preparation has no exact node rank binding",
                    code="PREPARATION_PLAN_CONFLICT",
                    details={"node_id": record.node_id},
                )
            payload.update(
                model_id=artifact["model_id"],
                repository=artifact["repository"],
                revision=artifact["revision"],
                manifest_digest=binding["manifest_digest"],
                quantization=artifact["quantization"],
                cache_kind=MODEL_CACHE_KIND_STAGE,
                artifact_set_digest=stage_artifact["artifact_set_digest"],
                contract_identity_digest=stage_artifact[
                    "contract_identity_digest"
                ],
                source_manifest_digest=stage_artifact[
                    "source_manifest_digest"
                ],
                runtime_image=stage_artifact["runtime_image"],
                vllm_version=stage_artifact["vllm_version"],
                exporter_build_digest=stage_artifact[
                    "exporter_build_digest"
                ],
                architecture=stage_artifact["architecture"],
                tensor_parallel_size=stage_artifact[
                    "tensor_parallel_size"
                ],
                pipeline_parallel_size=stage_artifact[
                    "pipeline_parallel_size"
                ],
                pipeline_rank=binding["pipeline_rank"],
                tensor_rank=binding["tensor_rank"],
                loader_format=stage_artifact["loader_format"],
                tensor_keys_digest=binding["tensor_keys_digest"],
            )
    else:
        payload["runtime_image"] = snapshot["runtime_image"]
    return payload


def _queue_attempt(
    session: Session,
    preparation: ArtifactPreparation,
    record: ArtifactPreparationNode,
    *,
    stage: str,
    download_expected_bytes: int | None = None,
) -> Task:
    if stage == "MODEL":
        record.model_current_attempt += 1
        attempt_no = record.model_current_attempt
        record.model_status = "QUEUED"
        record.model_failure_code = None
        task_type = TaskType.PREPARE_MODEL
    else:
        record.image_current_attempt += 1
        attempt_no = record.image_current_attempt
        record.image_status = "QUEUED"
        record.image_failure_code = None
        task_type = TaskType.PREPARE_IMAGE
    task = Task(
        id=str(uuid.uuid4()),
        bulk_id=preparation.id,
        node_id=record.node_id,
        type=task_type.value,
        deployment_id=preparation.deployment_id,
        payload={},
    )
    attempt = ArtifactPreparationAttempt(
        id=str(uuid.uuid4()),
        preparation_node_id=record.id,
        stage=stage,
        attempt_no=attempt_no,
        task_id=task.id,
        status="QUEUED",
        download_progress=(
            {
                "downloaded_bytes": 0,
                "expected_bytes": download_expected_bytes,
            }
            if stage == "MODEL"
            and type(download_expected_bytes) is int
            and download_expected_bytes > 0
            else None
        ),
    )
    if stage == "MODEL" and attempt.download_progress is None:
        raise ArtifactPreparationError(
            "preparation manifest has no downloadable chunks",
            code="PREPARATION_MANIFEST_UNAVAILABLE",
            details={"manifest_digest": record.model_manifest_digest},
        )
    task.payload = _task_payload(
        preparation, record, attempt, stage=stage
    )
    session.add(task)
    # No ORM relationship joins these immutable records, so make the FK
    # ordering explicit on SQLite as well as PostgreSQL.
    session.flush()
    session.add(attempt)
    node = session.get(Node, record.node_id)
    if node is not None:
        node.desired_state = task.type
    record.updated_at = utcnow()
    return task


def _preparation_nodes(
    session: Session,
    preparation_id: str,
    *,
    lock: bool = False,
) -> list[ArtifactPreparationNode]:
    statement = (
        select(ArtifactPreparationNode)
        .where(ArtifactPreparationNode.preparation_id == preparation_id)
        .order_by(ArtifactPreparationNode.node_id)
    )
    if lock:
        statement = statement.with_for_update()
    return list(session.scalars(statement))


def _recompute_status(
    session: Session,
    preparation: ArtifactPreparation,
) -> None:
    records = _preparation_nodes(session, preparation.id)
    statuses = [
        status
        for record in records
        for status in (record.model_status, record.image_status)
    ]
    now = utcnow()
    if "RUNNING" in statuses:
        preparation.status = "RUNNING"
        preparation.completed_at = None
    elif "QUEUED" in statuses:
        preparation.status = "QUEUED"
        preparation.completed_at = None
    elif records and all(
        record.model_status == "SUCCEEDED"
        and record.image_status == "SUCCEEDED"
        for record in records
    ):
        preparation.status = "SUCCEEDED"
        preparation.completed_at = now
    elif statuses and all(status == "PREPARED" for status in statuses):
        preparation.status = "PREPARED"
        preparation.completed_at = None
    else:
        any_node_complete = any(
            record.model_status == "SUCCEEDED"
            and record.image_status == "SUCCEEDED"
            for record in records
        )
        preparation.status = (
            "PARTIAL_FAILED" if any_node_complete else "FAILED"
        )
        preparation.completed_at = now
    preparation.updated_at = now


def _queue_for_apply(
    session: Session,
    preparation: ArtifactPreparation,
) -> list[Task]:
    records = _preparation_nodes(session, preparation.id, lock=True)
    if any(
        status in {"QUEUED", "RUNNING"}
        for record in records
        for status in (record.model_status, record.image_status)
    ):
        return []
    tasks: list[Task] = []
    model_records = [
        record
        for record in records
        if record.model_status in {"PREPARED", "FAILED"}
    ]
    if model_records:
        expected_by_manifest: dict[str, int] = {}
        for record in model_records:
            if record.model_manifest_digest not in expected_by_manifest:
                expected = _manifest_download_bytes(
                    session, record.model_manifest_digest
                )
                if expected is None:
                    raise ArtifactPreparationError(
                        "preparation manifest has no downloadable chunks",
                        code="PREPARATION_MANIFEST_UNAVAILABLE",
                        details={
                            "manifest_digest": record.model_manifest_digest
                        },
                    )
                expected_by_manifest[record.model_manifest_digest] = expected
        tasks.extend(
            _queue_attempt(
                session,
                preparation,
                record,
                stage="MODEL",
                download_expected_bytes=expected_by_manifest[
                    record.model_manifest_digest
                ],
            )
            for record in model_records
        )
    else:
        image_records = [
            record for record in records if record.image_status == "FAILED"
        ]
        tasks.extend(
            _queue_attempt(session, preparation, record, stage="IMAGE")
            for record in image_records
        )
    _recompute_status(session, preparation)
    return tasks


def prepare_deployment_artifacts(
    session: Session,
    deployment_id: str,
    *,
    request_id: str,
    artifact_set_digest: str | None = None,
    apply: bool = False,
    now: datetime | None = None,
) -> tuple[ArtifactPreparation, list[Task], bool]:
    _canonical_uuid(request_id, "request_id")
    candidate = session.get(Deployment, deployment_id)
    if candidate is None:
        raise ArtifactPreparationNotFoundError(
            "deployment generation not found",
            code="DEPLOYMENT_NOT_FOUND",
            details={"deployment_id": deployment_id},
        )
    candidate_plan = copy.deepcopy(candidate.plan)
    candidate_node_ids = sorted(
        {
            item.get("node_id")
            for item in candidate_plan.get("assignments", [])
            if isinstance(item, dict) and isinstance(item.get("node_id"), str)
        }
    )
    if not candidate_node_ids:
        raise ArtifactPreparationError(
            "accepted node assignment is invalid",
            code="PREPARATION_ASSIGNMENT_INVALID",
            details={"deployment_id": deployment_id},
        )
    # Match every other central mutation producer: lock the complete node set
    # in UUID order before locking deployment/lineage state.
    list(
        session.scalars(
            select(Node)
            .where(Node.id.in_(candidate_node_ids))
            .order_by(Node.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    deployment = session.scalar(
        select(Deployment)
        .where(Deployment.id == deployment_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if deployment is None or deployment.plan != candidate_plan:
        raise ArtifactPreparationError(
            "deployment generation changed while preparation was locked",
            code="PREPARATION_PLAN_CONFLICT",
            details={"deployment_id": deployment_id},
        )
    request_digest = _digest(
        _request_identity(
            deployment,
            stage_artifact_set_digest=artifact_set_digest,
        )
    )
    request_binding = session.scalar(
        select(ArtifactPreparation)
        .where(ArtifactPreparation.request_id == request_id)
        .with_for_update()
    )
    if (
        request_binding is not None
        and request_binding.deployment_id != deployment.id
    ):
        raise ArtifactPreparationError(
            "request_id is already bound to another deployment generation",
            code="PREPARATION_REQUEST_CONFLICT",
            details={
                "preparation_id": request_binding.id,
                "deployment_id": request_binding.deployment_id,
            },
        )
    existing = session.scalar(
        select(ArtifactPreparation)
        .where(ArtifactPreparation.deployment_id == deployment.id)
        .with_for_update()
    )
    evaluated_at = now if now is not None else utcnow()
    if existing is not None:
        if (
            existing.request_id != request_id
            or existing.request_digest != request_digest
        ):
            raise ArtifactPreparationError(
                "deployment generation already has a different preparation request",
                code="PREPARATION_REQUEST_CONFLICT",
                details={
                    "deployment_id": deployment.id,
                    "preparation_id": existing.id,
                },
            )
        if apply:
            expired_count = _reconcile_expired_attempts(
                session, existing, now=evaluated_at
            )
            if expired_count:
                # Lease expiry is an independent fencing decision and must not
                # be rolled back merely because the subsequent retry safety
                # gates (for example approval) reject a new attempt.
                session.commit()
                list(
                    session.scalars(
                        select(Node)
                        .where(Node.id.in_(candidate_node_ids))
                        .order_by(Node.id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                )
                deployment = session.scalar(
                    select(Deployment)
                    .where(Deployment.id == deployment_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                existing = session.scalar(
                    select(ArtifactPreparation)
                    .where(ArtifactPreparation.id == existing.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                if deployment is None or existing is None:
                    raise ArtifactPreparationError(
                        "preparation disappeared after lease fencing",
                        code="PREPARATION_ATTEMPT_CONFLICT",
                    )
                if now is None:
                    evaluated_at = utcnow()
        if not apply or existing.status == "SUCCEEDED" or _active_attempts(
            session, existing.id
        ):
            return existing, [], False
        initial_apply = existing.status == "PREPARED"
        (
            recommendation,
            selected,
            artifact,
            manifest,
            _nodes,
            stage_projection,
        ) = _selected_context(
            session,
            deployment,
            now=evaluated_at,
            stage_artifact_set_digest=artifact_set_digest,
            preparation_id=existing.id,
            revalidate_inventory=initial_apply,
            disk_node_ids=(
                None if initial_apply else set()
            ),
        )
        expected_snapshot = _plan_snapshot(
            deployment,
            recommendation,
            selected,
            artifact,
            manifest,
            stage_projection,
        )
        if existing.plan_snapshot != expected_snapshot:
            raise ArtifactPreparationError(
                "stored preparation plan no longer matches its immutable inputs",
                code="PREPARATION_PLAN_CONFLICT",
                details={"preparation_id": existing.id},
            )
        tasks = _queue_for_apply(session, existing)
        session.add(
            AuditEvent(
                actor="admin",
                action="deployment.prepare.apply",
                target=existing.id,
                outcome="success",
                detail={"retry": True, "task_count": len(tasks)},
            )
        )
        session.commit()
        return existing, tasks, bool(tasks)

    (
        recommendation,
        selected,
        artifact,
        manifest,
        nodes,
        stage_projection,
    ) = _selected_context(
        session,
        deployment,
        now=evaluated_at,
        stage_artifact_set_digest=artifact_set_digest,
    )
    preparation = ArtifactPreparation(
        id=str(uuid.uuid4()),
        request_id=request_id,
        request_digest=request_digest,
        deployment_id=deployment.id,
        status="PREPARED",
        plan_snapshot=_plan_snapshot(
            deployment,
            recommendation,
            selected,
            artifact,
            manifest,
            stage_projection,
        ),
    )
    try:
        session.add(preparation)
        session.flush()
        stage_by_node = (
            {
                item["node_id"]: item
                for item in stage_projection["node_bindings"]
            }
            if stage_projection is not None
            else {}
        )
        for node in nodes:
            session.add(
                ArtifactPreparationNode(
                    id=str(uuid.uuid4()),
                    preparation_id=preparation.id,
                    node_id=node.id,
                    model_manifest_digest=(
                        stage_by_node[node.id]["manifest_digest"]
                        if stage_projection is not None
                        else manifest.digest
                    ),
                    runtime_image=selected["runtime_image"],
                )
            )
        session.flush()
        tasks = _queue_for_apply(session, preparation) if apply else []
        session.add(
            AuditEvent(
                actor="admin",
                action=(
                    "deployment.prepare.apply"
                    if apply
                    else "deployment.prepare.preview"
                ),
                target=preparation.id,
                outcome="success",
                detail={
                    "deployment_id": deployment.id,
                    "task_count": len(tasks),
                },
            )
        )
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        request_binding = session.scalar(
            select(ArtifactPreparation).where(
                ArtifactPreparation.request_id == request_id
            )
        )
        deployment_binding = session.scalar(
            select(ArtifactPreparation).where(
                ArtifactPreparation.deployment_id == deployment_id
            )
        )
        if (
            request_binding is not None
            and deployment_binding is not None
            and request_binding.id == deployment_binding.id
            and request_binding.request_digest == request_digest
        ):
            return prepare_deployment_artifacts(
                session,
                deployment_id,
                request_id=request_id,
                artifact_set_digest=artifact_set_digest,
                apply=apply,
                now=now,
            )
        raise ArtifactPreparationError(
            "preparation request conflicts with an existing immutable binding",
            code="PREPARATION_REQUEST_CONFLICT",
            details={"deployment_id": deployment_id},
        ) from exc
    return preparation, tasks, True


def get_artifact_preparation(
    session: Session, preparation_id: str
) -> ArtifactPreparation:
    preparation = session.get(ArtifactPreparation, preparation_id)
    if preparation is None:
        raise ArtifactPreparationNotFoundError(
            details={"preparation_id": preparation_id}
        )
    return preparation


def _preparation_node_stage(record: ArtifactPreparationNode) -> str:
    """Project one node onto the closed operator-facing preparation stages."""

    if "FAILED" in {record.model_status, record.image_status}:
        return "FAILED"
    if record.model_status != "SUCCEEDED":
        return "MODEL"
    if record.image_status != "SUCCEEDED":
        return "IMAGE"
    return "COMPLETE"


def _preparation_stage(
    preparation: ArtifactPreparation,
    node_progress: list[dict[str, Any]],
) -> str:
    """Return the earliest unfinished gate, or a terminal closed stage."""

    if preparation.status in {"FAILED", "PARTIAL_FAILED"}:
        return "FAILED"
    stages = [item["stage"] for item in node_progress]
    if "FAILED" in stages:
        return "FAILED"
    if stages and all(stage == "COMPLETE" for stage in stages):
        return "COMPLETE"
    # MODEL takes precedence while nodes are progressing concurrently because
    # every deployment still depends on the earliest unfinished gate.
    if "MODEL" in stages:
        return "MODEL"
    if "IMAGE" in stages:
        return "IMAGE"
    return "FAILED"


def _attempt_progress(
    *,
    status: str,
    current_attempt: int,
    failure_code: str | None,
) -> dict[str, Any]:
    return {
        "status": status,
        "current_attempt": current_attempt,
        "retry_count": max(current_attempt - 1, 0),
        "failure_code": failure_code,
    }


def _manifest_download_bytes(
    session: Session, manifest_digest: str
) -> int | None:
    """Return distinct immutable chunk bytes, excluding duplicate references."""

    distinct_chunks = (
        select(
            ArtifactFileChunk.chunk_digest,
            ArtifactFileChunk.length_bytes,
        )
        .join(
            ArtifactManifestFile,
            ArtifactManifestFile.id == ArtifactFileChunk.file_id,
        )
        .where(ArtifactManifestFile.manifest_digest == manifest_digest)
        .distinct()
        .subquery()
    )
    expected = session.scalar(
        select(func.sum(distinct_chunks.c.length_bytes))
    )
    expected = int(expected) if expected is not None else 0
    return expected if expected > 0 else None


def artifact_preparation_detail(
    session: Session, preparation: ArtifactPreparation
) -> dict[str, Any]:
    records = _preparation_nodes(session, preparation.id)
    attempts = list(
        session.scalars(
            select(ArtifactPreparationAttempt)
            .join(
                ArtifactPreparationNode,
                ArtifactPreparationNode.id
                == ArtifactPreparationAttempt.preparation_node_id,
            )
            .where(ArtifactPreparationNode.preparation_id == preparation.id)
            .order_by(
                ArtifactPreparationNode.node_id,
                ArtifactPreparationAttempt.stage,
                ArtifactPreparationAttempt.attempt_no,
            )
        )
    )
    attempts_by_node: dict[str, list[dict[str, Any]]] = {
        record.id: [] for record in records
    }
    attempts_by_identity: dict[
        tuple[str, str, int], ArtifactPreparationAttempt
    ] = {}
    for attempt in attempts:
        attempts_by_identity[
            (
                attempt.preparation_node_id,
                attempt.stage,
                attempt.attempt_no,
            )
        ] = attempt
        attempts_by_node[attempt.preparation_node_id].append(
            {
                "id": attempt.id,
                "stage": attempt.stage,
                "attempt_no": attempt.attempt_no,
                "task_id": attempt.task_id,
                "status": attempt.status,
                "failure_code": attempt.failure_code,
                "result": attempt.result,
                "download_progress": attempt.download_progress,
                "created_at": _iso(attempt.created_at),
                "updated_at": _iso(attempt.updated_at),
                "completed_at": _iso(attempt.completed_at),
            }
        )
    manifest_digests = {
        record.model_manifest_digest for record in records
    }
    manifests = {
        manifest.digest: manifest
        for manifest in session.scalars(
            select(ArtifactManifest).where(
                ArtifactManifest.digest.in_(manifest_digests)
            )
        )
    }
    download_bytes_by_manifest = {
        digest: _manifest_download_bytes(session, digest)
        for digest in manifest_digests
    }
    node_progress: list[dict[str, Any]] = []
    for record in records:
        manifest = manifests.get(record.model_manifest_digest)
        expected_bytes = (
            manifest.total_size_bytes if manifest is not None else None
        )
        download_expected_bytes = download_bytes_by_manifest[
            record.model_manifest_digest
        ]
        current_model_attempt = attempts_by_identity.get(
            (record.id, "MODEL", record.model_current_attempt)
        )
        model_result = (
            current_model_attempt.result
            if current_model_attempt is not None
            and current_model_attempt.status == "SUCCEEDED"
            and record.model_status == "SUCCEEDED"
            and type(current_model_attempt.result) is dict
            else None
        )
        result_bytes = (
            model_result.get("bytes_verified")
            if model_result is not None
            else None
        )
        verified_bytes = result_bytes if type(result_bytes) is int else 0
        raw_download_progress = (
            current_model_attempt.download_progress
            if current_model_attempt is not None
            else None
        )
        valid_download_progress = (
            raw_download_progress
            if type(raw_download_progress) is dict
            and set(raw_download_progress)
            == {"downloaded_bytes", "expected_bytes"}
            and type(raw_download_progress.get("downloaded_bytes")) is int
            and type(raw_download_progress.get("expected_bytes")) is int
            and raw_download_progress["expected_bytes"]
            == download_expected_bytes
            and 0
            <= raw_download_progress["downloaded_bytes"]
            <= raw_download_progress["expected_bytes"]
            else None
        )
        if current_model_attempt is None:
            downloaded_bytes = 0
            download_bytes_source = (
                PREPARATION_DOWNLOAD_BYTES_NOT_STARTED_SOURCE
            )
        elif valid_download_progress is not None:
            downloaded_bytes = valid_download_progress["downloaded_bytes"]
            download_bytes_source = PREPARATION_DOWNLOAD_BYTES_SOURCE
        elif model_result is not None and download_expected_bytes is not None:
            # Pre-0010 successful attempts have no progress JSON, but their
            # already validated immutable result proves all manifest chunks
            # were locally available at terminal verification.
            downloaded_bytes = download_expected_bytes
            download_bytes_source = (
                PREPARATION_DOWNLOAD_BYTES_DERIVED_SOURCE
            )
        else:
            downloaded_bytes = None
            download_bytes_source = (
                PREPARATION_DOWNLOAD_BYTES_UNAVAILABLE_SOURCE
            )
        model_progress = _attempt_progress(
            status=record.model_status,
            current_attempt=record.model_current_attempt,
            failure_code=record.model_failure_code,
        )
        image_progress = _attempt_progress(
            status=record.image_status,
            current_attempt=record.image_current_attempt,
            failure_code=record.image_failure_code,
        )
        stage = _preparation_node_stage(record)
        if stage not in PREPARATION_PROGRESS_STAGES:  # pragma: no cover
            raise AssertionError("invalid preparation progress stage")
        node_progress.append(
            {
                "node_id": record.node_id,
                "expected_bytes": expected_bytes,
                "verified_bytes": verified_bytes,
                "bytes_source": PREPARATION_PROGRESS_BYTES_SOURCE,
                "downloaded_bytes": downloaded_bytes,
                "download_expected_bytes": download_expected_bytes,
                "download_bytes_source": download_bytes_source,
                "stage": stage,
                "retrying": any(
                    item["retry_count"] > 0
                    and item["status"] in {"QUEUED", "RUNNING"}
                    for item in (model_progress, image_progress)
                ),
                "model": model_progress,
                "image": image_progress,
            }
        )
    expected_values = [item["expected_bytes"] for item in node_progress]
    expected_bytes = (
        sum(expected_values)
        if all(type(value) is int for value in expected_values)
        else None
    )
    overall_stage = _preparation_stage(preparation, node_progress)
    if overall_stage not in PREPARATION_PROGRESS_STAGES:  # pragma: no cover
        raise AssertionError("invalid preparation progress stage")
    progress = {
        "expected_bytes": expected_bytes,
        "verified_bytes": sum(
            item["verified_bytes"] for item in node_progress
        ),
        "bytes_source": PREPARATION_PROGRESS_BYTES_SOURCE,
        "downloaded_bytes": (
            sum(item["downloaded_bytes"] for item in node_progress)
            if all(
                type(item["downloaded_bytes"]) is int
                for item in node_progress
            )
            else None
        ),
        "download_expected_bytes": (
            sum(item["download_expected_bytes"] for item in node_progress)
            if all(
                type(item["download_expected_bytes"]) is int
                for item in node_progress
            )
            else None
        ),
        "download_bytes_source": (
            next(
                iter(
                    {
                        item["download_bytes_source"]
                        for item in node_progress
                    }
                )
            )
            if len(
                {
                    item["download_bytes_source"]
                    for item in node_progress
                }
            )
            == 1
            else PREPARATION_DOWNLOAD_BYTES_MIXED_SOURCE
        ),
        "stage": overall_stage,
        "retrying": any(item["retrying"] for item in node_progress),
        "model_retry_count": sum(
            item["model"]["retry_count"] for item in node_progress
        ),
        "image_retry_count": sum(
            item["image"]["retry_count"] for item in node_progress
        ),
    }
    progress_by_node = {
        item["node_id"]: item for item in node_progress
    }
    return {
        "id": preparation.id,
        "request_id": preparation.request_id,
        "request_digest": preparation.request_digest,
        "deployment_id": preparation.deployment_id,
        "status": preparation.status,
        "plan_snapshot": preparation.plan_snapshot,
        "created_at": _iso(preparation.created_at),
        "updated_at": _iso(preparation.updated_at),
        "completed_at": _iso(preparation.completed_at),
        "progress": progress,
        "nodes": [
            {
                "id": record.id,
                "node_id": record.node_id,
                "model_manifest_digest": record.model_manifest_digest,
                "runtime_image": record.runtime_image,
                "model_status": record.model_status,
                "image_status": record.image_status,
                "model_current_attempt": record.model_current_attempt,
                "image_current_attempt": record.image_current_attempt,
                "model_failure_code": record.model_failure_code,
                "image_failure_code": record.image_failure_code,
                "created_at": _iso(record.created_at),
                "updated_at": _iso(record.updated_at),
                "progress": progress_by_node[record.node_id],
                "attempts": attempts_by_node[record.id],
            }
            for record in records
        ],
    }


def _bound_attempt(
    session: Session,
    task: Task,
    *,
    lock: bool = False,
) -> tuple[
    ArtifactPreparationAttempt | None,
    ArtifactPreparationNode | None,
    ArtifactPreparation | None,
]:
    statement = select(ArtifactPreparationAttempt).where(
        ArtifactPreparationAttempt.task_id == task.id
    )
    if lock:
        statement = statement.with_for_update()
    attempt = session.scalar(statement)
    if attempt is None:
        return None, None, None
    record_statement = select(ArtifactPreparationNode).where(
        ArtifactPreparationNode.id == attempt.preparation_node_id
    )
    if lock:
        record_statement = record_statement.with_for_update()
    record = session.scalar(record_statement)
    if record is None:
        return attempt, None, None
    preparation_statement = select(ArtifactPreparation).where(
        ArtifactPreparation.id == record.preparation_id
    )
    if lock:
        preparation_statement = preparation_statement.with_for_update()
    preparation = session.scalar(preparation_statement)
    return attempt, record, preparation


def _attempt_is_current(
    task: Task,
    attempt: ArtifactPreparationAttempt,
    record: ArtifactPreparationNode,
    preparation: ArtifactPreparation,
) -> bool:
    stage = "MODEL" if task.type == TaskType.PREPARE_MODEL.value else "IMAGE"
    current = (
        record.model_current_attempt
        if stage == "MODEL"
        else record.image_current_attempt
    )
    expected_status = (
        record.model_status if stage == "MODEL" else record.image_status
    )
    return (
        task.type in PREPARATION_TASK_TYPES
        and attempt.stage == stage
        and attempt.attempt_no == current
        and task.bulk_id == preparation.id
        and task.node_id == record.node_id
        and task.deployment_id == preparation.deployment_id
        and expected_status == attempt.status
    )


def claim_preparation_task(
    session: Session, task: Task, node_id: str
) -> bool:
    if task.type not in PREPARATION_TASK_TYPES:
        return True
    attempt, record, preparation = _bound_attempt(session, task, lock=True)
    if (
        attempt is None
        or record is None
        or preparation is None
        or task.node_id != node_id
        or task.status != TaskStatus.RUNNING.value
        or attempt.status != "QUEUED"
        or not _attempt_is_current(task, attempt, record, preparation)
    ):
        return False
    attempt.status = "RUNNING"
    attempt.updated_at = utcnow()
    if attempt.stage == "MODEL":
        record.model_status = "RUNNING"
    else:
        record.image_status = "RUNNING"
    record.updated_at = utcnow()
    _recompute_status(session, preparation)
    return True


def extend_preparation_task(
    session: Session,
    task: Task,
    node_id: str,
    *,
    progress: dict[str, Any] | None = None,
) -> bool:
    if task.type not in PREPARATION_TASK_TYPES:
        return True
    attempt, record, preparation = _bound_attempt(session, task, lock=True)
    current = bool(
        attempt
        and record
        and preparation
        and task.node_id == node_id
        and task.status == TaskStatus.RUNNING.value
        and attempt.status == "RUNNING"
        and _attempt_is_current(task, attempt, record, preparation)
    )
    if not current:
        return False
    if progress is None:
        return True
    if (
        task.type != PREPARE_MODEL_TASK
        or attempt.stage != "MODEL"
        or type(progress) is not dict
        or set(progress) != {"downloaded_bytes"}
        or type(progress.get("downloaded_bytes")) is not int
        or progress["downloaded_bytes"] < 0
    ):
        return False
    previous = attempt.download_progress
    stored_expected = (
        previous.get("expected_bytes")
        if type(previous) is dict
        and set(previous) == {"downloaded_bytes", "expected_bytes"}
        and type(previous.get("downloaded_bytes")) is int
        and type(previous.get("expected_bytes")) is int
        and previous["expected_bytes"] > 0
        and 0 <= previous["downloaded_bytes"] <= previous["expected_bytes"]
        else None
    )
    expected_bytes = (
        stored_expected
        if stored_expected is not None
        else _manifest_download_bytes(session, record.model_manifest_digest)
    )
    if (
        expected_bytes is None
        or progress["downloaded_bytes"] > expected_bytes
    ):
        return False
    if previous is None:
        previous_bytes = 0
    elif (
        type(previous) is dict
        and set(previous) == {"downloaded_bytes", "expected_bytes"}
        and type(previous.get("downloaded_bytes")) is int
        and previous.get("expected_bytes") == expected_bytes
        and 0 <= previous["downloaded_bytes"] <= expected_bytes
    ):
        previous_bytes = previous["downloaded_bytes"]
    else:
        return False
    high_water = max(previous_bytes, progress["downloaded_bytes"])
    if previous is None or high_water != previous_bytes:
        now = utcnow()
        attempt.download_progress = {
            "downloaded_bytes": high_water,
            "expected_bytes": expected_bytes,
        }
        attempt.updated_at = now
        record.updated_at = now
        preparation.updated_at = now
    return True


def _task_wire(task: Task) -> dict[str, Any]:
    return {
        "id": task.id,
        "node_id": task.node_id,
        "type": task.type,
        "deployment_id": task.deployment_id,
        "payload": task.payload,
    }


def finish_preparation_task(
    session: Session,
    task: Task,
    node_id: str,
    *,
    result: dict[str, Any] | None,
    error: str | None,
) -> tuple[bool, ArtifactPreparation | None]:
    if task.type not in PREPARATION_TASK_TYPES:
        return False, None
    node = session.scalar(
        select(Node).where(Node.id == node_id).with_for_update()
    )
    if node is None:
        return False, None
    locked_task = session.scalar(
        select(Task)
        .where(Task.id == task.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if locked_task is None:
        return False, None
    attempt, record, preparation = _bound_attempt(
        session, locked_task, lock=True
    )
    if (
        attempt is not None
        and record is not None
        and preparation is not None
        and locked_task.node_id == node_id
        and _attempt_is_current(
            locked_task, attempt, record, preparation
        )
    ):
        if (
            locked_task.status == TaskStatus.SUCCEEDED.value
            and attempt.status == "SUCCEEDED"
            and error is None
        ):
            try:
                replayed = validate_preparation_result(
                    _task_wire(locked_task), result, node_id
                )
            except Exception:
                return False, preparation
            return replayed == locked_task.result == attempt.result, preparation
        if (
            locked_task.status == TaskStatus.FAILED.value
            and attempt.status == "FAILED"
            and result is None
            and error is not None
        ):
            replayed_code = (
                error
                if error in PREPARATION_TERMINAL_FAILURE_CODES
                else "PREPARATION_EXECUTION_FAILED"
            )
            return (
                replayed_code == locked_task.error == attempt.failure_code,
                preparation,
            )
    lease_until = _aware(locked_task.lease_until)
    if (
        attempt is None
        or record is None
        or preparation is None
        or not node.approved
        or locked_task.node_id != node_id
        or locked_task.status != TaskStatus.RUNNING.value
        or lease_until is None
        or lease_until < utcnow()
        or attempt.status != "RUNNING"
        or not _attempt_is_current(
            locked_task, attempt, record, preparation
        )
    ):
        return False, preparation

    now = utcnow()
    failure_code: str | None = None
    validated_result: dict[str, Any] | None = None
    result_rejection: ArtifactPreparationError | None = None
    if error is not None:
        failure_code = (
            error
            if error in PREPARATION_TERMINAL_FAILURE_CODES
            else "PREPARATION_EXECUTION_FAILED"
        )
    else:
        try:
            validated_result = validate_preparation_result(
                _task_wire(locked_task), result, node_id
            )
        except Exception:
            failure_code = "PREPARATION_RESULT_REJECTED"
            result_rejection = ArtifactPreparationError(
                "preparation result does not match the closed schema",
                code=failure_code,
                details={"task_id": locked_task.id},
            )
        manifest = session.get(ArtifactManifest, record.model_manifest_digest)
        if (
            failure_code is None
            and attempt.stage == "MODEL"
            and (
                manifest is None
                or validated_result.get("bytes_verified")
                != manifest.total_size_bytes
                or validated_result.get("file_count") != manifest.file_count
            )
        ):
            failure_code = "PREPARATION_RESULT_REJECTED"
            validated_result = None
            result_rejection = ArtifactPreparationError(
                "preparation result does not match the registered manifest",
                code=failure_code,
                details={"task_id": locked_task.id},
            )

    if failure_code is None and attempt.stage == "MODEL":
        expected_download_bytes = _manifest_download_bytes(
            session, record.model_manifest_digest
        )
        if expected_download_bytes is None:  # pragma: no cover - manifest gate
            failure_code = "PREPARATION_RESULT_REJECTED"
            validated_result = None
            result_rejection = ArtifactPreparationError(
                "preparation manifest has no downloadable chunks",
                code=failure_code,
                details={"task_id": locked_task.id},
            )
        else:
            attempt.download_progress = {
                "downloaded_bytes": expected_download_bytes,
                "expected_bytes": expected_download_bytes,
            }

    locked_task.status = (
        TaskStatus.FAILED.value
        if failure_code is not None
        else TaskStatus.SUCCEEDED.value
    )
    locked_task.result = validated_result
    locked_task.error = failure_code
    locked_task.lease_until = None
    attempt.status = "FAILED" if failure_code else "SUCCEEDED"
    attempt.failure_code = failure_code
    attempt.result = validated_result
    attempt.updated_at = now
    attempt.completed_at = now
    if attempt.stage == "MODEL":
        record.model_status = "FAILED" if failure_code else "SUCCEEDED"
        record.model_failure_code = failure_code
        if failure_code is None:
            try:
                # Persist the terminal MODEL projection before the lifecycle
                # helper re-reads and fences the exact current attempt.  Its
                # cache/event writes live in a savepoint so any registry race
                # can be converted into a committed closed failure without a
                # half-written READY record.
                session.flush()
                with session.begin_nested():
                    identity = _artifact_cache_identity(
                        preparation.plan_snapshot, record.node_id
                    )
                    record_preparation_success(
                        session,
                        attempt_id=attempt.id,
                        identity=identity,
                    )
            except (ArtifactCacheLifecycleError, ArtifactPreparationError) as exc:
                failure_code = "PREPARATION_RESULT_REJECTED"
                validated_result = None
                locked_task.status = TaskStatus.FAILED.value
                locked_task.result = None
                locked_task.error = failure_code
                attempt.status = "FAILED"
                attempt.failure_code = failure_code
                attempt.result = None
                record.model_status = "FAILED"
                record.model_failure_code = failure_code
                result_rejection = ArtifactPreparationError(
                    "preparation success could not be bound to an exact READY cache",
                    code=failure_code,
                    details={
                        "task_id": locked_task.id,
                        "cache_failure_code": getattr(exc, "code", None),
                    },
                )
            else:
                _queue_attempt(session, preparation, record, stage="IMAGE")
    else:
        record.image_status = "FAILED" if failure_code else "SUCCEEDED"
        record.image_failure_code = failure_code
    record.updated_at = now
    if failure_code is not None or attempt.stage == "IMAGE":
        node.desired_state = None
    _recompute_status(session, preparation)
    session.add(
        AuditEvent(
            actor="agent",
            action="deployment.prepare.task.finish",
            target=attempt.id,
            outcome="failure" if failure_code else "success",
            detail={
                "stage": attempt.stage,
                "node_id": node_id,
                "failure_code": failure_code,
            },
        )
    )
    if result_rejection is not None:
        raise result_rejection
    return True, preparation


def expire_preparation_task(
    session: Session, task: Task, node_id: str
) -> bool:
    if task.type not in PREPARATION_TASK_TYPES:
        return False
    locked_task = session.scalar(
        select(Task)
        .where(Task.id == task.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if locked_task is None:
        return False
    attempt, record, preparation = _bound_attempt(
        session, locked_task, lock=True
    )
    now = utcnow()
    lease_until = _aware(locked_task.lease_until)
    if (
        attempt is None
        or record is None
        or preparation is None
        or locked_task.node_id != node_id
        or locked_task.status != TaskStatus.RUNNING.value
        or (lease_until is not None and lease_until >= now)
        or attempt.status != "RUNNING"
        or not _attempt_is_current(
            locked_task, attempt, record, preparation
        )
    ):
        return False
    failure_code = "PREPARATION_LEASE_EXPIRED"
    locked_task.status = TaskStatus.FAILED.value
    locked_task.result = None
    locked_task.error = failure_code
    locked_task.lease_until = None
    attempt.status = "FAILED"
    attempt.failure_code = failure_code
    attempt.result = None
    attempt.updated_at = now
    attempt.completed_at = now
    if attempt.stage == "MODEL":
        record.model_status = "FAILED"
        record.model_failure_code = failure_code
    else:
        record.image_status = "FAILED"
        record.image_failure_code = failure_code
    record.updated_at = now
    node = session.get(Node, node_id)
    if node is not None:
        node.desired_state = None
    _recompute_status(session, preparation)
    session.add(
        AuditEvent(
            actor="controller",
            action="deployment.prepare.task.expire",
            target=attempt.id,
            outcome="failure",
            detail={
                "stage": attempt.stage,
                "node_id": node_id,
                "failure_code": failure_code,
            },
        )
    )
    return True


def cancel_preparation_task(session: Session, task: Task) -> bool:
    if task.type not in PREPARATION_TASK_TYPES:
        return False
    locked_task = session.scalar(
        select(Task)
        .where(Task.id == task.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if locked_task is None:
        return False
    attempt, record, preparation = _bound_attempt(
        session, locked_task, lock=True
    )
    if (
        attempt is None
        or record is None
        or preparation is None
        or not _attempt_is_current(
            locked_task, attempt, record, preparation
        )
    ):
        return False
    now = utcnow()
    expired = (
        locked_task.status == TaskStatus.RUNNING.value
        and (_aware(locked_task.lease_until) or now) < now
    )
    if locked_task.status != TaskStatus.QUEUED.value and not expired:
        return False
    failure_code = (
        "PREPARATION_LEASE_EXPIRED"
        if expired
        else "PREPARATION_TASK_CANCELED"
    )
    locked_task.status = (
        TaskStatus.FAILED.value if expired else TaskStatus.CANCELED.value
    )
    locked_task.error = failure_code
    locked_task.lease_until = None
    attempt.status = "FAILED" if expired else "CANCELED"
    attempt.failure_code = failure_code
    attempt.updated_at = now
    attempt.completed_at = now
    if attempt.stage == "MODEL":
        record.model_status = "FAILED"
        record.model_failure_code = failure_code
    else:
        record.image_status = "FAILED"
        record.image_failure_code = failure_code
    record.updated_at = now
    node = session.get(Node, locked_task.node_id)
    if node is not None:
        node.desired_state = None
    _recompute_status(session, preparation)
    return True


def revoke_preparation_tasks_for_node(
    session: Session, node_id: str
) -> int:
    tasks = list(
        session.scalars(
            select(Task)
            .where(
                Task.node_id == node_id,
                Task.type.in_(PREPARATION_TASK_TYPES),
                Task.status.in_(
                    {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
                ),
            )
            .order_by(Task.created_at, Task.id)
            .with_for_update()
        )
    )
    affected: set[str] = set()
    now = utcnow()
    for task in tasks:
        attempt, record, preparation = _bound_attempt(
            session, task, lock=True
        )
        if (
            attempt is None
            or record is None
            or preparation is None
            or not _attempt_is_current(task, attempt, record, preparation)
        ):
            # Credential revocation is the primary safety action. A corrupted
            # child binding must not roll that action back; leave the task
            # fenced by credential loss and surface the cleanup incident.
            session.add(
                AuditEvent(
                    actor="controller",
                    action="deployment.prepare.revoke_cleanup",
                    target=task.id,
                    outcome="failure",
                    detail={"code": "PREPARATION_ATTEMPT_CONFLICT"},
                )
            )
            continue
        was_running = task.status == TaskStatus.RUNNING.value
        task.status = (
            TaskStatus.FAILED.value
            if was_running
            else TaskStatus.CANCELED.value
        )
        task.result = None
        task.error = "PREPARATION_NODE_REVOKED"
        task.lease_until = None
        attempt.status = "FAILED" if was_running else "CANCELED"
        attempt.failure_code = "PREPARATION_NODE_REVOKED"
        attempt.result = None
        attempt.updated_at = now
        attempt.completed_at = now
        if attempt.stage == "MODEL":
            record.model_status = "FAILED"
            record.model_failure_code = "PREPARATION_NODE_REVOKED"
        else:
            record.image_status = "FAILED"
            record.image_failure_code = "PREPARATION_NODE_REVOKED"
        record.updated_at = now
        affected.add(preparation.id)
    for preparation_id in sorted(affected):
        preparation = session.get(ArtifactPreparation, preparation_id)
        if preparation is not None:
            _recompute_status(session, preparation)
    return len(tasks)


def manifest_for_preparation_task(
    session: Session, task_id: str, node_id: str
) -> dict[str, Any]:
    # All central node mutations use Node -> Task -> operation state. Taking
    # the authenticated node first keeps manifest reads from deadlocking with
    # lease-expiry reconciliation, retry, completion, or credential revoke.
    node = session.scalar(
        select(Node).where(Node.id == node_id).with_for_update()
    )
    if node is None or not node.approved:
        raise ArtifactPreparationNotFoundError(
            "preparation manifest is unavailable",
            code="PREPARATION_MANIFEST_UNAVAILABLE",
        )
    task = session.scalar(
        select(Task).where(Task.id == task_id).with_for_update()
    )
    if task is None or task.type != TaskType.PREPARE_MODEL.value:
        raise ArtifactPreparationNotFoundError(
            "preparation manifest is unavailable",
            code="PREPARATION_MANIFEST_UNAVAILABLE",
        )
    attempt, record, preparation = _bound_attempt(session, task, lock=True)
    lease_until = _aware(task.lease_until)
    if (
        attempt is None
        or record is None
        or preparation is None
        or task.node_id != node_id
        or task.status != TaskStatus.RUNNING.value
        or lease_until is None
        or lease_until < utcnow()
        or attempt.status != "RUNNING"
        or not _attempt_is_current(task, attempt, record, preparation)
        or task.payload.get("manifest_digest")
        != record.model_manifest_digest
    ):
        raise ArtifactPreparationError(
            "preparation manifest is unavailable",
            code="PREPARATION_MANIFEST_UNAVAILABLE",
        )
    manifest = session.get(ArtifactManifest, record.model_manifest_digest)
    if manifest is None:
        raise ArtifactPreparationError(
            "preparation manifest is unavailable",
            code="PREPARATION_MANIFEST_UNAVAILABLE",
        )
    try:
        from .service import artifact_manifest_dict

        value = artifact_manifest_dict(session, manifest)
    except ValueError as exc:
        raise ArtifactPreparationError(
            "preparation manifest is unavailable",
            code="PREPARATION_MANIFEST_UNAVAILABLE",
        ) from exc
    return {
        "schema_version": value["schema_version"],
        "files": value["files"],
    }


def effective_deployment_plan(
    session: Session,
    deployment: Deployment,
    *,
    require_prepared: bool = True,
    lock_ready_caches: bool = True,
) -> dict[str, Any]:
    if deployment.source_recommendation_id is None:
        return deployment.plan
    preparation = session.scalar(
        select(ArtifactPreparation).where(
            ArtifactPreparation.deployment_id == deployment.id
        )
    )
    if not require_prepared:
        # STOP and rollback STOP_SOURCE never revalidate current release or
        # stage-registry eligibility.  They still need the immutable effective
        # cache-kind projection so a STAGE container can be matched by its
        # exact rank labels after the variant is revoked.  Accept only the two
        # narrow plan changes preparation is allowed to persist; corruption
        # falls back to the original plan and therefore cannot broaden a stop.
        snapshot = (
            preparation.plan_snapshot
            if preparation is not None
            and isinstance(preparation.plan_snapshot, dict)
            else None
        )
        projected = (
            snapshot.get("effective_plan")
            if isinstance(snapshot, dict)
            else None
        )
        if isinstance(projected, dict):
            normalized = copy.deepcopy(projected)
            original = copy.deepcopy(deployment.plan)
            normalized["model_path"] = original.get("model_path")
            if normalized.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE:
                normalized.pop("stage_artifact", None)
                normalized["model_cache_kind"] = original.get(
                    "model_cache_kind"
                )
                assignments = normalized.get("assignments")
                if isinstance(assignments, list):
                    for assignment in assignments:
                        if isinstance(assignment, dict):
                            assignment.pop("stage_manifest_digest", None)
                            assignment.pop("stage_tensor_keys_digest", None)
            try:
                parsed = DeploymentPlan.from_dict(projected)
            except (KeyError, TypeError, ValueError):
                parsed = None
            if (
                parsed is not None
                and normalized == original
                and snapshot.get("deployment_id") == deployment.id
                and snapshot.get("generation") == deployment.generation
                and snapshot.get("source_recommendation_id")
                == deployment.source_recommendation_id
            ):
                return copy.deepcopy(projected)
        return copy.deepcopy(deployment.plan)
    if preparation is None or preparation.status != "SUCCEEDED":
        raise ArtifactPreparationError(
            "recommended deployment artifacts are not fully prepared",
            code="DEPLOYMENT_ARTIFACTS_NOT_PREPARED",
            details={"deployment_id": deployment.id},
        )
    recommendation = session.scalar(
        select(DeploymentRecommendationRecord).where(
            DeploymentRecommendationRecord.id
            == deployment.source_recommendation_id
        )
    )
    selected = (
        recommendation.recommendation_snapshot.get("selected")
        if recommendation is not None
        and isinstance(recommendation.recommendation_snapshot, dict)
        else None
    )
    release_id = (
        selected.get("model_release_id")
        if isinstance(selected, dict)
        else None
    )
    # Mutation callers lock their complete node scope before reaching this
    # gate.  Hold the release transition barrier through their task-creation
    # commit so a concurrent revoke cannot slip between validation and queueing.
    _lock_model_release_transitions(session)
    release = (
        session.scalar(
            select(ModelRelease)
            .where(ModelRelease.id == release_id)
            .execution_options(populate_existing=True)
        )
        if isinstance(release_id, str)
        else None
    )
    if release is None or release.status == "REVOKED":
        raise ArtifactPreparationError(
            "prepared deployment references a missing or revoked model release",
            code="DEPLOYMENT_MODEL_RELEASE_REVOKED",
            details={
                "deployment_id": deployment.id,
                "model_release_id": release_id,
            },
        )
    snapshot = preparation.plan_snapshot
    plan = snapshot.get("effective_plan")
    artifact = snapshot.get("artifact")
    stage_artifact = snapshot.get("stage_artifact")
    cache_kind = (
        artifact.get("cache_kind") if isinstance(artifact, dict) else None
    )
    expected_node_ids = sorted(
        item.get("node_id")
        for item in deployment.plan.get("assignments", [])
        if isinstance(item, dict) and isinstance(item.get("node_id"), str)
    )
    if (
        not isinstance(plan, dict)
        or not isinstance(artifact, dict)
        or snapshot.get("deployment_id") != deployment.id
        or snapshot.get("generation") != deployment.generation
        or snapshot.get("source_recommendation_id")
        != deployment.source_recommendation_id
        or snapshot.get("model_release_id") != release_id
        or snapshot.get("node_ids") != expected_node_ids
        or plan.get("deployment_id") != deployment.id
        or plan.get("generation") != deployment.generation
        or snapshot.get("runtime_image") != deployment.plan.get("image")
        or cache_kind
        not in {MODEL_CACHE_KIND_FULL_SNAPSHOT, MODEL_CACHE_KIND_STAGE}
        or not isinstance(artifact.get("manifest_digest"), str)
        or re.fullmatch(
            r"sha256:[0-9a-f]{64}", artifact["manifest_digest"]
        )
        is None
    ):
        raise ArtifactPreparationError(
            "prepared deployment plan evidence is inconsistent",
            code="DEPLOYMENT_PREPARATION_INVALID",
            details={"deployment_id": deployment.id},
        )
    expected_plan = copy.deepcopy(deployment.plan)
    expected_manifest_by_node: dict[str, str] = {}
    expected_model_summary_by_node: dict[str, tuple[int, int]] = {}
    if cache_kind == MODEL_CACHE_KIND_FULL_SNAPSHOT:
        if stage_artifact is not None:
            raise ArtifactPreparationError(
                "FULL preparation unexpectedly contains stage identity",
                code="DEPLOYMENT_PREPARATION_INVALID",
                details={"deployment_id": deployment.id},
            )
        expected_plan["model_path"] = (
            "/var/lib/dure/models/sha256-"
            + artifact["manifest_digest"].removeprefix("sha256:")
        )
        expected_manifest_by_node = {
            node_id: artifact["manifest_digest"]
            for node_id in expected_node_ids
        }
        expected_model_summary_by_node = {
            node_id: (
                artifact.get("total_size_bytes"),
                artifact.get("file_count"),
            )
            for node_id in expected_node_ids
        }
    else:
        if not isinstance(stage_artifact, dict):
            raise ArtifactPreparationError(
                "STAGE preparation has no immutable variant identity",
                code="DEPLOYMENT_PREPARATION_INVALID",
                details={"deployment_id": deployment.id},
            )
        common_fields = (
            "artifact_set_digest",
            "contract_identity_digest",
            "source_manifest_digest",
            "runtime_image",
            "vllm_version",
            "exporter_build_digest",
            "architecture",
            "quantization",
            "tensor_parallel_size",
            "pipeline_parallel_size",
            "loader_format",
        )
        bindings = stage_artifact.get("node_bindings")
        if (
            type(bindings) is not list
            or len(bindings) != len(expected_node_ids)
            or stage_artifact.get("source_manifest_digest")
            != artifact["manifest_digest"]
            or stage_artifact.get("runtime_image") != snapshot["runtime_image"]
        ):
            raise ArtifactPreparationError(
                "STAGE preparation variant binding is inconsistent",
                code="DEPLOYMENT_PREPARATION_INVALID",
                details={"deployment_id": deployment.id},
            )
        try:
            current_projection = validated_stage_artifact_projection(
                session, stage_artifact["artifact_set_digest"]
            )
        except (StageArtifactNotFoundError, StageArtifactConflictError, ValueError) as exc:
            raise ArtifactPreparationError(
                "prepared STAGE variant is no longer validated",
                code="DEPLOYMENT_STAGE_VARIANT_UNAVAILABLE",
                details={
                    "deployment_id": deployment.id,
                    "artifact_set_digest": stage_artifact.get(
                        "artifact_set_digest"
                    ),
                },
            ) from exc
        projected_ranks = [
            {
                key: binding[key]
                for key in (
                    "rank",
                    "pipeline_rank",
                    "tensor_rank",
                    "manifest_digest",
                    "tensor_key_count",
                    "tensor_keys_digest",
                    "weight_size_bytes",
                    "total_size_bytes",
                    "file_count",
                )
            }
            for binding in bindings
            if type(binding) is dict
        ]
        expected_projection = {
            **{key: stage_artifact.get(key) for key in common_fields},
            "ranks": projected_ranks,
        }
        if current_projection != expected_projection:
            raise ArtifactPreparationError(
                "prepared STAGE variant registry projection changed",
                code="DEPLOYMENT_STAGE_VARIANT_UNAVAILABLE",
                details={"deployment_id": deployment.id},
            )
        expected_plan["model_path"] = "/var/lib/dure/models/stages"
        expected_plan["model_cache_kind"] = MODEL_CACHE_KIND_STAGE
        expected_plan["stage_artifact"] = {
            key: stage_artifact[key] for key in common_fields
        }
        binding_by_node = {
            item.get("node_id"): item
            for item in bindings
            if type(item) is dict
        }
        if (
            len(binding_by_node) != len(expected_node_ids)
            or set(binding_by_node) != set(expected_node_ids)
        ):
            raise ArtifactPreparationError(
                "prepared STAGE node rank set is incomplete",
                code="DEPLOYMENT_PREPARATION_INVALID",
                details={"deployment_id": deployment.id},
            )
        for assignment in expected_plan.get("assignments", []):
            binding = binding_by_node.get(assignment.get("node_id"))
            if binding is None:
                raise ArtifactPreparationError(
                    "prepared STAGE assignment is incomplete",
                    code="DEPLOYMENT_PREPARATION_INVALID",
                )
            assignment["stage_manifest_digest"] = binding[
                "manifest_digest"
            ]
            assignment["stage_tensor_keys_digest"] = binding[
                "tensor_keys_digest"
            ]
        expected_manifest_by_node = {
            node_id: binding_by_node[node_id]["manifest_digest"]
            for node_id in expected_node_ids
        }
        expected_model_summary_by_node = {
            node_id: (
                binding_by_node[node_id]["total_size_bytes"],
                binding_by_node[node_id]["file_count"],
            )
            for node_id in expected_node_ids
        }
    if plan != expected_plan:
        raise ArtifactPreparationError(
            "prepared deployment plan evidence is inconsistent",
            code="DEPLOYMENT_PREPARATION_INVALID",
            details={"deployment_id": deployment.id},
        )
    records = _preparation_nodes(session, preparation.id)
    if [record.node_id for record in records] != expected_node_ids:
        raise ArtifactPreparationError(
            "prepared node evidence is incomplete",
            code="DEPLOYMENT_PREPARATION_INVALID",
            details={"deployment_id": deployment.id},
        )
    for record in records:
        if (
            record.model_status != "SUCCEEDED"
            or record.image_status != "SUCCEEDED"
            or record.model_failure_code is not None
            or record.image_failure_code is not None
            or record.model_manifest_digest
            != expected_manifest_by_node.get(record.node_id)
            or record.runtime_image != snapshot["runtime_image"]
        ):
            raise ArtifactPreparationError(
                "prepared node evidence is incomplete",
                code="DEPLOYMENT_PREPARATION_INVALID",
                details={
                    "deployment_id": deployment.id,
                    "node_id": record.node_id,
                },
            )
        try:
            cache_identity = _artifact_cache_identity(snapshot, record.node_id)
            require_ready_cache(
                session,
                node_id=record.node_id,
                identity=cache_identity,
                lock=lock_ready_caches,
            )
        except (ArtifactCacheLifecycleError, ArtifactPreparationError) as exc:
            raise ArtifactPreparationError(
                "the exact node artifact cache is not READY",
                code="DEPLOYMENT_ARTIFACT_CACHE_NOT_READY",
                details={
                    "deployment_id": deployment.id,
                    "node_id": record.node_id,
                    "cache_failure_code": getattr(exc, "code", None),
                },
            ) from exc
        for stage, attempt_no in (
            ("MODEL", record.model_current_attempt),
            ("IMAGE", record.image_current_attempt),
        ):
            attempt = session.scalar(
                select(ArtifactPreparationAttempt).where(
                    ArtifactPreparationAttempt.preparation_node_id
                    == record.id,
                    ArtifactPreparationAttempt.stage == stage,
                    ArtifactPreparationAttempt.attempt_no == attempt_no,
                )
            )
            task = session.get(Task, attempt.task_id) if attempt else None
            result = attempt.result if attempt else None
            validated_result: dict[str, Any] | None = None
            if task is not None and isinstance(result, dict):
                try:
                    validated_result = validate_preparation_result(
                        _task_wire(task), result, record.node_id
                    )
                except Exception:
                    validated_result = None
            if (
                attempt is None
                or task is None
                or attempt.status != "SUCCEEDED"
                or task.status != TaskStatus.SUCCEEDED.value
                or not isinstance(result, dict)
                or not _attempt_is_current(
                    task, attempt, record, preparation
                )
                or validated_result != result
                or task.result != result
            ):
                raise ArtifactPreparationError(
                    "prepared attempt evidence is incomplete",
                    code="DEPLOYMENT_PREPARATION_INVALID",
                    details={
                        "deployment_id": deployment.id,
                        "node_id": record.node_id,
                        "stage": stage,
                    },
                )
            if stage == "MODEL" and (
                result.get("manifest_digest")
                != expected_manifest_by_node[record.node_id]
                or result.get("bytes_verified")
                != expected_model_summary_by_node[record.node_id][0]
                or result.get("file_count")
                != expected_model_summary_by_node[record.node_id][1]
            ):
                raise ArtifactPreparationError(
                    "prepared model evidence is inconsistent",
                    code="DEPLOYMENT_PREPARATION_INVALID",
                    details={"deployment_id": deployment.id},
                )
            if stage == "IMAGE" and result.get("runtime_image") != snapshot[
                "runtime_image"
            ]:
                raise ArtifactPreparationError(
                    "prepared image evidence is inconsistent",
                    code="DEPLOYMENT_PREPARATION_INVALID",
                    details={"deployment_id": deployment.id},
                )
    return copy.deepcopy(plan)
