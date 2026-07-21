from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dure.resource_pool import FLEET_MODEL_IDS
from dure.fleet_scheduler import FleetSchedulingError
from dure.models import DeploymentPlan

from .fleet import FleetEvaluationError, _plan_contract_digest
from .fleet_recommendation import (
    FleetRecommendationError,
    FleetRecommendationIntegrityError,
    FleetRecommendationNotFoundError,
    evaluate_fleet_recommendation,
    validate_stored_fleet_recommendation,
)
from .models import (
    AuditEvent,
    Deployment,
    FleetDeploymentRuntime,
    FleetRecommendationRecord,
    FleetRecord,
    FleetResourceReservation,
    Node,
)
from .recommendation import (
    RecommendationError,
    RecommendationNotAcceptableError,
    _build_generation_plan,
    _lock_recommendation_inputs,
    deployment_generation_dict,
)
from .resource_reservation import lock_fleet_reservation_gate
from .service import aware


FLEET_ACCEPTANCE_NAMESPACE = uuid.UUID(
    "7bda5521-8340-40fb-a374-2e167be5f7ee"
)
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}")


class FleetAcceptanceError(FleetRecommendationError):
    pass


class FleetNotFoundError(FleetAcceptanceError):
    def __init__(self, fleet_id: str) -> None:
        super().__init__(
            "Fleet not found",
            code="FLEET_NOT_FOUND",
            details={"fleet_id": fleet_id},
        )


def _error(
    message: str,
    *,
    code: str,
    details: dict[str, Any] | None = None,
) -> FleetAcceptanceError:
    return FleetAcceptanceError(message, code=code, details=details)


def _lock_fleet_inputs(
    session: Session,
    recommendation: FleetRecommendationRecord,
) -> None:
    # Every path that can create resource ownership takes this gate first.
    # Keeping the order gate -> frozen inventory rows prevents a node-row/gate
    # inversion with qualification, task, benchmark and rollout writers.
    lock_fleet_reservation_gate(session)
    # Lock only the frozen inventory rows. Registry/evidence changes are
    # detected by the byte-for-byte re-evaluation below and can be serialized
    # immediately before or after this acceptance. Avoiding broad table locks
    # keeps node/task/evidence completion paths deadlock-free.
    _lock_recommendation_inputs(session, recommendation)  # type: ignore[arg-type]


def _canonical_uuid(value: Any, *, field: str) -> str:
    if type(value) is not str:
        raise _error(
            f"{field} must be a canonical UUID",
            code="FLEET_BINDING_INVALID",
        )
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except ValueError as exc:
        raise _error(
            f"{field} must be a canonical UUID",
            code="FLEET_BINDING_INVALID",
        ) from exc
    return value


def _selected_candidates(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    evaluation = snapshot.get("evaluation")
    schedule = evaluation.get("schedule") if isinstance(evaluation, dict) else None
    if not isinstance(schedule, dict):
        raise FleetRecommendationIntegrityError()
    if schedule.get("unmet_minimum_replicas"):
        raise _error(
            "Fleet minimum replica policy is not satisfied",
            code="FLEET_MINIMUM_REPLICAS_UNMET",
            details={
                "unmet_minimum_replicas": schedule.get(
                    "unmet_minimum_replicas"
                )
            },
        )
    score = schedule.get("score")
    if not isinstance(score, dict) or score.get("reserve_policy_met") is not True:
        raise _error(
            "Fleet reserve-node policy is not satisfied",
            code="FLEET_RESERVE_POLICY_UNMET",
        )
    selected = schedule.get("selected")
    if not isinstance(selected, list) or not selected:
        raise _error(
            "Fleet recommendation contains no selected deployment",
            code="FLEET_RECOMMENDATION_EMPTY",
        )

    expected_ids = evaluation.get("selected_candidate_ids")
    candidate_ids = [
        item.get("candidate_id") if isinstance(item, dict) else None
        for item in selected
    ]
    if (
        any(type(candidate_id) is not str for candidate_id in candidate_ids)
        or len(candidate_ids) != len(set(candidate_ids))
        or sorted(candidate_ids) != expected_ids
    ):
        raise FleetRecommendationIntegrityError(
            "stored Fleet selection identity is invalid"
        )
    return selected


def _candidate_bindings(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    candidate_id = candidate.get("candidate_id")
    if type(candidate_id) is not str or _SHA256.fullmatch(candidate_id) is None:
        raise _error(
            "Fleet candidate ID is invalid",
            code="FLEET_BINDING_INVALID",
        )
    if candidate.get("model_id") not in FLEET_MODEL_IDS:
        raise _error(
            "Fleet candidate model is outside the allowlist",
            code="FLEET_MODEL_NOT_ALLOWED",
        )
    if type(candidate.get("tensor_parallel_size")) is not int or candidate.get(
        "tensor_parallel_size"
    ) != 1:
        raise _error(
            "Fleet candidate requires TP=1",
            code="FLEET_TP_UNSUPPORTED",
        )
    bindings = candidate.get("bindings")
    pipeline_parallel_size = candidate.get("pipeline_parallel_size")
    if (
        not isinstance(bindings, list)
        or not bindings
        or type(pipeline_parallel_size) is not int
        or pipeline_parallel_size != len(bindings)
    ):
        raise _error(
            "Fleet PP must equal the exact binding count",
            code="FLEET_BINDING_INVALID",
        )

    normalized: list[dict[str, Any]] = []
    nodes: set[str] = set()
    gpu_uuids: set[str] = set()
    ranks: set[int] = set()
    for binding in bindings:
        if not isinstance(binding, dict) or set(binding) != {
            "node_id",
            "gpu_index",
            "gpu_uuid",
            "rank",
        }:
            raise _error(
                "Fleet candidate binding schema is invalid",
                code="FLEET_BINDING_INVALID",
            )
        node_id = _canonical_uuid(binding.get("node_id"), field="node_id")
        gpu_index = binding.get("gpu_index")
        gpu_uuid = binding.get("gpu_uuid")
        rank = binding.get("rank")
        if (
            type(gpu_index) is not int
            or gpu_index < 0
            or type(gpu_uuid) is not str
            or not gpu_uuid.startswith("GPU-")
            or len(gpu_uuid) > 128
            or type(rank) is not int
            or rank < 0
            or node_id in nodes
            or gpu_uuid in gpu_uuids
            or rank in ranks
        ):
            raise _error(
                "Fleet candidate contains a duplicate or invalid GPU binding",
                code="FLEET_BINDING_INVALID",
            )
        nodes.add(node_id)
        gpu_uuids.add(gpu_uuid)
        ranks.add(rank)
        normalized.append(
            {
                "node_id": node_id,
                "gpu_index": gpu_index,
                "gpu_uuid": gpu_uuid,
                "rank": rank,
            }
        )
    normalized.sort(key=lambda item: item["rank"])
    if [item["rank"] for item in normalized] != list(range(len(normalized))):
        raise _error(
            "Fleet candidate ranks must be contiguous from zero",
            code="FLEET_BINDING_INVALID",
        )
    if candidate.get("rank_node_ids") != [
        item["node_id"] for item in normalized
    ]:
        raise _error(
            "Fleet rank-to-node identity is invalid",
            code="FLEET_BINDING_INVALID",
        )
    evidence_bindings = candidate.get("gpu_bindings")
    if not isinstance(evidence_bindings, list) or [
        {
            key: item.get(key)
            for key in ("node_id", "gpu_index", "gpu_uuid", "rank")
        }
        for item in sorted(
            evidence_bindings,
            key=lambda item: item.get("rank") if isinstance(item, dict) else -1,
        )
        if isinstance(item, dict)
    ] != normalized:
        raise _error(
            "Fleet scheduler and qualification GPU bindings differ",
            code="FLEET_BINDING_INVALID",
        )
    stage_node_bindings = candidate.get("stage_node_bindings")
    if candidate.get("model_cache_kind") == "STAGE":
        stage_ranks = candidate.get("stage_ranks")
        if (
            not isinstance(stage_ranks, list)
            or len(stage_ranks) != len(normalized)
            or any(not isinstance(item, dict) for item in stage_ranks)
        ):
            raise _error(
                "Fleet STAGE rank metadata is invalid",
                code="FLEET_BINDING_INVALID",
            )
        expected_stage_bindings = [
            {"node_id": normalized[rank]["node_id"], **stage_ranks[rank]}
            for rank in range(len(stage_ranks))
        ]
        if stage_node_bindings != expected_stage_bindings:
            raise _error(
                "Fleet STAGE node/rank binding differs from qualification",
                code="FLEET_BINDING_INVALID",
            )
    elif stage_node_bindings is not None:
        raise _error(
            "FULL_SNAPSHOT Fleet candidate contains STAGE node bindings",
            code="FLEET_BINDING_INVALID",
        )
    _canonical_uuid(candidate.get("placement_id"), field="placement_id")
    return normalized


def _assert_plan_candidate_identity(
    *,
    plan: dict[str, Any],
    candidate: dict[str, Any],
    bindings: list[dict[str, Any]],
    deployment_id: str,
    generation: int,
) -> None:
    candidate_id = candidate["candidate_id"]
    if _SHA256.fullmatch(str(candidate.get("plan_contract_digest"))) is None:
        raise _error(
            "Fleet candidate has no immutable plan contract digest",
            code="FLEET_GENERATION_IDENTITY_MISMATCH",
            details={"candidate_id": candidate_id},
        )
    try:
        parsed = DeploymentPlan.from_dict(plan)
    except (KeyError, TypeError, ValueError) as exc:
        raise _error(
            "Fleet deployment plan violates the runtime wire contract",
            code="FLEET_GENERATION_PLAN_INVALID",
            details={"candidate_id": candidate_id},
        ) from exc
    actual_bindings = [
        {
            "node_id": item.node_id,
            "gpu_index": item.gpu_index,
            "gpu_uuid": item.gpu_uuid,
            "rank": item.rank,
        }
        for item in parsed.assignments
    ]
    cache_kind = candidate.get("model_cache_kind")
    expected_model_path = (
        "/var/lib/dure/models/stages"
        if cache_kind == "STAGE"
        else (
            "/var/lib/dure/models/sha256-"
            + str(candidate.get("artifact_manifest_digest", "")).removeprefix(
                "sha256:"
            )
            if len(bindings) > 1
            else "/var/lib/dure/models/"
            f"{candidate.get('model_id')}--{candidate.get('artifact_revision')}"
        )
    )
    if (
        _plan_contract_digest(plan) != candidate["plan_contract_digest"]
        or
        actual_bindings != bindings
        or parsed.deployment_id != deployment_id
        or parsed.generation != generation
        or parsed.model.model_id != candidate.get("model_id")
        or parsed.model.repository != candidate.get("artifact_repository")
        or parsed.model.quantization != candidate.get("quantization")
        or parsed.model_revision != candidate.get("artifact_revision")
        or parsed.image != candidate.get("runtime_image")
        or parsed.tensor_parallel_size != 1
        or parsed.pipeline_parallel_size != len(bindings)
        or parsed.ray_head_node_id != bindings[0]["node_id"]
        or parsed.execution_backend != candidate.get("execution_backend")
        or parsed.runtime_vllm_version
        != candidate.get("runtime_vllm_version")
        or parsed.model_cache_kind != cache_kind
        or parsed.model_path != expected_model_path
    ):
        raise _error(
            "stored Fleet plan changed the immutable candidate identity",
            code="FLEET_GENERATION_IDENTITY_MISMATCH",
            details={"candidate_id": candidate_id},
        )
    if cache_kind == "STAGE":
        expected_stage = candidate.get("stage_artifact")
        if (
            parsed.stage_artifact is None
            or parsed.stage_artifact.to_dict() != expected_stage
        ):
            raise _error(
                "stored Fleet plan changed the STAGE artifact identity",
                code="FLEET_GENERATION_IDENTITY_MISMATCH",
                details={"candidate_id": candidate_id},
            )
        expected_stages = candidate.get("stage_node_bindings")
        if not isinstance(expected_stages, list) or any(
            not isinstance(item, dict) for item in expected_stages
        ):
            raise _error(
                "Fleet STAGE rank identity is not representable",
                code="FLEET_GENERATION_IDENTITY_MISMATCH",
                details={"candidate_id": candidate_id},
            )
        expected_stage_projection = [
            {
                "node_id": item.get("node_id"),
                "rank": item.get("rank"),
                "manifest_digest": item.get("manifest_digest"),
                "tensor_keys_digest": item.get("tensor_keys_digest"),
            }
            for item in expected_stages
        ]
        actual_stage_projection = [
            {
                "node_id": item.node_id,
                "rank": item.rank,
                "manifest_digest": item.stage_manifest_digest,
                "tensor_keys_digest": item.stage_tensor_keys_digest,
            }
            for item in parsed.assignments
        ]
        if expected_stage_projection != actual_stage_projection:
            raise _error(
                "stored Fleet plan changed a STAGE rank identity",
                code="FLEET_GENERATION_IDENTITY_MISMATCH",
                details={"candidate_id": candidate_id},
            )
    elif parsed.stage_artifact is not None:
        raise _error(
            "stored FULL_SNAPSHOT plan contains a STAGE artifact",
            code="FLEET_GENERATION_IDENTITY_MISMATCH",
            details={"candidate_id": candidate_id},
        )


def _plan_for_candidate(
    session: Session,
    *,
    recommendation_id: str,
    fleet_id: str,
    candidate: dict[str, Any],
    bindings: list[dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    candidate_id = candidate["candidate_id"]
    deployment_id = str(
        uuid.uuid5(
            FLEET_ACCEPTANCE_NAMESPACE,
            f"fleet-deployment:{fleet_id}:{candidate_id}",
        )
    )
    selected = dict(candidate)
    selected.update(
        {
            "feasible": True,
            "node_ids": sorted(item["node_id"] for item in bindings),
            "rejections": [],
        }
    )
    plan = _build_generation_plan(
        session,
        recommendation={
            "id": recommendation_id,
            "selected": selected,
        },
        deployment_id=deployment_id,
        generation=1,
    )
    assignments = plan.get("assignments")
    if (
        not isinstance(assignments, list)
        or len(assignments) != len(bindings)
        or any(
            not isinstance(item, dict)
            or type(item.get("rank")) is not int
            for item in assignments
        )
    ):
        raise _error(
            "Fleet deployment plan has no exact assignments",
            code="FLEET_GENERATION_PLAN_INVALID",
        )
    actual = sorted(
        (
            {
                "node_id": item.get("node_id"),
                "gpu_index": item.get("gpu_index"),
                "gpu_uuid": item.get("gpu_uuid"),
                "rank": item.get("rank"),
            }
            for item in assignments
            if isinstance(item, dict)
        ),
        key=lambda item: item["rank"],
    )
    if actual != bindings:
        raise _error(
            "generated plan changed the qualified node/GPU/rank binding",
            code="FLEET_GENERATION_BINDING_MISMATCH",
            details={"candidate_id": candidate_id},
        )
    try:
        parsed = DeploymentPlan.from_dict(plan)
    except (KeyError, TypeError, ValueError) as exc:
        raise _error(
            "Fleet deployment plan violates the runtime wire contract",
            code="FLEET_GENERATION_PLAN_INVALID",
            details={"candidate_id": candidate_id},
        ) from exc
    expected_head = bindings[0]["node_id"]
    if (
        parsed.deployment_id != deployment_id
        or parsed.generation != 1
        or parsed.model.model_id != candidate.get("model_id")
        or parsed.model_revision != candidate.get("artifact_revision")
        or parsed.image != candidate.get("runtime_image")
        or parsed.tensor_parallel_size != 1
        or parsed.pipeline_parallel_size != len(bindings)
        or parsed.ray_head_node_id != expected_head
        or parsed.model_cache_kind != candidate.get("model_cache_kind")
    ):
        raise _error(
            "generated plan changed the immutable Fleet identity",
            code="FLEET_GENERATION_IDENTITY_MISMATCH",
            details={"candidate_id": candidate_id},
        )
    if candidate.get("model_cache_kind") == "STAGE":
        stage = candidate.get("stage_artifact")
        if parsed.stage_artifact is None or parsed.stage_artifact.to_dict() != stage:
            raise _error(
                "generated plan changed the Fleet STAGE artifact identity",
                code="FLEET_GENERATION_IDENTITY_MISMATCH",
                details={"candidate_id": candidate_id},
            )
        expected_stages = candidate.get("stage_node_bindings")
        actual_stages = [
            {
                "node_id": item.node_id,
                "rank": item.rank,
                "manifest_digest": item.stage_manifest_digest,
                "tensor_keys_digest": item.stage_tensor_keys_digest,
            }
            for item in parsed.assignments
        ]
        if (
            not isinstance(expected_stages, list)
            or len(expected_stages) != len(actual_stages)
            or any(not isinstance(item, dict) for item in expected_stages)
        ):
            raise _error(
                "Fleet STAGE rank identity is not representable",
                code="FLEET_GENERATION_IDENTITY_MISMATCH",
                details={"candidate_id": candidate_id},
            )
        expected_stage_projection = [
            {
                "node_id": item.get("node_id"),
                "rank": item.get("rank"),
                "manifest_digest": item.get("manifest_digest"),
                "tensor_keys_digest": item.get("tensor_keys_digest"),
            }
            for item in expected_stages
        ]
        if expected_stage_projection != actual_stages:
            raise _error(
                "generated plan changed a Fleet STAGE rank identity",
                code="FLEET_GENERATION_IDENTITY_MISMATCH",
                details={"candidate_id": candidate_id},
            )
    elif parsed.stage_artifact is not None:
        raise _error(
            "generated FULL_SNAPSHOT plan contains a STAGE artifact",
            code="FLEET_GENERATION_IDENTITY_MISMATCH",
            details={"candidate_id": candidate_id},
        )
    _assert_plan_candidate_identity(
        plan=plan,
        candidate=candidate,
        bindings=bindings,
        deployment_id=deployment_id,
        generation=1,
    )
    return deployment_id, plan


def _reservation_dict(record: FleetResourceReservation) -> dict[str, Any]:
    return {
        "id": record.id,
        "fleet_id": record.fleet_id,
        "deployment_id": record.deployment_id,
        "node_id": record.node_id,
        "gpu_index": record.gpu_index,
        "gpu_uuid": record.gpu_uuid,
        "rank": record.rank,
        "released_at": (
            aware(record.released_at).isoformat()
            if record.released_at is not None
            else None
        ),
        "created_at": aware(record.created_at).isoformat(),
    }


def fleet_detail(session: Session, fleet: FleetRecord) -> dict[str, Any]:
    deployments = list(
        session.scalars(
            select(Deployment)
            .where(Deployment.fleet_id == fleet.id)
            .order_by(Deployment.fleet_candidate_id, Deployment.id)
        )
    )
    reservations = list(
        session.scalars(
            select(FleetResourceReservation)
            .where(FleetResourceReservation.fleet_id == fleet.id)
            .order_by(
                FleetResourceReservation.node_id,
                FleetResourceReservation.gpu_uuid,
            )
        )
    )
    from .fleet_runtime import fleet_runtime_projection

    return {
        "id": fleet.id,
        "source_recommendation_id": fleet.source_recommendation_id,
        "status": fleet.status,
        "deployments": [
            {
                **deployment_generation_dict(deployment),
                "fleet_candidate_id": deployment.fleet_candidate_id,
            }
            for deployment in deployments
        ],
        "reservations": [_reservation_dict(item) for item in reservations],
        "runtime": fleet_runtime_projection(session, fleet.id),
        "created_at": aware(fleet.created_at).isoformat(),
        "updated_at": aware(fleet.updated_at).isoformat(),
    }


def _validated_existing_fleet(
    session: Session,
    *,
    fleet: FleetRecord,
    recommendation: FleetRecommendationRecord,
) -> dict[str, Any]:
    detail = fleet_detail(session, fleet)
    selected = _selected_candidates(recommendation.recommendation_snapshot)
    deployment_records = list(
        session.scalars(
            select(Deployment)
            .where(Deployment.fleet_id == fleet.id)
            .order_by(Deployment.fleet_candidate_id, Deployment.id)
        )
    )
    records_by_candidate = {
        item.fleet_candidate_id: item for item in deployment_records
    }
    for candidate in selected:
        candidate_id = candidate["candidate_id"]
        deployment = records_by_candidate.get(candidate_id)
        expected_deployment_id = str(
            uuid.uuid5(
                FLEET_ACCEPTANCE_NAMESPACE,
                f"fleet-deployment:{fleet.id}:{candidate_id}",
            )
        )
        if (
            deployment is None
            or deployment.id != expected_deployment_id
            or deployment.lineage_id != deployment.id
            or deployment.previous_generation_id is not None
            or deployment.source_recommendation_id is not None
            or deployment.fleet_id != fleet.id
            or deployment.generation != 1
            or deployment.accept_model_download is not False
            or deployment.pull_image is not False
        ):
            raise _error(
                "stored Fleet deployment lineage is invalid",
                code="FLEET_ACCEPTANCE_RECORD_INVALID",
                details={"fleet_id": fleet.id, "candidate_id": candidate_id},
            )
        _assert_plan_candidate_identity(
            plan=deployment.plan,
            candidate=candidate,
            bindings=_candidate_bindings(candidate),
            deployment_id=deployment.id,
            generation=1,
        )
    expected_candidates = sorted(item["candidate_id"] for item in selected)
    actual_candidate_values = [
        item["fleet_candidate_id"] for item in detail["deployments"]
    ]
    if any(type(item) is not str for item in actual_candidate_values):
        raise _error(
            "stored Fleet deployment identity is invalid",
            code="FLEET_ACCEPTANCE_RECORD_INVALID",
            details={"fleet_id": fleet.id},
        )
    actual_candidates = sorted(actual_candidate_values)
    expected_deployments = {
        candidate_id: str(
            uuid.uuid5(
                FLEET_ACCEPTANCE_NAMESPACE,
                f"fleet-deployment:{fleet.id}:{candidate_id}",
            )
        )
        for candidate_id in expected_candidates
    }
    actual_deployments = {
        item["fleet_candidate_id"]: item["id"]
        for item in detail["deployments"]
    }
    expected_bindings = sorted(
        (
            expected_deployments[candidate["candidate_id"]],
            item["node_id"],
            item["gpu_uuid"],
            item["gpu_index"],
            item["rank"],
        )
        for candidate in selected
        for item in _candidate_bindings(candidate)
    )
    actual_bindings = sorted(
        (
            item["deployment_id"],
            item["node_id"],
            item["gpu_uuid"],
            item["gpu_index"],
            item["rank"],
        )
        for item in detail["reservations"]
        if item["released_at"] is None
    )
    if (
        actual_candidates != expected_candidates
        or actual_deployments != expected_deployments
        or actual_bindings != expected_bindings
    ):
        raise _error(
            "stored Fleet acceptance is incomplete or inconsistent",
            code="FLEET_ACCEPTANCE_RECORD_INVALID",
            details={"fleet_id": fleet.id},
        )
    runtime_rows = list(
        session.scalars(
            select(FleetDeploymentRuntime)
            .where(FleetDeploymentRuntime.fleet_id == fleet.id)
            .order_by(FleetDeploymentRuntime.deployment_id)
        )
    )
    if (
        [row.deployment_id for row in runtime_rows]
        != sorted(expected_deployments.values())
        or any(
            row.status not in {
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
            for row in runtime_rows
        )
    ):
        raise _error(
            "stored Fleet runtime is incomplete or inconsistent",
            code="FLEET_ACCEPTANCE_RECORD_INVALID",
            details={"fleet_id": fleet.id},
        )
    return detail


def _recover_integrity_conflict(
    session: Session,
    *,
    recommendation: FleetRecommendationRecord,
    error: IntegrityError,
) -> dict[str, Any]:
    session.rollback()
    existing = session.scalar(
        select(FleetRecord).where(
            FleetRecord.source_recommendation_id == recommendation.id
        )
    )
    if existing is not None:
        return {
            "fleet": _validated_existing_fleet(
                session,
                fleet=existing,
                recommendation=recommendation,
            ),
            "created": False,
        }
    raise _error(
        "Fleet resources could not be reserved atomically",
        code="FLEET_ACCEPTANCE_CONFLICT",
        details={"recommendation_id": recommendation.id},
    ) from error


def accept_fleet_recommendation(
    session: Session,
    recommendation_id: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    # This must be the first write-side lock in the acceptance transaction.
    # Every competing resource owner follows gate -> domain rows.
    lock_fleet_reservation_gate(session)
    recommendation = session.scalar(
        select(FleetRecommendationRecord)
        .where(FleetRecommendationRecord.id == recommendation_id)
        .with_for_update()
    )
    if recommendation is None:
        raise FleetRecommendationNotFoundError(recommendation_id)
    validate_stored_fleet_recommendation(recommendation)
    existing = session.scalar(
        select(FleetRecord)
        .where(FleetRecord.source_recommendation_id == recommendation_id)
        .with_for_update()
    )
    if existing is not None:
        return {
            "fleet": _validated_existing_fleet(
                session,
                fleet=existing,
                recommendation=recommendation,
            ),
            "created": False,
        }

    _lock_fleet_inputs(session, recommendation)
    existing = session.scalar(
        select(FleetRecord)
        .where(FleetRecord.source_recommendation_id == recommendation_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if existing is not None:
        return {
            "fleet": _validated_existing_fleet(
                session,
                fleet=existing,
                recommendation=recommendation,
            ),
            "created": False,
        }
    try:
        current = evaluate_fleet_recommendation(
            session,
            node_ids=(
                list(recommendation.requested_node_ids)
                if recommendation.selection_mode == "explicit_nodes"
                else []
            ),
            all_online=recommendation.selection_mode == "all_online",
            objective=recommendation.objective,
            minimum_replicas=dict(recommendation.minimum_replicas),
            minimum_reserve_nodes=recommendation.minimum_reserve_nodes,
            reserve_node_ids=list(recommendation.reserve_node_ids),
            now=now,
        )
    except (
        FleetEvaluationError,
        FleetRecommendationError,
        FleetSchedulingError,
        RecommendationError,
    ) as exc:
        raise _error(
            "Fleet recommendation inputs are no longer eligible",
            code="FLEET_RECOMMENDATION_STALE",
            details={"reason": getattr(exc, "code", type(exc).__name__)},
        ) from exc
    if current != recommendation.recommendation_snapshot:
        expected_snapshot = recommendation.recommendation_snapshot
        current_evaluation = current.get("evaluation")
        expected_evaluation = expected_snapshot.get("evaluation")
        raise _error(
            "Fleet recommendation no longer matches current inputs",
            code="FLEET_RECOMMENDATION_STALE",
            details={
                "recommendation_id": recommendation_id,
                "expected_inventory_fingerprint": recommendation.inventory_fingerprint,
                "current_inventory_fingerprint": current.get(
                    "inventory_fingerprint"
                ),
                "changed_fields": sorted(
                    key
                    for key in set(current).union(expected_snapshot)
                    if current.get(key) != expected_snapshot.get(key)
                ),
                "changed_evaluation_fields": sorted(
                    key
                    for key in (
                        set(current_evaluation).union(expected_evaluation)
                        if isinstance(current_evaluation, dict)
                        and isinstance(expected_evaluation, dict)
                        else set()
                    )
                    if current_evaluation.get(key)
                    != expected_evaluation.get(key)
                ),
            },
        )

    selected = _selected_candidates(current)
    seen_nodes: set[str] = set()
    seen_gpus: set[str] = set()
    for candidate in selected:
        bindings = _candidate_bindings(candidate)
        for binding in bindings:
            if (
                binding["node_id"] in seen_nodes
                or binding["gpu_uuid"] in seen_gpus
            ):
                raise _error(
                    "one node or GPU appears in multiple Fleet deployments",
                    code="FLEET_RESOURCE_DUPLICATE",
                )
            seen_nodes.add(binding["node_id"])
            seen_gpus.add(binding["gpu_uuid"])
    locked_nodes = list(
        session.scalars(
            select(Node)
            .where(Node.id.in_(sorted(seen_nodes)))
            .order_by(Node.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    if [node.id for node in locked_nodes] != sorted(seen_nodes):
        raise _error(
            "one or more selected Fleet nodes no longer exist",
            code="FLEET_RECOMMENDATION_STALE",
        )
    conflict = session.scalar(
        select(FleetResourceReservation)
        .where(
            FleetResourceReservation.released_at.is_(None),
            or_(
                FleetResourceReservation.node_id.in_(seen_nodes),
                FleetResourceReservation.gpu_uuid.in_(seen_gpus),
            ),
        )
        .order_by(FleetResourceReservation.node_id)
        .with_for_update()
    )
    if conflict is not None:
        raise _error(
            "a selected node or GPU is already reserved",
            code="FLEET_RESOURCE_CONFLICT",
            details={
                "node_id": conflict.node_id,
                "gpu_uuid": conflict.gpu_uuid,
                "fleet_id": conflict.fleet_id,
            },
        )

    fleet_id = str(
        uuid.uuid5(
            FLEET_ACCEPTANCE_NAMESPACE,
            f"fleet:{recommendation_id}",
        )
    )
    planned: list[
        tuple[dict[str, Any], list[dict[str, Any]], str, dict[str, Any]]
    ] = []
    for candidate in selected:
        bindings = _candidate_bindings(candidate)
        deployment_id, plan = _plan_for_candidate(
            session,
            recommendation_id=recommendation_id,
            fleet_id=fleet_id,
            candidate=candidate,
            bindings=bindings,
        )
        planned.append((candidate, bindings, deployment_id, plan))

    fleet = FleetRecord(
        id=fleet_id,
        source_recommendation_id=recommendation_id,
        status="ACCEPTED",
    )
    session.add(fleet)
    try:
        # No ORM relationship links FleetRecord to Deployment, so force the
        # parent row before flushing children that carry the fleet_id FK.
        session.flush([fleet])
    except IntegrityError as exc:
        return _recover_integrity_conflict(
            session,
            recommendation=recommendation,
            error=exc,
        )
    deployments: list[Deployment] = []
    reservations: list[FleetResourceReservation] = []
    for candidate, bindings, deployment_id, plan in planned:
        deployment = Deployment(
            id=deployment_id,
            lineage_id=deployment_id,
            previous_generation_id=None,
            source_recommendation_id=None,
            fleet_id=fleet_id,
            fleet_candidate_id=candidate["candidate_id"],
            generation=1,
            plan=plan,
            accept_model_download=False,
            pull_image=False,
            status="CREATED",
        )
        deployments.append(deployment)
        session.add(deployment)
    try:
        session.flush(deployments)
    except IntegrityError as exc:
        return _recover_integrity_conflict(
            session,
            recommendation=recommendation,
            error=exc,
        )
    for deployment in deployments:
        session.add(
            FleetDeploymentRuntime(
                id=str(
                    uuid.uuid5(
                        FLEET_ACCEPTANCE_NAMESPACE,
                        f"fleet-runtime:{fleet_id}:{deployment.id}",
                    )
                ),
                fleet_id=fleet_id,
                deployment_id=deployment.id,
                status="ACCEPTED",
            )
        )
    for _, bindings, deployment_id, _ in planned:
        for binding in bindings:
            reservation = FleetResourceReservation(
                id=str(
                    uuid.uuid5(
                        FLEET_ACCEPTANCE_NAMESPACE,
                        "fleet-reservation:"
                        f"{fleet_id}:{deployment_id}:"
                        f"{binding['node_id']}:{binding['gpu_uuid']}",
                    )
                ),
                fleet_id=fleet_id,
                deployment_id=deployment_id,
                node_id=binding["node_id"],
                gpu_index=binding["gpu_index"],
                gpu_uuid=binding["gpu_uuid"],
                rank=binding["rank"],
            )
            reservations.append(reservation)
            session.add(reservation)

    try:
        session.flush()
        session.add(
            AuditEvent(
                actor="admin",
                action="fleet.accept",
                target=fleet_id,
                outcome="success",
                detail={
                    "recommendation_id": recommendation_id,
                    "deployment_ids": sorted(item.id for item in deployments),
                    "node_ids": sorted(seen_nodes),
                    "gpu_uuids": sorted(seen_gpus),
                },
            )
        )
        session.commit()
    except IntegrityError as exc:
        return _recover_integrity_conflict(
            session,
            recommendation=recommendation,
            error=exc,
        )
    return {"fleet": fleet_detail(session, fleet), "created": True}


def show_fleet(session: Session, fleet_id: str) -> dict[str, Any]:
    fleet = session.get(FleetRecord, fleet_id)
    if fleet is None:
        raise FleetNotFoundError(fleet_id)
    recommendation = session.get(
        FleetRecommendationRecord, fleet.source_recommendation_id
    )
    if recommendation is None:
        raise _error(
            "Fleet source recommendation is missing",
            code="FLEET_ACCEPTANCE_RECORD_INVALID",
            details={"fleet_id": fleet_id},
        )
    validate_stored_fleet_recommendation(recommendation)
    return {
        "fleet": _validated_existing_fleet(
            session,
            fleet=fleet,
            recommendation=recommendation,
        )
    }
