from __future__ import annotations

import hashlib
import json
import math
from dataclasses import replace
from datetime import datetime
from typing import Any, Mapping

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from dure.catalog import ModelCatalog
from dure.fleet_scheduler import (
    DEFAULT_MAX_CANDIDATES,
    DEFAULT_MAX_SEARCH_STATES,
    FleetDeploymentCandidate,
    FleetGpuBinding,
    schedule_fleet,
)
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
)
from dure.resource_pool import FLEET_MODEL_IDS, build_gpu_pool_snapshot
from dure.selector import InventoryNode, recommend_model

from .models import (
    Deployment,
    DeploymentOperation,
    FleetResourceReservation,
    Node,
    ProfileQualificationEvidence,
    ProfileQualificationRun,
    Task,
    TaskStatus,
    utcnow,
)
from .recommendation import (
    _FULL_SNAPSHOT_AGENT_VERSION,
    _active_catalog,
    _agent_supports,
    _build_generation_plan,
    _inventory_nodes,
    _strict_candidate_rejections,
    canonical_inventory_snapshot,
)


FLEET_CANDIDATE_POLICY_VERSION = "fleet-candidate-v2"
FLEET_SCHEDULER_VERSION = "fleet-set-packing-v1"


class FleetEvaluationError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "FLEET_EVALUATION_FAILED",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _plan_contract_digest(plan: dict[str, Any]) -> str:
    """Bind every runtime field except the acceptance-assigned generation ID."""

    if not isinstance(plan, dict):
        raise ValueError("Fleet generation plan must be an object")
    contract = dict(plan)
    contract.pop("deployment_id", None)
    contract.pop("generation", None)
    return _digest(contract)


def _fleet_occupancy(
    session: Session,
    inventory: list[InventoryNode],
) -> dict[str, str]:
    requested = {item.node_id for item in inventory}
    reasons: dict[str, str] = {}
    gpu_nodes: dict[str, set[str]] = {}
    for item in inventory:
        if item.profile is None:
            continue
        for gpu in item.profile.gpus:
            gpu_nodes.setdefault(gpu.uuid, set()).add(item.node_id)
    reservation_filter = []
    if requested:
        reservation_filter.append(
            FleetResourceReservation.node_id.in_(requested)
        )
    if gpu_nodes:
        reservation_filter.append(
            FleetResourceReservation.gpu_uuid.in_(gpu_nodes)
        )
    if reservation_filter:
        for reservation in session.scalars(
            select(FleetResourceReservation)
            .where(
                FleetResourceReservation.released_at.is_(None),
                or_(*reservation_filter),
            )
            .order_by(FleetResourceReservation.node_id)
        ):
            affected = set(gpu_nodes.get(reservation.gpu_uuid, set()))
            if reservation.node_id in requested:
                affected.add(reservation.node_id)
            for node_id in sorted(affected):
                reasons.setdefault(
                    node_id,
                    "ACTIVE_FLEET_RESERVATION:"
                    f"{reservation.fleet_id}:{reservation.gpu_uuid}",
                )
    for run in session.scalars(
        select(ProfileQualificationRun)
        .where(ProfileQualificationRun.status == "QUALIFYING")
        .order_by(ProfileQualificationRun.id)
    ):
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
                    node_id,
                    f"ACTIVE_DEPLOYMENT_OPERATION:{operation.id}",
                )
    observed = {
        node.id: node.observed_deployment_id
        for node in session.scalars(
            select(Node).where(Node.id.in_(requested)).order_by(Node.id)
        )
        if node.observed_deployment_id
    }
    for node_id, deployment_id in observed.items():
        reasons.setdefault(
            node_id,
            f"OBSERVED_DEPLOYMENT:{deployment_id}",
        )
    for item in inventory:
        if item.profile is not None and item.profile.workloads:
            reasons.setdefault(item.node_id, "OBSERVED_RUNNING_WORKLOAD")
    return reasons


def _cache_hit(
    node: InventoryNode,
    *,
    context: dict[str, Any],
    rank: int,
) -> bool:
    if node.profile is None:
        return False
    cache_kind = context["model_cache_kind"]
    for marker in node.profile.installed_models:
        if not marker.complete or marker.cache_kind != cache_kind:
            continue
        if cache_kind == MODEL_CACHE_KIND_FULL_SNAPSHOT:
            if (
                marker.model_id
                not in {context["model_id"], context["artifact_repository"]}
                or marker.revision != context["artifact_revision"]
                or marker.quantization != context["quantization"]
                or marker.manifest_digest
                != context["artifact_manifest_digest"]
            ):
                continue
            return True
        if cache_kind == MODEL_CACHE_KIND_STAGE and (
            marker.artifact_set_digest
            == context["stage_artifact"]["artifact_set_digest"]
            and marker.source_manifest_digest
            == context["artifact_manifest_digest"]
            and marker.runtime_image == context["runtime_image"]
            and marker.pipeline_rank == rank
            and marker.tensor_rank == 0
        ):
            return True
    return False


def _candidate_identity(
    *,
    catalog_candidate_id: str,
    evidence_id: str,
    evidence_digest: str,
    context: dict[str, Any],
    bindings: list[dict[str, Any]],
    network_zone: str | None,
    zone_penalty: float,
) -> str:
    delivery_identity = {
        key: context.get(key)
        for key in (
            "model_cache_kind",
            "artifact_manifest_digest",
            "runtime_image",
            "full_snapshot_total_size_bytes",
            "full_snapshot_required_cache_bytes",
            "stage_artifact",
            "stage_ranks",
        )
    }
    return _digest(
        {
            "policy_version": FLEET_CANDIDATE_POLICY_VERSION,
            "catalog_candidate_id": catalog_candidate_id,
            "evidence_id": evidence_id,
            "evidence_digest": evidence_digest,
            "delivery_identity": delivery_identity,
            "bindings": bindings,
            "network_locality": {
                "network_zone": network_zone,
                "zone_penalty": zone_penalty,
            },
        }
    )


def _project_candidates(
    session: Session,
    *,
    inventory: list[InventoryNode],
    available_node_ids: set[str],
    catalog,
    contexts: dict[str, dict[str, Any]],
    network_zones: Mapping[str, str],
) -> tuple[
    list[FleetDeploymentCandidate],
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
]:
    available_inventory = [
        item for item in inventory if item.node_id in available_node_ids
    ]
    inventory_by_id = {item.node_id: item for item in available_inventory}
    candidates: list[FleetDeploymentCandidate] = []
    details: dict[str, dict[str, Any]] = {}
    rejections: list[dict[str, Any]] = []

    for entry in sorted(
        catalog.entries,
        key=lambda item: (item.candidate_id or "", item.placement.profile_id),
    ):
        context = contexts[entry.candidate_id]
        if entry.model.model_id not in FLEET_MODEL_IDS:
            rejections.append(
                {
                    "catalog_candidate_id": entry.candidate_id,
                    "code": "MODEL_NOT_ALLOWED",
                    "node_ids": [],
                }
            )
            continue
        if entry.placement.tensor_parallel_size != 1:
            rejections.append(
                {
                    "catalog_candidate_id": entry.candidate_id,
                    "code": "TP_UNSUPPORTED",
                    "node_ids": [],
                }
            )
            continue
        if not entry.placement.requires_qualification_evidence:
            rejections.append(
                {
                    "catalog_candidate_id": entry.candidate_id,
                    "code": "AUTO_QUALIFICATION_REQUIRED",
                    "node_ids": [],
                }
            )
            continue
        if not entry.network_evidence:
            rejections.append(
                {
                    "catalog_candidate_id": entry.candidate_id,
                    "code": "QUALIFICATION_EVIDENCE_MISSING",
                    "node_ids": [],
                }
            )
            continue

        for binding in entry.network_evidence:
            run = session.scalar(
                select(ProfileQualificationRun)
                .join(
                    ProfileQualificationEvidence,
                    ProfileQualificationEvidence.run_id
                    == ProfileQualificationRun.id,
                )
                .where(
                    ProfileQualificationEvidence.id == binding.evidence_id,
                    ProfileQualificationRun.evidence_id == binding.evidence_id,
                )
            )
            if run is None:
                rejections.append(
                    {
                        "catalog_candidate_id": entry.candidate_id,
                        "evidence_id": binding.evidence_id,
                        "code": "QUALIFICATION_EVIDENCE_INVALID",
                        "node_ids": list(binding.node_ids),
                    }
                )
                continue
            exact_catalog = ModelCatalog(
                version=catalog.version,
                policy_version=catalog.policy_version,
                entries=(replace(entry, network_evidence=(binding,)),),
            )
            evaluation = recommend_model(
                available_inventory,
                catalog=exact_catalog,
            ).evaluations[0]
            if (
                not evaluation.feasible
                or evaluation.network_evidence_id != binding.evidence_id
                or set(evaluation.node_ids) != set(binding.node_ids)
            ):
                rejections.append(
                    {
                        "catalog_candidate_id": entry.candidate_id,
                        "evidence_id": binding.evidence_id,
                        "code": "STATIC_CANDIDATE_INELIGIBLE",
                        "node_ids": list(binding.node_ids),
                        "rejections": [
                            item.to_dict() for item in evaluation.rejections
                        ],
                    }
                )
                continue
            candidate_node_ids = list(evaluation.node_ids)
            if context.get("execution_backend") is not None:
                strict_rejections, runtime_rank_node_ids = (
                    _strict_candidate_rejections(
                        context=context,
                        entry=entry,
                        node_ids=sorted(candidate_node_ids),
                        inventory_by_id=inventory_by_id,
                    )
                )
                if strict_rejections or tuple(runtime_rank_node_ids) != tuple(
                    run.rank_node_ids
                ):
                    rejections.append(
                        {
                            "catalog_candidate_id": entry.candidate_id,
                            "evidence_id": binding.evidence_id,
                            "code": "STRICT_RUNTIME_INELIGIBLE",
                            "node_ids": sorted(candidate_node_ids),
                            "rejections": strict_rejections,
                        }
                    )
                    continue
            elif (
                len(candidate_node_ids) != 1
                or not _agent_supports(
                    inventory_by_id[candidate_node_ids[0]].agent_version,
                    _FULL_SNAPSHOT_AGENT_VERSION,
                )
            ):
                rejections.append(
                    {
                        "catalog_candidate_id": entry.candidate_id,
                        "evidence_id": binding.evidence_id,
                        "code": "SINGLE_NODE_RUNTIME_INELIGIBLE",
                        "node_ids": sorted(candidate_node_ids),
                    }
                )
                continue

            gpu_bindings = sorted(
                run.gpu_bindings,
                key=lambda item: item["rank"],
            )
            if (
                len(gpu_bindings) != entry.placement.node_count
                or [item["rank"] for item in gpu_bindings]
                != list(range(entry.placement.node_count))
                or tuple(item["node_id"] for item in gpu_bindings)
                != tuple(run.rank_node_ids)
            ):
                rejections.append(
                    {
                        "catalog_candidate_id": entry.candidate_id,
                        "evidence_id": binding.evidence_id,
                        "code": "QUALIFICATION_BINDING_INVALID",
                        "node_ids": sorted(candidate_node_ids),
                    }
                )
                continue
            projected_bindings = []
            slot_matches = True
            for item in gpu_bindings:
                node = inventory_by_id.get(item["node_id"])
                if node is None:
                    slot_matches = False
                    break
                healthy_matches = [
                    gpu
                    for gpu in node.profile.gpus
                    if gpu.healthy
                    and gpu.index == item["gpu_index"]
                    and gpu.uuid == item["gpu_uuid"]
                ]
                if len(healthy_matches) != 1:
                    slot_matches = False
                    break
                projected_bindings.append(dict(item))
            if not slot_matches:
                rejections.append(
                    {
                        "catalog_candidate_id": entry.candidate_id,
                        "evidence_id": binding.evidence_id,
                        "code": "QUALIFICATION_GPU_CHANGED",
                        "node_ids": sorted(candidate_node_ids),
                    }
                )
                continue

            stage_node_bindings: list[dict[str, Any]] | None = None
            if context.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE:
                stage_ranks = context.get("stage_ranks")
                if (
                    not isinstance(stage_ranks, list)
                    or len(stage_ranks) != len(run.rank_node_ids)
                    or len(stage_ranks) != len(projected_bindings)
                    or any(not isinstance(item, dict) for item in stage_ranks)
                ):
                    rejections.append(
                        {
                            "catalog_candidate_id": entry.candidate_id,
                            "evidence_id": binding.evidence_id,
                            "code": "STAGE_RANK_BINDING_INVALID",
                            "node_ids": sorted(candidate_node_ids),
                        }
                    )
                    continue
                stage_node_bindings = [
                    {
                        "node_id": run.rank_node_ids[rank],
                        **stage_ranks[rank],
                    }
                    for rank in range(len(stage_ranks))
                ]

            evidence = session.get(
                ProfileQualificationEvidence,
                binding.evidence_id,
            )
            if evidence is None:
                continue
            throughput = evidence.metrics.get("throughput_tps")
            if type(throughput) not in {int, float} or not math.isfinite(
                throughput
            ):
                continue
            memory_values = [item["memory_mib"] for item in projected_bindings]
            maximum_memory = max(memory_values)
            imbalance = (
                (maximum_memory - min(memory_values)) / maximum_memory
                if maximum_memory
                else 0.0
            )
            cache_hits = sum(
                _cache_hit(
                    inventory_by_id[item["node_id"]],
                    context=context,
                    rank=item["rank"],
                )
                for item in projected_bindings
            )
            candidate_zones = {
                network_zones[item["node_id"]]
                for item in projected_bindings
                if item["node_id"] in network_zones
            }
            zones_complete = len(candidate_zones) > 0 and all(
                item["node_id"] in network_zones
                for item in projected_bindings
            )
            network_zone = (
                next(iter(candidate_zones))
                if zones_complete and len(candidate_zones) == 1
                else None
            )
            zone_penalty = (
                float(len(candidate_zones) - 1)
                if zones_complete
                else 0.0
            )
            candidate_id = _candidate_identity(
                catalog_candidate_id=entry.candidate_id,
                evidence_id=binding.evidence_id,
                evidence_digest=binding.evidence_digest,
                context=context,
                bindings=projected_bindings,
                network_zone=network_zone,
                zone_penalty=zone_penalty,
            )
            scheduler_candidate = FleetDeploymentCandidate(
                candidate_id=candidate_id,
                model_id=entry.model.model_id,
                placement_profile_id=context["placement_id"],
                evidence_id=binding.evidence_id,
                evidence_digest=binding.evidence_digest,
                bindings=tuple(
                    FleetGpuBinding(
                        node_id=item["node_id"],
                        gpu_index=item["gpu_index"],
                        gpu_uuid=item["gpu_uuid"],
                        rank=item["rank"],
                    )
                    for item in projected_bindings
                ),
                tensor_parallel_size=entry.placement.tensor_parallel_size,
                pipeline_parallel_size=entry.placement.pipeline_parallel_size,
                quality_score=float(entry.quality_rank),
                throughput_tps=float(throughput),
                cache_hit_count=cache_hits,
                network_zone=network_zone,
                zone_penalty=zone_penalty,
                imbalance_score=imbalance,
            )
            candidates.append(scheduler_candidate)
            details[candidate_id] = {
                **scheduler_candidate.to_dict(),
                **context,
                "candidate_id": candidate_id,
                "catalog_candidate_id": entry.candidate_id,
                "placement_profile_id": entry.placement.profile_id,
                "qualification_purpose": run.workload.get(
                    "qualification_purpose", "PRIMARY"
                ),
                "rank_node_ids": list(run.rank_node_ids),
                "gpu_bindings": projected_bindings,
                "qualification_registered_at": binding.registered_at,
            }
            if stage_node_bindings is not None:
                details[candidate_id]["stage_node_bindings"] = (
                    stage_node_bindings
                )
    return (
        sorted(candidates, key=lambda item: item.candidate_id),
        details,
        sorted(
            rejections,
            key=lambda item: (
                item.get("catalog_candidate_id", ""),
                item.get("evidence_id", ""),
                item["code"],
            ),
        ),
    )


def evaluate_fleet_schedule(
    session: Session,
    *,
    node_ids: list[str],
    all_online: bool,
    objective: str = "quality-first",
    minimum_replicas: Mapping[str, int] | None = None,
    minimum_reserve_nodes: int = 0,
    reserve_node_ids: tuple[str, ...] = (),
    network_zones: Mapping[str, str] | None = None,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    max_search_states: int = DEFAULT_MAX_SEARCH_STATES,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build and schedule exact, validated candidates without writing state."""

    if objective != "quality-first":
        raise ValueError("unsupported Fleet objective")
    zone_by_node = dict(network_zones or {})
    if any(
        type(node_id) is not str
        or not node_id
        or type(zone) is not str
        or not zone
        for node_id, zone in zone_by_node.items()
    ):
        raise FleetEvaluationError(
            "network zones must map non-empty node IDs to non-empty zone IDs",
            code="FLEET_NETWORK_ZONE_INVALID",
        )
    evaluated_at = now or utcnow()
    with session.no_autoflush:
        inventory = _inventory_nodes(
            session,
            node_ids=node_ids,
            all_online=all_online,
            now=evaluated_at,
        )
        occupancy = _fleet_occupancy(session, inventory)
        pool = build_gpu_pool_snapshot(
            inventory,
            occupied_node_ids=occupancy,
            occupancy_reasons=occupancy,
            network_zones=zone_by_node,
        )
        available_node_ids = {
            slot.node_id for slot in pool.selected_slots
        }
        unavailable_reserves = sorted(
            set(reserve_node_ids).difference(available_node_ids)
        )
        if unavailable_reserves:
            raise FleetEvaluationError(
                "reserved Fleet nodes are not currently available",
                code="FLEET_RESERVE_NODE_UNAVAILABLE",
                details={"node_ids": unavailable_reserves},
            )
        catalog, contexts = _active_catalog(
            session,
            inventory=inventory,
            now=evaluated_at,
        )
        candidates, candidate_details, projection_rejections = (
            _project_candidates(
                session,
                inventory=inventory,
                available_node_ids=available_node_ids,
                catalog=catalog,
                contexts=contexts,
                network_zones=zone_by_node,
            )
        )
        schedule = schedule_fleet(
            candidates,
            minimum_replicas=minimum_replicas,
            available_node_ids=available_node_ids,
            minimum_reserve_nodes=minimum_reserve_nodes,
            reserve_node_ids=reserve_node_ids,
            max_candidates=max_candidates,
            max_search_states=max_search_states,
        )

    # Freeze the complete runtime plan contract in the content-addressed
    # recommendation. Deployment ID and generation are assigned only when the
    # operator accepts the recommendation, so they are excluded from this
    # digest and validated separately by the acceptance service.
    for scheduled in schedule.selected:
        detail = candidate_details[scheduled.candidate_id]
        selected = dict(detail)
        selected.update(
            {
                "feasible": True,
                "node_ids": sorted(item["node_id"] for item in detail["bindings"]),
                "rejections": [],
            }
        )
        contract_plan = _build_generation_plan(
            session,
            recommendation={
                "id": "fleet-plan-contract",
                "selected": selected,
            },
            deployment_id="00000000-0000-4000-8000-000000000000",
            generation=1,
        )
        detail["plan_contract_digest"] = _plan_contract_digest(contract_plan)

    selected_ids = {item.candidate_id for item in schedule.selected}
    selected_nodes = set(schedule.used_node_ids)
    candidates_by_node: dict[str, list[str]] = {}
    for candidate in candidates:
        for node_id in candidate.node_ids:
            candidates_by_node.setdefault(node_id, []).append(
                candidate.candidate_id
            )
    projection_codes_by_node: dict[str, set[str]] = {}
    for rejection in projection_rejections:
        for node_id in rejection.get("node_ids", []):
            projection_codes_by_node.setdefault(node_id, set()).add(
                rejection["code"]
            )

    unassigned = []
    for node in pool.nodes:
        if node.node_id in selected_nodes:
            continue
        if node.selected_gpu is None:
            reason = node.unavailable_reason
        elif node.node_id not in candidates_by_node:
            reason = "NO_VALIDATED_CANDIDATE"
        else:
            reason = "OBJECTIVE_NOT_SELECTED"
        unassigned.append(
            {
                "node_id": node.node_id,
                "reason": reason,
                "occupancy_reason": node.occupancy_reason,
                "candidate_ids": sorted(
                    candidates_by_node.get(node.node_id, [])
                ),
                "candidate_rejection_codes": sorted(
                    projection_codes_by_node.get(node.node_id, set())
                ),
            }
        )

    pool_snapshot = pool.to_dict()
    inventory_snapshot = canonical_inventory_snapshot(inventory)
    fleet_inventory_fingerprint = _digest(
        {
            "inventory": inventory_snapshot,
            "gpu_pool": pool_snapshot["nodes"],
        }
    )
    schedule_dict = schedule.to_dict()
    schedule_dict["selected"] = [
        candidate_details[candidate.candidate_id]
        for candidate in schedule.selected
    ]
    return {
        "objective": objective,
        "inventory_fingerprint": fleet_inventory_fingerprint,
        "source_inventory_fingerprint": pool.inventory_fingerprint,
        "inventory_snapshot": inventory_snapshot,
        "gpu_pool_snapshot": pool_snapshot,
        "catalog_version": catalog.version,
        "catalog_policy_version": catalog.policy_version,
        "candidate_policy_version": FLEET_CANDIDATE_POLICY_VERSION,
        "scheduler_version": FLEET_SCHEDULER_VERSION,
        "candidates": [
            candidate_details[candidate.candidate_id]
            for candidate in candidates
        ],
        "projection_rejections": projection_rejections,
        "schedule": schedule_dict,
        "unassigned_nodes": unassigned,
        "limits": {
            "max_candidates": max_candidates,
            "max_search_states": max_search_states,
        },
        "selected_candidate_ids": sorted(selected_ids),
    }
