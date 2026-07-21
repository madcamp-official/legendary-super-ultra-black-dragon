from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Mapping

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dure.resource_pool import FLEET_MODEL_IDS

from .fleet import evaluate_fleet_schedule
from .models import FleetRecommendationRecord
from .service import aware


FLEET_RECOMMENDATION_SCHEMA_VERSION = 1


class FleetRecommendationError(ValueError):
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


class FleetRecommendationNotFoundError(FleetRecommendationError):
    def __init__(self, recommendation_id: str) -> None:
        super().__init__(
            "Fleet recommendation not found",
            code="FLEET_RECOMMENDATION_NOT_FOUND",
            details={"recommendation_id": recommendation_id},
        )


class FleetRecommendationConflictError(FleetRecommendationError):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            code="FLEET_RECOMMENDATION_SNAPSHOT_CONFLICT",
        )


class FleetRecommendationIntegrityError(FleetRecommendationError):
    def __init__(self, message: str = "stored Fleet recommendation is invalid") -> None:
        super().__init__(
            message,
            code="FLEET_RECOMMENDATION_RECORD_INVALID",
        )


def _content_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _normalized_minimum_replicas(
    value: Mapping[str, int] | None,
) -> dict[str, int]:
    normalized: dict[str, int] = {}
    for model_id, count in (value or {}).items():
        if model_id not in FLEET_MODEL_IDS:
            raise FleetRecommendationError(
                f"model is outside the Fleet allowlist: {model_id}",
                code="FLEET_RECOMMENDATION_POLICY_INVALID",
            )
        if type(count) is not int or count < 0:
            raise FleetRecommendationError(
                "minimum replica counts must be non-negative integers",
                code="FLEET_RECOMMENDATION_POLICY_INVALID",
            )
        if count:
            normalized[model_id] = count
    return {model_id: normalized[model_id] for model_id in sorted(normalized)}


def _normalized_node_ids(values: list[str], *, field: str) -> list[str]:
    if any(type(value) is not str or not value for value in values):
        raise FleetRecommendationError(
            f"{field} must contain non-empty node IDs",
            code="FLEET_RECOMMENDATION_POLICY_INVALID",
        )
    if len(values) != len(set(values)):
        raise FleetRecommendationError(
            f"{field} must not contain duplicates",
            code="FLEET_RECOMMENDATION_POLICY_INVALID",
        )
    return sorted(values)


def _snapshot_core(
    *,
    evaluation: dict[str, Any],
    selection_mode: str,
    requested_node_ids: list[str],
    minimum_replicas: dict[str, int],
    minimum_reserve_nodes: int,
    reserve_node_ids: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": FLEET_RECOMMENDATION_SCHEMA_VERSION,
        "objective": evaluation["objective"],
        "selection_mode": selection_mode,
        "requested_node_ids": requested_node_ids,
        "minimum_replicas": minimum_replicas,
        "minimum_reserve_nodes": minimum_reserve_nodes,
        "reserve_node_ids": reserve_node_ids,
        "inventory_fingerprint": evaluation["inventory_fingerprint"],
        "source_inventory_fingerprint": evaluation[
            "source_inventory_fingerprint"
        ],
        "catalog_version": evaluation["catalog_version"],
        "catalog_policy_version": evaluation["catalog_policy_version"],
        "candidate_policy_version": evaluation["candidate_policy_version"],
        "scheduler_version": evaluation["scheduler_version"],
        "evaluation": evaluation,
    }


def evaluate_fleet_recommendation(
    session: Session,
    *,
    node_ids: list[str],
    all_online: bool,
    objective: str = "quality-first",
    minimum_replicas: Mapping[str, int] | None = None,
    minimum_reserve_nodes: int = 0,
    reserve_node_ids: list[str] | tuple[str, ...] = (),
    now: datetime | None = None,
) -> dict[str, Any]:
    """Evaluate a Fleet and return a content-addressed immutable snapshot."""

    normalized_nodes = _normalized_node_ids(node_ids, field="node_ids")
    if bool(normalized_nodes) == all_online:
        raise FleetRecommendationError(
            "choose exactly one of node_ids or all_online",
            code="FLEET_RECOMMENDATION_POLICY_INVALID",
        )
    normalized_minimums = _normalized_minimum_replicas(minimum_replicas)
    normalized_reserves = _normalized_node_ids(
        list(reserve_node_ids),
        field="reserve_node_ids",
    )
    if type(minimum_reserve_nodes) is not int or minimum_reserve_nodes < 0:
        raise FleetRecommendationError(
            "minimum_reserve_nodes must be a non-negative integer",
            code="FLEET_RECOMMENDATION_POLICY_INVALID",
        )
    if normalized_nodes and not set(normalized_reserves).issubset(normalized_nodes):
        raise FleetRecommendationError(
            "reserve_node_ids must be a subset of explicit node_ids",
            code="FLEET_RECOMMENDATION_POLICY_INVALID",
        )

    evaluation = evaluate_fleet_schedule(
        session,
        node_ids=normalized_nodes,
        all_online=all_online,
        objective=objective,
        minimum_replicas=normalized_minimums,
        minimum_reserve_nodes=minimum_reserve_nodes,
        reserve_node_ids=tuple(normalized_reserves),
        now=now,
    )
    observed_node_ids = sorted(
        item["node_id"] for item in evaluation["inventory_snapshot"]
    )
    core = _snapshot_core(
        evaluation=evaluation,
        selection_mode="all_online" if all_online else "explicit_nodes",
        requested_node_ids=(
            observed_node_ids if all_online else normalized_nodes
        ),
        minimum_replicas=normalized_minimums,
        minimum_reserve_nodes=minimum_reserve_nodes,
        reserve_node_ids=normalized_reserves,
    )
    return {"id": _content_digest(core), **core}


def _record_values(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": snapshot["id"],
        "schema_version": snapshot["schema_version"],
        "objective": snapshot["objective"],
        "selection_mode": snapshot["selection_mode"],
        "requested_node_ids": snapshot["requested_node_ids"],
        "minimum_replicas": snapshot["minimum_replicas"],
        "minimum_reserve_nodes": snapshot["minimum_reserve_nodes"],
        "reserve_node_ids": snapshot["reserve_node_ids"],
        "inventory_fingerprint": snapshot["inventory_fingerprint"],
        "source_inventory_fingerprint": snapshot[
            "source_inventory_fingerprint"
        ],
        "catalog_version": snapshot["catalog_version"],
        "catalog_policy_version": snapshot["catalog_policy_version"],
        "candidate_policy_version": snapshot["candidate_policy_version"],
        "scheduler_version": snapshot["scheduler_version"],
        "recommendation_snapshot": snapshot,
    }


def _record_matches(
    record: FleetRecommendationRecord,
    snapshot: dict[str, Any],
) -> bool:
    try:
        return all(
            getattr(record, key) == value
            for key, value in _record_values(snapshot).items()
        )
    except (AttributeError, KeyError, TypeError):
        return False


def validate_stored_fleet_recommendation(
    record: FleetRecommendationRecord,
) -> None:
    snapshot = record.recommendation_snapshot
    if not isinstance(snapshot, dict) or snapshot.get("id") != record.id:
        raise FleetRecommendationIntegrityError()
    core = dict(snapshot)
    core.pop("id", None)
    if _content_digest(core) != record.id or not _record_matches(record, snapshot):
        raise FleetRecommendationIntegrityError(
            "stored Fleet recommendation integrity check failed"
        )


def persist_fleet_recommendation(
    session: Session,
    snapshot: dict[str, Any],
) -> tuple[FleetRecommendationRecord, bool]:
    values = _record_values(snapshot)
    existing = session.get(FleetRecommendationRecord, values["id"])
    if existing is not None:
        if not _record_matches(existing, snapshot):
            raise FleetRecommendationConflictError(
                "Fleet recommendation content ID is already bound to different content"
            )
        validate_stored_fleet_recommendation(existing)
        return existing, False

    record = FleetRecommendationRecord(**values)
    session.add(record)
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        existing = session.get(FleetRecommendationRecord, values["id"])
        if existing is None or not _record_matches(existing, snapshot):
            raise FleetRecommendationConflictError(
                "Fleet recommendation snapshot could not be persisted"
            ) from exc
        validate_stored_fleet_recommendation(existing)
        return existing, False
    return record, True


def recommend_fleet(
    session: Session,
    **kwargs: Any,
) -> dict[str, Any]:
    snapshot = evaluate_fleet_recommendation(session, **kwargs)
    record, created = persist_fleet_recommendation(session, snapshot)
    return {
        "recommendation": record.recommendation_snapshot,
        "recorded_at": aware(record.created_at).isoformat(),
        "created": created,
    }


def show_fleet_recommendation(
    session: Session,
    recommendation_id: str,
) -> dict[str, Any]:
    record = session.get(FleetRecommendationRecord, recommendation_id)
    if record is None:
        raise FleetRecommendationNotFoundError(recommendation_id)
    validate_stored_fleet_recommendation(record)
    return {
        "recommendation": record.recommendation_snapshot,
        "recorded_at": aware(record.created_at).isoformat(),
    }
