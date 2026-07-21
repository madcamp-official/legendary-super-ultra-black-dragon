from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DDL,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    event,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base
from ..task import TaskStatus, TaskType


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _deployment_lineage_default(context: Any) -> str:
    """Keep legacy/manual deployments in a one-record lineage by default."""
    return str(context.get_current_parameters()["id"])


def _canonical_uuid_check(column: str = "id") -> str:
    hyphen_positions = {9, 14, 19, 24}
    hexadecimal = ", ".join(
        repr(character) for character in "0123456789abcdef"
    )
    character_checks = " AND ".join(
        f"substr({column}, {position}, 1) IN ({hexadecimal})"
        for position in range(1, 37)
        if position not in hyphen_positions
    )
    return (
        f"length({column}) = 36 AND {column} = lower({column}) "
        f"AND substr({column}, 9, 1) = '-' "
        f"AND substr({column}, 14, 1) = '-' "
        f"AND substr({column}, 19, 1) = '-' "
        f"AND substr({column}, 24, 1) = '-' "
        f"AND {character_checks}"
    )


class Node(Base):
    __tablename__ = "nodes"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    install_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    agent_version: Mapped[str] = mapped_column(String(32), nullable=False)
    approved: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_seen: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_phase: Mapped[str | None] = mapped_column(String(40))
    observed_role: Mapped[str | None] = mapped_column(String(80))
    observed_deployment_id: Mapped[str | None] = mapped_column(String(255))
    desired_state: Mapped[str | None] = mapped_column(String(40))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class NodeProfileRecord(Base):
    __tablename__ = "node_profiles"
    node_id: Mapped[str] = mapped_column(ForeignKey("nodes.id", ondelete="CASCADE"), primary_key=True)
    profile: Mapped[dict] = mapped_column(JSON, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class DeploymentRecommendationRecord(Base):
    __tablename__ = "deployment_recommendations"
    __table_args__ = (
        CheckConstraint(
            "length(id) = 71 AND id LIKE 'sha256:%'",
            name="ck_deployment_recommendation_id_sha256",
        ),
        CheckConstraint(
            "selection_mode IN ('all_online', 'explicit_nodes')",
            name="ck_deployment_recommendation_selection_mode",
        ),
        Index(
            "ix_deployment_recommendations_created_at",
            "created_at",
        ),
    )
    id: Mapped[str] = mapped_column(String(71), primary_key=True)
    objective: Mapped[str] = mapped_column(String(40), nullable=False)
    selection_mode: Mapped[str] = mapped_column(String(20), nullable=False)
    requested_node_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    catalog_version: Mapped[str] = mapped_column(String(71), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    inventory_fingerprint: Mapped[str] = mapped_column(String(71), nullable=False)
    recommendation_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    inventory_snapshot: Mapped[list[dict]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class FleetRecommendationRecord(Base):
    """여러 배포 후보를 한 번에 고정한 콘텐츠 주소 Fleet 추천."""

    __tablename__ = "fleet_recommendations"
    __table_args__ = (
        CheckConstraint(
            "length(id) = 71 AND id LIKE 'sha256:%'",
            name="ck_fleet_recommendation_id_sha256",
        ),
        CheckConstraint(
            "schema_version = 1",
            name="ck_fleet_recommendation_schema_version",
        ),
        CheckConstraint(
            "objective = 'quality-first'",
            name="ck_fleet_recommendation_objective",
        ),
        CheckConstraint(
            "selection_mode IN ('all_online', 'explicit_nodes')",
            name="ck_fleet_recommendation_selection_mode",
        ),
        CheckConstraint(
            "minimum_reserve_nodes >= 0",
            name="ck_fleet_recommendation_reserve_nonnegative",
        ),
        CheckConstraint(
            "length(inventory_fingerprint) = 71 "
            "AND inventory_fingerprint LIKE 'sha256:%'",
            name="ck_fleet_recommendation_inventory_sha256",
        ),
        CheckConstraint(
            "length(source_inventory_fingerprint) = 71 "
            "AND source_inventory_fingerprint LIKE 'sha256:%'",
            name="ck_fleet_recommendation_source_inventory_sha256",
        ),
        CheckConstraint(
            "length(catalog_version) = 71 "
            "AND catalog_version LIKE 'sha256:%'",
            name="ck_fleet_recommendation_catalog_version_sha256",
        ),
        CheckConstraint(
            "length(catalog_policy_version) BETWEEN 1 AND 64",
            name="ck_fleet_recommendation_catalog_policy_version",
        ),
        CheckConstraint(
            "length(candidate_policy_version) BETWEEN 1 AND 64",
            name="ck_fleet_recommendation_candidate_policy_version",
        ),
        CheckConstraint(
            "length(scheduler_version) BETWEEN 1 AND 64",
            name="ck_fleet_recommendation_scheduler_version",
        ),
        Index("ix_fleet_recommendations_created_at", "created_at"),
    )
    id: Mapped[str] = mapped_column(String(71), primary_key=True)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    objective: Mapped[str] = mapped_column(
        String(40), default="quality-first", nullable=False
    )
    selection_mode: Mapped[str] = mapped_column(String(20), nullable=False)
    requested_node_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    minimum_replicas: Mapped[dict[str, int]] = mapped_column(JSON, nullable=False)
    minimum_reserve_nodes: Mapped[int] = mapped_column(Integer, nullable=False)
    reserve_node_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    inventory_fingerprint: Mapped[str] = mapped_column(String(71), nullable=False)
    source_inventory_fingerprint: Mapped[str] = mapped_column(
        String(71), nullable=False
    )
    catalog_version: Mapped[str] = mapped_column(String(71), nullable=False)
    catalog_policy_version: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    candidate_policy_version: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    scheduler_version: Mapped[str] = mapped_column(String(64), nullable=False)
    recommendation_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "schema_version": self.schema_version,
            "objective": self.objective,
            "selection_mode": self.selection_mode,
            "requested_node_ids": list(self.requested_node_ids),
            "minimum_replicas": dict(self.minimum_replicas),
            "minimum_reserve_nodes": self.minimum_reserve_nodes,
            "reserve_node_ids": list(self.reserve_node_ids),
            "inventory_fingerprint": self.inventory_fingerprint,
            "source_inventory_fingerprint": self.source_inventory_fingerprint,
            "catalog_version": self.catalog_version,
            "catalog_policy_version": self.catalog_policy_version,
            "candidate_policy_version": self.candidate_policy_version,
            "scheduler_version": self.scheduler_version,
            "recommendation_snapshot": dict(self.recommendation_snapshot),
            "created_at": (
                self.created_at.isoformat()
                if self.created_at is not None
                else None
            ),
        }


class FleetRecord(Base):
    """수락된 불변 Fleet 추천의 원자적 생성 단위."""

    __tablename__ = "fleets"
    __table_args__ = (
        CheckConstraint(
            _canonical_uuid_check(),
            name="ck_fleets_id_canonical_uuid",
        ),
        CheckConstraint(
            "status IN ('ACCEPTED', 'PREPARING', 'PREPARED', 'APPLYING', "
            "'VERIFYING', 'ACTIVE', 'PARTIAL_FAILED', 'FAILED')",
            name="ck_fleets_status",
        ),
        UniqueConstraint(
            "source_recommendation_id",
            name="uq_fleets_source_recommendation_id",
        ),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    source_recommendation_id: Mapped[str] = mapped_column(
        ForeignKey(
            "fleet_recommendations.id",
            name="fk_fleets_source_recommendation_id",
        ),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(20), default="ACCEPTED", nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_recommendation_id": self.source_recommendation_id,
            "status": self.status,
            "created_at": (
                self.created_at.isoformat()
                if self.created_at is not None
                else None
            ),
            "updated_at": (
                self.updated_at.isoformat()
                if self.updated_at is not None
                else None
            ),
        }


class Deployment(Base):
    __tablename__ = "deployments"
    __table_args__ = (
        UniqueConstraint(
            "lineage_id",
            "generation",
            name="uq_deployments_lineage_generation",
        ),
        UniqueConstraint(
            "previous_generation_id",
            name="uq_deployments_previous_generation_id",
        ),
        UniqueConstraint(
            "source_recommendation_id",
            name="uq_deployments_source_recommendation_id",
        ),
        UniqueConstraint(
            "fleet_id",
            "fleet_candidate_id",
            name="uq_deployments_fleet_candidate_id",
        ),
        UniqueConstraint(
            "fleet_id",
            "id",
            name="uq_deployments_fleet_ownership",
        ),
        CheckConstraint(
            "(fleet_id IS NULL AND fleet_candidate_id IS NULL) OR "
            "(fleet_id IS NOT NULL AND fleet_candidate_id IS NOT NULL)",
            name="ck_deployments_fleet_binding",
        ),
        CheckConstraint(
            "fleet_candidate_id IS NULL OR "
            "(length(fleet_candidate_id) = 71 "
            "AND fleet_candidate_id LIKE 'sha256:%')",
            name="ck_deployments_fleet_candidate_sha256",
        ),
    )
    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    lineage_id: Mapped[str] = mapped_column(
        String(255), nullable=False, default=_deployment_lineage_default
    )
    previous_generation_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "deployments.id",
            name="fk_deployments_previous_generation_id",
        )
    )
    source_recommendation_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "deployment_recommendations.id",
            name="fk_deployments_source_recommendation_id",
        )
    )
    fleet_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "fleets.id",
            name="fk_deployments_fleet_id",
        )
    )
    fleet_candidate_id: Mapped[str | None] = mapped_column(String(71))
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    plan: Mapped[dict] = mapped_column(JSON, nullable=False)
    accept_model_download: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    pull_image: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    status: Mapped[str] = mapped_column(String(40), default="CREATED", nullable=False)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class FleetResourceReservation(Base):
    """Fleet 수락 트랜잭션에서 고정한 활성 노드·GPU 예약."""

    __tablename__ = "fleet_resource_reservations"
    __table_args__ = (
        CheckConstraint(
            _canonical_uuid_check(),
            name="ck_fleet_resource_reservation_id_canonical_uuid",
        ),
        CheckConstraint(
            "gpu_index >= 0",
            name="ck_fleet_resource_reservation_gpu_index",
        ),
        CheckConstraint(
            "gpu_uuid LIKE 'GPU-%' AND length(gpu_uuid) BETWEEN 5 AND 128",
            name="ck_fleet_resource_reservation_gpu_uuid",
        ),
        CheckConstraint(
            "rank >= 0",
            name="ck_fleet_resource_reservation_rank",
        ),
        UniqueConstraint(
            "fleet_id",
            "node_id",
            name="uq_fleet_resource_reservations_fleet_node",
        ),
        UniqueConstraint(
            "fleet_id",
            "gpu_uuid",
            name="uq_fleet_resource_reservations_fleet_gpu_uuid",
        ),
        UniqueConstraint(
            "fleet_id",
            "deployment_id",
            "rank",
            name="uq_fleet_resource_reservations_fleet_deployment_rank",
        ),
        ForeignKeyConstraint(
            ["fleet_id", "deployment_id"],
            ["deployments.fleet_id", "deployments.id"],
            name="fk_fleet_resource_reservations_fleet_deployment",
        ),
        Index(
            "ux_fleet_resource_reservations_active_node",
            "node_id",
            unique=True,
            sqlite_where=text("released_at IS NULL"),
            postgresql_where=text("released_at IS NULL"),
        ),
        Index(
            "ux_fleet_resource_reservations_active_gpu_uuid",
            "gpu_uuid",
            unique=True,
            sqlite_where=text("released_at IS NULL"),
            postgresql_where=text("released_at IS NULL"),
        ),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    fleet_id: Mapped[str] = mapped_column(
        ForeignKey(
            "fleets.id",
            ondelete="CASCADE",
            name="fk_fleet_resource_reservations_fleet_id",
        ),
        nullable=False,
    )
    deployment_id: Mapped[str] = mapped_column(String(255), nullable=False)
    node_id: Mapped[str] = mapped_column(
        ForeignKey(
            "nodes.id",
            name="fk_fleet_resource_reservations_node_id",
        ),
        nullable=False,
    )
    gpu_index: Mapped[int] = mapped_column(Integer, nullable=False)
    gpu_uuid: Mapped[str] = mapped_column(String(128), nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "fleet_id": self.fleet_id,
            "deployment_id": self.deployment_id,
            "node_id": self.node_id,
            "gpu_index": self.gpu_index,
            "gpu_uuid": self.gpu_uuid,
            "rank": self.rank,
            "released_at": (
                self.released_at.isoformat()
                if self.released_at is not None
                else None
            ),
            "created_at": (
                self.created_at.isoformat()
                if self.created_at is not None
                else None
            ),
        }


class FleetDeploymentRuntime(Base):
    """Fleet 안의 배포 하나에 대한 준비·적용·검증 실행 상태."""

    __tablename__ = "fleet_deployment_runtime"
    __table_args__ = (
        CheckConstraint(
            _canonical_uuid_check(),
            name="ck_fleet_deployment_runtime_id_canonical_uuid",
        ),
        CheckConstraint(
            "status IN ('ACCEPTED', 'PREPARING', 'PREPARED', "
            "'PREPARE_FAILED', 'APPLYING', 'VERIFYING', 'ACTIVE', "
            "'APPLY_FAILED', 'VERIFY_FAILED')",
            name="ck_fleet_deployment_runtime_status",
        ),
        CheckConstraint(
            "(status NOT IN ('PREPARE_FAILED', 'APPLY_FAILED', "
            "'VERIFY_FAILED') AND failure_phase IS NULL "
            "AND failure_code IS NULL) OR "
            "(status = 'PREPARE_FAILED' AND failure_phase = 'PREPARE' "
            "AND failure_code IS NOT NULL "
            "AND length(failure_code) BETWEEN 1 AND 64) OR "
            "(status = 'APPLY_FAILED' AND failure_phase = 'APPLY' "
            "AND failure_code IS NOT NULL "
            "AND length(failure_code) BETWEEN 1 AND 64) OR "
            "(status = 'VERIFY_FAILED' AND failure_phase = 'VERIFY' "
            "AND failure_code IS NOT NULL "
            "AND length(failure_code) BETWEEN 1 AND 64)",
            name="ck_fleet_deployment_runtime_failure",
        ),
        ForeignKeyConstraint(
            ["fleet_id", "deployment_id"],
            ["deployments.fleet_id", "deployments.id"],
            ondelete="CASCADE",
            name="fk_fleet_deployment_runtime_fleet_deployment",
        ),
        UniqueConstraint(
            "fleet_id",
            "deployment_id",
            name="uq_fleet_deployment_runtime_fleet_deployment",
        ),
        UniqueConstraint(
            "preparation_id",
            name="uq_fleet_deployment_runtime_preparation",
        ),
        UniqueConstraint(
            "current_operation_id",
            name="uq_fleet_deployment_runtime_current_operation",
        ),
        Index(
            "ix_fleet_deployment_runtime_fleet_status",
            "fleet_id",
            "status",
        ),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    fleet_id: Mapped[str] = mapped_column(
        ForeignKey(
            "fleets.id",
            ondelete="CASCADE",
            name="fk_fleet_deployment_runtime_fleet_id",
        ),
        nullable=False,
    )
    deployment_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), default="ACCEPTED", nullable=False
    )
    preparation_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey(
            "artifact_preparations.id",
            name="fk_fleet_deployment_runtime_preparation_id",
        ),
    )
    current_operation_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey(
            "deployment_operations.id",
            name="fk_fleet_deployment_runtime_current_operation_id",
        ),
    )
    failure_phase: Mapped[str | None] = mapped_column(String(16))
    failure_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "fleet_id": self.fleet_id,
            "deployment_id": self.deployment_id,
            "status": self.status,
            "preparation_id": self.preparation_id,
            "current_operation_id": self.current_operation_id,
            "failure_phase": self.failure_phase,
            "failure_code": self.failure_code,
            "created_at": (
                self.created_at.isoformat()
                if self.created_at is not None
                else None
            ),
            "updated_at": (
                self.updated_at.isoformat()
                if self.updated_at is not None
                else None
            ),
        }


class DeploymentOperation(Base):
    __tablename__ = "deployment_operations"
    __table_args__ = (
        CheckConstraint(
            "length(id) = 36",
            name="ck_deployment_operation_id_length",
        ),
        CheckConstraint(
            "length(request_digest) = 71 AND request_digest LIKE 'sha256:%'",
            name="ck_deployment_operation_request_digest_sha256",
        ),
        CheckConstraint(
            "kind IN ('APPLY', 'VERIFY', 'ROLLBACK')",
            name="ck_deployment_operation_kind",
        ),
        CheckConstraint(
            "status IN ('PREPARED', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'PARTIAL_FAILED', 'FAILED')",
            name="ck_deployment_operation_status",
        ),
        CheckConstraint(
            "phase IN ('APPLY', 'VERIFY', 'STOP_SOURCE', 'START_TARGET', "
            "'VERIFY_TARGET', 'START_API', 'VERIFY_API', 'COMPLETE')",
            name="ck_deployment_operation_phase",
        ),
        CheckConstraint(
            "(kind = 'ROLLBACK' AND rollback_target_id IS NOT NULL) OR "
            "(kind IN ('APPLY', 'VERIFY') AND rollback_target_id IS NULL)",
            name="ck_deployment_operation_rollback_target",
        ),
        UniqueConstraint(
            "request_digest",
            name="uq_deployment_operations_request_digest",
        ),
        UniqueConstraint(
            "active_lineage_id",
            name="uq_deployment_operations_active_lineage_id",
        ),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    request_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    lineage_id: Mapped[str] = mapped_column(String(255), nullable=False)
    deployment_id: Mapped[str] = mapped_column(
        ForeignKey(
            "deployments.id",
            name="fk_deployment_operations_deployment_id",
        ),
        nullable=False,
    )
    rollback_target_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "deployments.id",
            name="fk_deployment_operations_rollback_target_id",
        )
    )
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    phase: Mapped[str] = mapped_column(String(20), nullable=False)
    node_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    serve: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    api: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    active_lineage_id: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DeploymentOperationNode(Base):
    __tablename__ = "deployment_operation_nodes"
    __table_args__ = (
        CheckConstraint(
            "length(id) = 36",
            name="ck_deployment_operation_node_id_length",
        ),
        CheckConstraint(
            "phase IN ('APPLY', 'VERIFY', 'STOP_SOURCE', 'START_TARGET', "
            "'VERIFY_TARGET', 'START_API', 'VERIFY_API', 'COMPLETE')",
            name="ck_deployment_operation_node_phase",
        ),
        CheckConstraint(
            "status IN ('PENDING', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'FAILED', 'CANCELED')",
            name="ck_deployment_operation_node_status",
        ),
        CheckConstraint(
            "attempt_count >= 0",
            name="ck_deployment_operation_node_attempt_nonnegative",
        ),
        CheckConstraint(
            "failure_code IS NULL OR "
            "(length(failure_code) > 0 AND length(failure_code) <= 64)",
            name="ck_deployment_operation_node_failure_code",
        ),
        UniqueConstraint(
            "operation_id",
            "node_id",
            "phase",
            name="uq_deployment_operation_nodes_operation_node_phase",
        ),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    operation_id: Mapped[str] = mapped_column(
        ForeignKey(
            "deployment_operations.id",
            ondelete="CASCADE",
            name="fk_deployment_operation_nodes_operation_id",
        ),
        nullable=False,
    )
    node_id: Mapped[str] = mapped_column(
        ForeignKey(
            "nodes.id",
            name="fk_deployment_operation_nodes_node_id",
        ),
        nullable=False,
    )
    phase: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failure_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ModelArtifact(Base):
    __tablename__ = "model_artifacts"
    __table_args__ = (
        UniqueConstraint("repository", "revision", "quantization"),
        UniqueConstraint(
            "id",
            "manifest_digest",
            name="uq_model_artifacts_id_manifest_digest",
        ),
        CheckConstraint("size_mib > 0", name="ck_model_artifact_size_positive"),
        CheckConstraint("default_max_model_len > 0", name="ck_model_artifact_context_positive"),
        CheckConstraint("layer_count > 0", name="ck_model_artifact_layers_positive"),
        Index("ix_model_artifacts_model_id", "model_id"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    model_id: Mapped[str] = mapped_column(String(100), nullable=False)
    repository: Mapped[str] = mapped_column(String(255), nullable=False)
    revision: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest_digest: Mapped[str] = mapped_column(String(71), nullable=False, unique=True)
    quantization: Mapped[str] = mapped_column(String(40), nullable=False)
    size_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    default_max_model_len: Mapped[int] = mapped_column(Integer, nullable=False)
    layer_count: Mapped[int] = mapped_column(Integer, nullable=False)
    license_id: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ArtifactManifest(Base):
    __tablename__ = "artifact_manifests"
    __table_args__ = (
        CheckConstraint(
            "length(digest) = 71 AND digest LIKE 'sha256:%'",
            name="ck_artifact_manifest_digest_sha256",
        ),
        CheckConstraint(
            "schema_version = 1",
            name="ck_artifact_manifest_schema_version",
        ),
        CheckConstraint(
            "total_size_bytes > 0",
            name="ck_artifact_manifest_total_size_positive",
        ),
        CheckConstraint(
            "file_count > 0",
            name="ck_artifact_manifest_file_count_positive",
        ),
        CheckConstraint(
            "chunk_count > 0",
            name="ck_artifact_manifest_chunk_count_positive",
        ),
        CheckConstraint(
            "length(canonical_json) > 0",
            name="ck_artifact_manifest_canonical_json_nonempty",
        ),
        ForeignKeyConstraint(
            ["model_artifact_id", "digest"],
            ["model_artifacts.id", "model_artifacts.manifest_digest"],
            name="fk_artifact_manifests_model_artifact_identity",
        ),
        Index(
            "ix_artifact_manifests_model_artifact_id",
            "model_artifact_id",
        ),
    )
    digest: Mapped[str] = mapped_column(String(71), primary_key=True)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    model_artifact_id: Mapped[str | None] = mapped_column(String(36))
    total_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    file_count: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False)
    canonical_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ArtifactManifestFile(Base):
    __tablename__ = "artifact_manifest_files"
    __table_args__ = (
        CheckConstraint(
            "length(id) = 36",
            name="ck_artifact_manifest_file_id_length",
        ),
        CheckConstraint(
            "ordinal >= 0",
            name="ck_artifact_manifest_file_ordinal_nonnegative",
        ),
        CheckConstraint(
            "length(path) >= 1 AND length(path) <= 1024",
            name="ck_artifact_manifest_file_path_length",
        ),
        CheckConstraint(
            "path NOT LIKE '/%' AND path <> '.' AND path <> '..' "
            "AND path NOT LIKE './%' AND path NOT LIKE '../%' "
            "AND path NOT LIKE '%/./%' AND path NOT LIKE '%/../%' "
            "AND path NOT LIKE '%/.' AND path NOT LIKE '%/..' "
            "AND path NOT LIKE '%//%' AND path NOT LIKE '%/'",
            name="ck_artifact_manifest_file_path_relative",
        ),
        CheckConstraint(
            "kind = 'REGULAR'",
            name="ck_artifact_manifest_file_kind",
        ),
        CheckConstraint(
            "size_bytes >= 0",
            name="ck_artifact_manifest_file_size_nonnegative",
        ),
        CheckConstraint(
            "length(file_digest) = 71 AND file_digest LIKE 'sha256:%'",
            name="ck_artifact_manifest_file_digest_sha256",
        ),
        UniqueConstraint(
            "manifest_digest",
            "path",
            name="uq_artifact_manifest_files_manifest_path",
        ),
        UniqueConstraint(
            "manifest_digest",
            "ordinal",
            name="uq_artifact_manifest_files_manifest_ordinal",
        ),
        Index(
            "ix_artifact_manifest_files_manifest_digest",
            "manifest_digest",
        ),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    manifest_digest: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_manifests.digest",
            ondelete="CASCADE",
            name="fk_artifact_manifest_files_manifest_digest",
        ),
        nullable=False,
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    path: Mapped[str] = mapped_column(String(1024), nullable=False)
    kind: Mapped[str] = mapped_column(String(20), default="REGULAR", nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    file_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ArtifactChunk(Base):
    __tablename__ = "artifact_chunks"
    __table_args__ = (
        CheckConstraint(
            "length(digest) = 71 AND digest LIKE 'sha256:%'",
            name="ck_artifact_chunk_digest_sha256",
        ),
        CheckConstraint(
            "size_bytes > 0",
            name="ck_artifact_chunk_size_positive",
        ),
    )
    digest: Mapped[str] = mapped_column(String(71), primary_key=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ArtifactFileChunk(Base):
    __tablename__ = "artifact_file_chunks"
    __table_args__ = (
        CheckConstraint(
            "ordinal >= 0",
            name="ck_artifact_file_chunk_ordinal_nonnegative",
        ),
        CheckConstraint(
            "offset_bytes >= 0",
            name="ck_artifact_file_chunk_offset_nonnegative",
        ),
        CheckConstraint(
            "length_bytes > 0",
            name="ck_artifact_file_chunk_length_positive",
        ),
        UniqueConstraint(
            "file_id",
            "offset_bytes",
            name="uq_artifact_file_chunks_file_offset",
        ),
        Index(
            "ix_artifact_file_chunks_chunk_digest",
            "chunk_digest",
        ),
    )
    file_id: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_manifest_files.id",
            ondelete="CASCADE",
            name="fk_artifact_file_chunks_file_id",
        ),
        primary_key=True,
    )
    ordinal: Mapped[int] = mapped_column(Integer, primary_key=True)
    chunk_digest: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_chunks.digest",
            name="fk_artifact_file_chunks_chunk_digest",
        ),
        nullable=False,
    )
    offset_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    length_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)


class StageArtifactVariant(Base):
    __tablename__ = "stage_artifact_variants"
    __table_args__ = (
        CheckConstraint(
            "length(artifact_set_digest) = 71 "
            "AND artifact_set_digest LIKE 'sha256:%'",
            name="ck_stage_variant_set_sha256",
        ),
        CheckConstraint(
            "length(contract_identity_digest) = 71 "
            "AND contract_identity_digest LIKE 'sha256:%'",
            name="ck_stage_variant_contract_sha256",
        ),
        CheckConstraint(
            "length(source_manifest_digest) = 71 "
            "AND source_manifest_digest LIKE 'sha256:%'",
            name="ck_stage_variant_source_sha256",
        ),
        CheckConstraint(
            "runtime_image LIKE '%@sha256:" + "_" * 64 + "'",
            name="ck_stage_variant_runtime_digest",
        ),
        CheckConstraint(
            "vllm_version = '0.9.0'",
            name="ck_stage_variant_vllm_version",
        ),
        CheckConstraint(
            "length(exporter_build_digest) = 71 "
            "AND exporter_build_digest LIKE 'sha256:%'",
            name="ck_stage_variant_exporter_sha256",
        ),
        CheckConstraint(
            "architecture = 'Qwen2ForCausalLM'",
            name="ck_stage_variant_architecture",
        ),
        CheckConstraint(
            "quantization = 'awq'",
            name="ck_stage_variant_quantization",
        ),
        CheckConstraint(
            "tensor_parallel_size = 1",
            name="ck_stage_variant_tp_supported",
        ),
        CheckConstraint(
            "pipeline_parallel_size > 0 AND pipeline_parallel_size <= 64",
            name="ck_stage_variant_pp_range",
        ),
        CheckConstraint(
            "rank_count = tensor_parallel_size * pipeline_parallel_size",
            name="ck_stage_variant_rank_count",
        ),
        CheckConstraint(
            "loader_format = 'VLLM_SHARDED_STATE_V1'",
            name="ck_stage_variant_loader_format",
        ),
        CheckConstraint(
            "status IN ('DRAFT', 'VALIDATED', 'REVOKED')",
            name="ck_stage_variant_status",
        ),
        CheckConstraint(
            "length(canonical_identity_json) > 0",
            name="ck_stage_variant_identity_json_nonempty",
        ),
        CheckConstraint(
            "(status = 'DRAFT' AND validated_at IS NULL AND revoked_at IS NULL) OR "
            "(status = 'VALIDATED' AND validated_at IS NOT NULL AND revoked_at IS NULL) OR "
            "(status = 'REVOKED' AND revoked_at IS NOT NULL)",
            name="ck_stage_variant_status_timestamps",
        ),
        UniqueConstraint(
            "artifact_set_digest",
            "tensor_parallel_size",
            "pipeline_parallel_size",
            name="uq_stage_variant_set_topology",
        ),
        UniqueConstraint(
            "contract_identity_digest",
            name="uq_stage_variant_contract_identity",
        ),
        UniqueConstraint(
            "artifact_set_digest",
            "source_manifest_digest",
            name="uq_stage_variant_set_source",
        ),
        Index("ix_stage_variants_source_manifest", "source_manifest_digest"),
        Index("ix_stage_variants_runtime_release", "runtime_release_id"),
        Index("ix_stage_variants_status", "status"),
    )
    artifact_set_digest: Mapped[str] = mapped_column(String(71), primary_key=True)
    contract_identity_digest: Mapped[str] = mapped_column(
        String(71), nullable=False
    )
    source_manifest_digest: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_manifests.digest",
            name="fk_stage_variant_source_manifest",
        ),
        nullable=False,
    )
    runtime_release_id: Mapped[str] = mapped_column(
        ForeignKey(
            "runtime_releases.id",
            name="fk_stage_variant_runtime_release",
        ),
        nullable=False,
    )
    runtime_image: Mapped[str] = mapped_column(String(512), nullable=False)
    vllm_version: Mapped[str] = mapped_column(String(64), nullable=False)
    exporter_build_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    architecture: Mapped[str] = mapped_column(String(100), nullable=False)
    quantization: Mapped[str] = mapped_column(String(40), nullable=False)
    tensor_parallel_size: Mapped[int] = mapped_column(Integer, nullable=False)
    pipeline_parallel_size: Mapped[int] = mapped_column(Integer, nullable=False)
    rank_count: Mapped[int] = mapped_column(Integer, nullable=False)
    loader_format: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="DRAFT", nullable=False)
    canonical_identity_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class StageArtifactRank(Base):
    __tablename__ = "stage_artifact_ranks"
    __table_args__ = (
        CheckConstraint("length(id) = 36", name="ck_stage_rank_id_length"),
        CheckConstraint(
            "rank >= 0 AND rank = pipeline_rank * tensor_parallel_size + tensor_rank",
            name="ck_stage_rank_linear_coordinate",
        ),
        CheckConstraint(
            "pipeline_rank >= 0 AND pipeline_rank < pipeline_parallel_size",
            name="ck_stage_rank_pipeline_range",
        ),
        CheckConstraint(
            "tensor_rank >= 0 AND tensor_rank < tensor_parallel_size",
            name="ck_stage_rank_tensor_range",
        ),
        CheckConstraint(
            "tensor_parallel_size = 1 AND pipeline_parallel_size > 0",
            name="ck_stage_rank_supported_topology",
        ),
        CheckConstraint(
            "length(manifest_digest) = 71 AND manifest_digest LIKE 'sha256:%'",
            name="ck_stage_rank_manifest_sha256",
        ),
        CheckConstraint(
            "tensor_key_count > 0",
            name="ck_stage_rank_tensor_count_positive",
        ),
        CheckConstraint(
            "length(tensor_keys_digest) = 71 "
            "AND tensor_keys_digest LIKE 'sha256:%'",
            name="ck_stage_rank_tensor_keys_sha256",
        ),
        CheckConstraint(
            "weight_size_bytes > 0",
            name="ck_stage_rank_weight_size_positive",
        ),
        ForeignKeyConstraint(
            ["variant_id", "tensor_parallel_size", "pipeline_parallel_size"],
            [
                "stage_artifact_variants.artifact_set_digest",
                "stage_artifact_variants.tensor_parallel_size",
                "stage_artifact_variants.pipeline_parallel_size",
            ],
            ondelete="CASCADE",
            name="fk_stage_rank_variant_topology",
        ),
        UniqueConstraint(
            "variant_id",
            "rank",
            name="uq_stage_rank_variant_rank",
        ),
        UniqueConstraint(
            "variant_id",
            "pipeline_rank",
            "tensor_rank",
            name="uq_stage_rank_variant_coordinate",
        ),
        UniqueConstraint(
            "variant_id",
            "manifest_digest",
            name="uq_stage_rank_variant_manifest",
        ),
        UniqueConstraint(
            "variant_id",
            "rank",
            "manifest_digest",
            "tensor_keys_digest",
            name="uq_stage_rank_evidence_identity",
        ),
        Index("ix_stage_ranks_manifest_digest", "manifest_digest"),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    variant_id: Mapped[str] = mapped_column(String(71), nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    pipeline_rank: Mapped[int] = mapped_column(Integer, nullable=False)
    tensor_rank: Mapped[int] = mapped_column(Integer, nullable=False)
    tensor_parallel_size: Mapped[int] = mapped_column(Integer, nullable=False)
    pipeline_parallel_size: Mapped[int] = mapped_column(Integer, nullable=False)
    manifest_digest: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_manifests.digest",
            name="fk_stage_rank_manifest",
        ),
        nullable=False,
    )
    tensor_key_count: Mapped[int] = mapped_column(Integer, nullable=False)
    tensor_keys_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    weight_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class StageArtifactValidationEvidence(Base):
    __tablename__ = "stage_artifact_validation_evidence"
    __table_args__ = (
        CheckConstraint(
            "length(identity_digest) = 71 AND identity_digest LIKE 'sha256:%'",
            name="ck_stage_evidence_identity_sha256",
        ),
        CheckConstraint(
            "length(validation_run_id) = 36",
            name="ck_stage_evidence_run_id_length",
        ),
        CheckConstraint(
            "registration_sequence > 0",
            name="ck_stage_evidence_sequence_positive",
        ),
        CheckConstraint(
            "schema_version = 1",
            name="ck_stage_evidence_schema_version",
        ),
        CheckConstraint(
            "kind IN ('SYNTHETIC', 'GPU_EXPORT_LOAD')",
            name="ck_stage_evidence_kind",
        ),
        CheckConstraint(
            "status IN ('PASSED', 'FAILED', 'NOT_RUN')",
            name="ck_stage_evidence_status",
        ),
        CheckConstraint(
            "length(validator_version) > 0",
            name="ck_stage_evidence_validator_nonempty",
        ),
        CheckConstraint(
            "length(validator_build_digest) = 71 "
            "AND validator_build_digest LIKE 'sha256:%'",
            name="ck_stage_evidence_validator_sha256",
        ),
        CheckConstraint(
            "(status = 'PASSED' AND rank_count > 0 AND failure_code IS NULL) OR "
            "(status IN ('FAILED', 'NOT_RUN') AND rank_count >= 0 "
            "AND failure_code IS NOT NULL)",
            name="ck_stage_evidence_result_shape",
        ),
        CheckConstraint(
            "failure_code IS NULL OR failure_code IN ("
            "'STAGE_EXPORT_FAILED', 'STAGE_LOAD_FAILED', "
            "'STAGE_TENSOR_COVERAGE_INVALID', 'STAGE_MANIFEST_MISMATCH', "
            "'STAGE_TOPOLOGY_MISMATCH', 'STAGE_GPU_NOT_AVAILABLE', "
            "'STAGE_VALIDATION_NOT_RUN')",
            name="ck_stage_evidence_failure_code",
        ),
        CheckConstraint(
            "length(canonical_evidence_json) > 0",
            name="ck_stage_evidence_json_nonempty",
        ),
        UniqueConstraint(
            "variant_id",
            "registration_sequence",
            name="uq_stage_evidence_variant_sequence",
        ),
        UniqueConstraint(
            "variant_id",
            "validation_run_id",
            name="uq_stage_evidence_variant_run",
        ),
        UniqueConstraint(
            "identity_digest",
            "variant_id",
            name="uq_stage_evidence_identity_variant",
        ),
        Index(
            "ix_stage_evidence_variant_kind_sequence",
            "variant_id",
            "kind",
            "registration_sequence",
        ),
    )
    identity_digest: Mapped[str] = mapped_column(String(71), primary_key=True)
    variant_id: Mapped[str] = mapped_column(
        ForeignKey(
            "stage_artifact_variants.artifact_set_digest",
            ondelete="CASCADE",
            name="fk_stage_evidence_variant",
        ),
        nullable=False,
    )
    validation_run_id: Mapped[str] = mapped_column(String(36), nullable=False)
    registration_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    validator_version: Mapped[str] = mapped_column(String(64), nullable=False)
    validator_build_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    rank_count: Mapped[int] = mapped_column(Integer, nullable=False)
    failure_code: Mapped[str | None] = mapped_column(String(64))
    canonical_evidence_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class StageArtifactValidationRank(Base):
    __tablename__ = "stage_artifact_validation_ranks"
    __table_args__ = (
        CheckConstraint(
            "rank >= 0",
            name="ck_stage_evidence_rank_nonnegative",
        ),
        CheckConstraint(
            "length(manifest_digest) = 71 AND manifest_digest LIKE 'sha256:%'",
            name="ck_stage_evidence_rank_manifest_sha256",
        ),
        CheckConstraint(
            "length(tensor_keys_digest) = 71 "
            "AND tensor_keys_digest LIKE 'sha256:%'",
            name="ck_stage_evidence_rank_keys_sha256",
        ),
        CheckConstraint(
            "loaded_tensor_count > 0",
            name="ck_stage_evidence_rank_tensor_count",
        ),
        CheckConstraint(
            "loaded_weight_size_bytes > 0",
            name="ck_stage_evidence_rank_weight_size",
        ),
        ForeignKeyConstraint(
            ["evidence_id", "variant_id"],
            [
                "stage_artifact_validation_evidence.identity_digest",
                "stage_artifact_validation_evidence.variant_id",
            ],
            ondelete="CASCADE",
            name="fk_stage_evidence_rank_evidence",
        ),
        ForeignKeyConstraint(
            ["variant_id", "rank", "manifest_digest", "tensor_keys_digest"],
            [
                "stage_artifact_ranks.variant_id",
                "stage_artifact_ranks.rank",
                "stage_artifact_ranks.manifest_digest",
                "stage_artifact_ranks.tensor_keys_digest",
            ],
            name="fk_stage_evidence_rank_stage",
        ),
    )
    evidence_id: Mapped[str] = mapped_column(String(71), primary_key=True)
    rank: Mapped[int] = mapped_column(Integer, primary_key=True)
    variant_id: Mapped[str] = mapped_column(String(71), nullable=False)
    manifest_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    tensor_keys_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    loaded_tensor_count: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_weight_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)


class RuntimeRelease(Base):
    __tablename__ = "runtime_releases"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    image: Mapped[str] = mapped_column(String(512), nullable=False, unique=True)
    vllm_version: Mapped[str] = mapped_column(String(64), nullable=False)
    cuda_version: Mapped[str] = mapped_column(String(64), nullable=False)
    gpu_architectures: Mapped[list] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ModelRelease(Base):
    __tablename__ = "model_releases"
    __table_args__ = (
        UniqueConstraint("artifact_id", "runtime_id"),
        CheckConstraint("quality_rank > 0", name="ck_model_release_quality_positive"),
        CheckConstraint(
            "status IN ('DRAFT', 'VALIDATED', 'ACTIVE', 'DEPRECATED', 'REVOKED')",
            name="ck_model_release_status",
        ),
        Index("ix_model_releases_status", "status"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    artifact_id: Mapped[str] = mapped_column(ForeignKey("model_artifacts.id"), nullable=False)
    runtime_id: Mapped[str] = mapped_column(ForeignKey("runtime_releases.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="DRAFT", nullable=False)
    quality_rank: Mapped[int] = mapped_column(Integer, nullable=False)
    promotion_evidence_ids: Mapped[list[str] | None] = mapped_column(JSON)
    promotion_evidence_digest: Mapped[str | None] = mapped_column(String(71))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class PlacementProfileRecord(Base):
    __tablename__ = "placement_profiles"
    __table_args__ = (
        UniqueConstraint("release_id", "profile_id"),
        CheckConstraint("node_count > 0", name="ck_placement_node_count_positive"),
        CheckConstraint("min_gpu_memory_mib > 0", name="ck_placement_vram_positive"),
        CheckConstraint("min_disk_free_mib > 0", name="ck_placement_disk_positive"),
        CheckConstraint("pipeline_parallel_size > 0", name="ck_placement_pp_positive"),
        CheckConstraint("tensor_parallel_size > 0", name="ck_placement_tp_positive"),
        CheckConstraint("max_model_len > 0", name="ck_placement_context_positive"),
        CheckConstraint("max_concurrency > 0", name="ck_placement_concurrency_positive"),
        CheckConstraint(
            "origin IN ('MANUAL', 'AUTO')",
            name="ck_placement_origin",
        ),
        CheckConstraint(
            "status IN ('DRAFT', 'QUALIFYING', 'VALIDATED', 'ACTIVE', 'REVOKED')",
            name="ck_placement_status",
        ),
        CheckConstraint(
            "origin != 'AUTO' OR tensor_parallel_size = 1",
            name="ck_placement_auto_tp1",
        ),
        CheckConstraint(
            "origin != 'AUTO' OR pipeline_parallel_size = node_count",
            name="ck_placement_auto_pp_nodes",
        ),
        CheckConstraint(
            "origin != 'AUTO' OR status IN ('DRAFT', 'QUALIFYING') "
            "OR qualification_evidence_id IS NOT NULL",
            name="ck_placement_auto_evidence",
        ),
        CheckConstraint(
            "origin != 'AUTO' OR status NOT IN ('VALIDATED', 'ACTIVE') "
            "OR qualified_at IS NOT NULL",
            name="ck_placement_auto_qualified_at",
        ),
        CheckConstraint(
            "origin != 'AUTO' OR status != 'ACTIVE' OR activated_at IS NOT NULL",
            name="ck_placement_auto_activation",
        ),
        CheckConstraint(
            "max_packet_loss_pct IS NULL OR (max_packet_loss_pct >= 0 AND max_packet_loss_pct <= 100)",
            name="ck_placement_packet_loss_range",
        ),
        CheckConstraint(
            "min_success_rate >= 0 AND min_success_rate <= 1",
            name="ck_placement_success_rate_range",
        ),
        CheckConstraint(
            "min_vram_headroom_pct >= 0 AND min_vram_headroom_pct <= 100",
            name="ck_placement_vram_headroom_range",
        ),
        CheckConstraint("max_ttft_p95_ms > 0", name="ck_placement_ttft_positive"),
        CheckConstraint("max_tpot_p95_ms > 0", name="ck_placement_tpot_positive"),
        CheckConstraint("max_e2e_p95_ms > 0", name="ck_placement_e2e_positive"),
        CheckConstraint("min_throughput_tps > 0", name="ck_placement_throughput_positive"),
        CheckConstraint(
            "min_bandwidth_mbps IS NULL OR min_bandwidth_mbps > 0",
            name="ck_placement_bandwidth_positive",
        ),
        CheckConstraint(
            "max_rtt_ms IS NULL OR max_rtt_ms >= 0", name="ck_placement_rtt_nonnegative"
        ),
        Index("ix_placement_profiles_status", "status"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    release_id: Mapped[str] = mapped_column(ForeignKey("model_releases.id", ondelete="CASCADE"), nullable=False)
    profile_id: Mapped[str] = mapped_column(String(100), nullable=False)
    topology: Mapped[str] = mapped_column(String(30), nullable=False)
    node_count: Mapped[int] = mapped_column(Integer, nullable=False)
    min_gpu_memory_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    min_disk_free_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    pipeline_parallel_size: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    tensor_parallel_size: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    max_model_len: Mapped[int] = mapped_column(Integer, default=8192, nullable=False)
    max_concurrency: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    origin: Mapped[str] = mapped_column(String(20), default="MANUAL", nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="ACTIVE", nullable=False)
    spec_digest: Mapped[str | None] = mapped_column(String(71))
    qualification_evidence_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "profile_qualification_evidence.id",
            name="fk_placement_profiles_qualification_evidence",
            use_alter=True,
        )
    )
    qualified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    requires_network_evidence: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    requires_nccl: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    min_bandwidth_mbps: Mapped[int | None] = mapped_column(Integer)
    max_rtt_ms: Mapped[float | None] = mapped_column(Float)
    max_packet_loss_pct: Mapped[float | None] = mapped_column(Float)
    max_ttft_p95_ms: Mapped[float] = mapped_column(Float, nullable=False)
    max_tpot_p95_ms: Mapped[float] = mapped_column(Float, nullable=False)
    max_e2e_p95_ms: Mapped[float] = mapped_column(Float, nullable=False)
    min_success_rate: Mapped[float] = mapped_column(Float, default=0.99, nullable=False)
    min_vram_headroom_pct: Mapped[float] = mapped_column(Float, default=10.0, nullable=False)
    min_throughput_tps: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ProfileQualificationRun(Base):
    __tablename__ = "profile_qualification_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('QUALIFYING', 'PASSED', 'FAILED', 'CANCELED')",
            name="ck_profile_qualification_run_status",
        ),
        CheckConstraint(
            "length(inventory_fingerprint) = 71 "
            "AND inventory_fingerprint LIKE 'sha256:%'",
            name="ck_profile_qualification_run_inventory_sha256",
        ),
        CheckConstraint(
            "length(profile_spec_digest) = 71 "
            "AND profile_spec_digest LIKE 'sha256:%'",
            name="ck_profile_qualification_run_spec_sha256",
        ),
        CheckConstraint(
            "length(workload_digest) = 71 "
            "AND workload_digest LIKE 'sha256:%'",
            name="ck_profile_qualification_run_workload_sha256",
        ),
        CheckConstraint(
            "max_model_len > 0",
            name="ck_profile_qualification_run_context_positive",
        ),
        CheckConstraint(
            "max_concurrency > 0",
            name="ck_profile_qualification_run_concurrency_positive",
        ),
        CheckConstraint(
            "(status = 'QUALIFYING' AND evidence_id IS NULL "
            "AND failure_code IS NULL) OR "
            "(status = 'PASSED' AND evidence_id IS NOT NULL "
            "AND failure_code IS NULL) OR "
            "(status IN ('FAILED', 'CANCELED') AND failure_code IS NOT NULL)",
            name="ck_profile_qualification_run_outcome",
        ),
        Index("ix_profile_qualification_runs_placement", "placement_id"),
        Index("ix_profile_qualification_runs_status", "status"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    release_id: Mapped[str] = mapped_column(
        ForeignKey("model_releases.id"), nullable=False
    )
    placement_id: Mapped[str] = mapped_column(
        ForeignKey("placement_profiles.id"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    node_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    rank_node_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    gpu_bindings: Mapped[list[dict]] = mapped_column(JSON, nullable=False)
    inventory_fingerprint: Mapped[str] = mapped_column(String(71), nullable=False)
    profile_spec_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    suite_id: Mapped[str] = mapped_column(String(64), nullable=False)
    required_steps: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    workload: Mapped[dict] = mapped_column(JSON, nullable=False)
    workload_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    max_model_len: Mapped[int] = mapped_column(Integer, nullable=False)
    max_concurrency: Mapped[int] = mapped_column(Integer, nullable=False)
    artifact_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_manifest_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    runtime_image: Mapped[str] = mapped_column(String(512), nullable=False)
    runtime_vllm_version: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "profile_qualification_evidence.id",
            name="fk_profile_qualification_runs_evidence",
            use_alter=True,
        )
    )
    failure_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class ProfileQualificationBinding(Base):
    __tablename__ = "profile_qualification_bindings"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "node_id",
            name="uq_profile_qualification_binding_node",
        ),
        UniqueConstraint(
            "run_id",
            "gpu_uuid",
            name="uq_profile_qualification_binding_gpu_uuid",
        ),
        CheckConstraint(
            "rank >= 0", name="ck_profile_qualification_binding_rank"
        ),
        CheckConstraint(
            "gpu_index >= 0", name="ck_profile_qualification_binding_gpu_index"
        ),
        CheckConstraint(
            "gpu_uuid LIKE 'GPU-%'",
            name="ck_profile_qualification_binding_gpu_uuid",
        ),
        CheckConstraint(
            "memory_mib > 0",
            name="ck_profile_qualification_binding_memory",
        ),
        Index("ix_profile_qualification_bindings_node", "node_id"),
    )
    run_id: Mapped[str] = mapped_column(
        ForeignKey("profile_qualification_runs.id"), primary_key=True
    )
    rank: Mapped[int] = mapped_column(Integer, primary_key=True)
    node_id: Mapped[str] = mapped_column(ForeignKey("nodes.id"), nullable=False)
    gpu_index: Mapped[int] = mapped_column(Integer, nullable=False)
    gpu_uuid: Mapped[str] = mapped_column(String(128), nullable=False)
    memory_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    compute_capability: Mapped[str | None] = mapped_column(String(32))


class ProfileQualificationEvidence(Base):
    __tablename__ = "profile_qualification_evidence"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PASSED', 'FAILED')",
            name="ck_profile_qualification_evidence_status",
        ),
        CheckConstraint(
            "length(evidence_digest) = 71 "
            "AND evidence_digest LIKE 'sha256:%'",
            name="ck_profile_qualification_evidence_sha256",
        ),
        CheckConstraint(
            "executor_image LIKE '%@sha256:%'",
            name="ck_profile_qualification_executor_digest",
        ),
        CheckConstraint(
            "length(workload_digest) = 71 "
            "AND workload_digest LIKE 'sha256:%'",
            name="ck_profile_qualification_evidence_workload_sha256",
        ),
        UniqueConstraint("run_id"),
        UniqueConstraint("evidence_digest"),
        Index("ix_profile_qualification_evidence_run", "run_id"),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    run_id: Mapped[str] = mapped_column(
        ForeignKey("profile_qualification_runs.id"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    steps: Mapped[list[dict]] = mapped_column(JSON, nullable=False)
    metrics: Mapped[dict] = mapped_column(JSON, nullable=False)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    suite_id: Mapped[str] = mapped_column(String(64), nullable=False)
    workload_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    executor_image: Mapped[str] = mapped_column(String(512), nullable=False)
    dure_commit: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )


class BenchmarkEvidence(Base):
    __tablename__ = "benchmark_evidence"
    __table_args__ = (
        CheckConstraint("request_count > 0", name="ck_benchmark_request_count_positive"),
        CheckConstraint(
            "registration_sequence > 0",
            name="ck_benchmark_registration_sequence_positive",
        ),
        CheckConstraint("duration_seconds > 0", name="ck_benchmark_duration_positive"),
        CheckConstraint(
            "oom_count >= 0",
            name="ck_benchmark_oom_count_nonnegative",
        ),
        CheckConstraint(
            "crash_count >= 0",
            name="ck_benchmark_crash_count_nonnegative",
        ),
        CheckConstraint(
            "restart_count >= 0",
            name="ck_benchmark_restart_count_nonnegative",
        ),
        CheckConstraint("input_tokens > 0", name="ck_benchmark_input_tokens_positive"),
        CheckConstraint("output_tokens > 0", name="ck_benchmark_output_tokens_positive"),
        CheckConstraint("concurrency > 0", name="ck_benchmark_concurrency_positive"),
        CheckConstraint(
            "warmup_requests >= 0", name="ck_benchmark_warmup_nonnegative"
        ),
        CheckConstraint(
            "ttft_p95_ms IS NULL OR ttft_p95_ms > 0",
            name="ck_benchmark_ttft_positive",
        ),
        CheckConstraint(
            "tpot_p95_ms IS NULL OR tpot_p95_ms > 0",
            name="ck_benchmark_tpot_positive",
        ),
        CheckConstraint(
            "e2e_p95_ms IS NULL OR e2e_p95_ms > 0",
            name="ck_benchmark_e2e_positive",
        ),
        CheckConstraint(
            "throughput_tps IS NULL OR throughput_tps > 0",
            name="ck_benchmark_throughput_positive",
        ),
        CheckConstraint(
            "success_rate >= 0 AND success_rate <= 1",
            name="ck_benchmark_success_rate_range",
        ),
        CheckConstraint(
            "vram_headroom_pct >= 0 AND vram_headroom_pct <= 100",
            name="ck_benchmark_vram_headroom_range",
        ),
        CheckConstraint(
            "quality_score >= 0 AND quality_score <= 1",
            name="ck_benchmark_quality_score_range",
        ),
        CheckConstraint(
            "network_bandwidth_mbps IS NULL OR network_bandwidth_mbps > 0",
            name="ck_benchmark_bandwidth_positive",
        ),
        CheckConstraint(
            "network_rtt_ms IS NULL OR network_rtt_ms >= 0",
            name="ck_benchmark_rtt_nonnegative",
        ),
        CheckConstraint(
            "packet_loss_pct IS NULL OR (packet_loss_pct >= 0 AND packet_loss_pct <= 100)",
            name="ck_benchmark_packet_loss_range",
        ),
        CheckConstraint(
            "status IN ('PASSED', 'FAILED')",
            name="ck_benchmark_status",
        ),
        CheckConstraint(
            "length(inventory_fingerprint) = 71 AND inventory_fingerprint LIKE 'sha256:%'",
            name="ck_benchmark_inventory_fingerprint_sha256",
        ),
        CheckConstraint(
            "length(evidence_digest) = 71 AND evidence_digest LIKE 'sha256:%'",
            name="ck_benchmark_evidence_digest_sha256",
        ),
        CheckConstraint(
            "benchmark_run_id IS NULL OR length(benchmark_run_id) = 36",
            name="ck_benchmark_evidence_run_id_length",
        ),
        Index("ix_benchmark_evidence_release_id", "release_id"),
        Index("ix_benchmark_evidence_placement_id", "placement_id"),
        Index("ix_benchmark_evidence_status", "status"),
        Index(
            "ux_benchmark_evidence_benchmark_run_id",
            "benchmark_run_id",
            unique=True,
        ),
        UniqueConstraint("placement_id", "registration_sequence"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    benchmark_run_id: Mapped[str | None] = mapped_column(String(36))
    release_id: Mapped[str] = mapped_column(
        ForeignKey("model_releases.id"), nullable=False
    )
    placement_id: Mapped[str] = mapped_column(
        ForeignKey("placement_profiles.id"), nullable=False
    )
    registration_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    suite_id: Mapped[str] = mapped_column(String(100), nullable=False)
    node_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    inventory_fingerprint: Mapped[str] = mapped_column(String(71), nullable=False)
    artifact_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_manifest_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    runtime_image: Mapped[str] = mapped_column(String(512), nullable=False)
    dure_commit: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    input_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    concurrency: Mapped[int] = mapped_column(Integer, nullable=False)
    warmup_requests: Mapped[int] = mapped_column(Integer, nullable=False)
    request_count: Mapped[int] = mapped_column(Integer, nullable=False)
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    oom_count: Mapped[int] = mapped_column(Integer, nullable=False)
    crash_count: Mapped[int] = mapped_column(Integer, nullable=False)
    restart_count: Mapped[int] = mapped_column(Integer, nullable=False)
    ttft_p95_ms: Mapped[float | None] = mapped_column(Float)
    tpot_p95_ms: Mapped[float | None] = mapped_column(Float)
    e2e_p95_ms: Mapped[float | None] = mapped_column(Float)
    throughput_tps: Mapped[float | None] = mapped_column(Float)
    success_rate: Mapped[float] = mapped_column(Float, nullable=False)
    vram_headroom_pct: Mapped[float] = mapped_column(Float, nullable=False)
    quality_score: Mapped[float] = mapped_column(Float, nullable=False)
    network_bandwidth_mbps: Mapped[float | None] = mapped_column(Float)
    network_rtt_ms: Mapped[float | None] = mapped_column(Float)
    packet_loss_pct: Mapped[float | None] = mapped_column(Float)
    nccl_all_reduce_ok: Mapped[bool | None] = mapped_column(Boolean)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    failure_codes: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    evidence_digest: Mapped[str] = mapped_column(String(71), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint(
            "(operation_node_id IS NULL AND operation_attempt IS NULL) OR "
            "(operation_node_id IS NOT NULL AND operation_attempt IS NOT NULL)",
            name="ck_tasks_operation_binding",
        ),
        CheckConstraint(
            "operation_attempt IS NULL OR operation_attempt >= 1",
            name="ck_tasks_operation_attempt_positive",
        ),
        UniqueConstraint(
            "operation_node_id",
            "operation_attempt",
            name="uq_tasks_operation_node_attempt",
        ),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    bulk_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    node_id: Mapped[str] = mapped_column(ForeignKey("nodes.id"), nullable=False, index=True)
    type: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default=TaskStatus.QUEUED.value, nullable=False)
    deployment_id: Mapped[str | None] = mapped_column(ForeignKey("deployments.id"))
    operation_node_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "deployment_operation_nodes.id",
            name="fk_tasks_operation_node_id",
        )
    )
    operation_attempt: Mapped[int | None] = mapped_column(Integer)
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[dict | None] = mapped_column(JSON)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class ArtifactPreparation(Base):
    __tablename__ = "artifact_preparations"
    __table_args__ = (
        CheckConstraint(
            "length(id) = 36",
            name="ck_artifact_preparation_id_length",
        ),
        CheckConstraint(
            "length(request_id) = 36",
            name="ck_artifact_preparation_request_id_length",
        ),
        CheckConstraint(
            "length(request_digest) = 71 AND request_digest LIKE 'sha256:%'",
            name="ck_artifact_preparation_request_digest_sha256",
        ),
        CheckConstraint(
            "status IN ('PREPARED', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'PARTIAL_FAILED', 'FAILED')",
            name="ck_artifact_preparation_status",
        ),
        UniqueConstraint(
            "request_id",
            name="uq_artifact_preparations_request_id",
        ),
        UniqueConstraint(
            "request_digest",
            name="uq_artifact_preparations_request_digest",
        ),
        UniqueConstraint(
            "deployment_id",
            name="uq_artifact_preparations_deployment_id",
        ),
        Index("ix_artifact_preparations_status", "status"),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    request_id: Mapped[str] = mapped_column(String(36), nullable=False)
    request_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    deployment_id: Mapped[str] = mapped_column(
        ForeignKey(
            "deployments.id",
            ondelete="CASCADE",
            name="fk_artifact_preparations_deployment_id",
        ),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(20), default="PREPARED", nullable=False
    )
    plan_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ArtifactPreparationNode(Base):
    __tablename__ = "artifact_preparation_nodes"
    __table_args__ = (
        CheckConstraint(
            "length(id) = 36",
            name="ck_artifact_preparation_node_id_length",
        ),
        CheckConstraint(
            "length(model_manifest_digest) = 71 "
            "AND model_manifest_digest LIKE 'sha256:%'",
            name="ck_artifact_preparation_node_manifest_digest_sha256",
        ),
        CheckConstraint(
            "runtime_image LIKE '%@sha256:" + "_" * 64 + "'",
            name="ck_artifact_preparation_node_runtime_image_digest",
        ),
        CheckConstraint(
            "model_status IN ('PREPARED', 'QUEUED', 'RUNNING', "
            "'SUCCEEDED', 'FAILED')",
            name="ck_artifact_preparation_node_model_status",
        ),
        CheckConstraint(
            "image_status IN ('PREPARED', 'QUEUED', 'RUNNING', "
            "'SUCCEEDED', 'FAILED')",
            name="ck_artifact_preparation_node_image_status",
        ),
        CheckConstraint(
            "model_current_attempt >= 0",
            name="ck_artifact_preparation_node_model_attempt_nonnegative",
        ),
        CheckConstraint(
            "image_current_attempt >= 0",
            name="ck_artifact_preparation_node_image_attempt_nonnegative",
        ),
        CheckConstraint(
            "(model_status = 'PREPARED' AND model_current_attempt = 0) OR "
            "(model_status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED') "
            "AND model_current_attempt >= 1)",
            name="ck_artifact_preparation_node_model_attempt_status",
        ),
        CheckConstraint(
            "(image_status = 'PREPARED' AND image_current_attempt = 0) OR "
            "(image_status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED') "
            "AND image_current_attempt >= 1)",
            name="ck_artifact_preparation_node_image_attempt_status",
        ),
        CheckConstraint(
            "model_failure_code IS NULL OR "
            "(model_status = 'FAILED' AND length(model_failure_code) > 0 "
            "AND length(model_failure_code) <= 64)",
            name="ck_artifact_preparation_node_model_failure_code",
        ),
        CheckConstraint(
            "image_failure_code IS NULL OR "
            "(image_status = 'FAILED' AND length(image_failure_code) > 0 "
            "AND length(image_failure_code) <= 64)",
            name="ck_artifact_preparation_node_image_failure_code",
        ),
        UniqueConstraint(
            "preparation_id",
            "node_id",
            name="uq_artifact_preparation_nodes_preparation_node",
        ),
        Index("ix_artifact_preparation_nodes_node_id", "node_id"),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    preparation_id: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_preparations.id",
            ondelete="CASCADE",
            name="fk_artifact_preparation_nodes_preparation_id",
        ),
        nullable=False,
    )
    node_id: Mapped[str] = mapped_column(
        ForeignKey(
            "nodes.id",
            name="fk_artifact_preparation_nodes_node_id",
        ),
        nullable=False,
    )
    model_manifest_digest: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_manifests.digest",
            name="fk_artifact_preparation_nodes_manifest_digest",
        ),
        nullable=False,
    )
    runtime_image: Mapped[str] = mapped_column(String(512), nullable=False)
    model_status: Mapped[str] = mapped_column(
        String(20), default="PREPARED", nullable=False
    )
    image_status: Mapped[str] = mapped_column(
        String(20), default="PREPARED", nullable=False
    )
    model_current_attempt: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False
    )
    image_current_attempt: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False
    )
    model_failure_code: Mapped[str | None] = mapped_column(String(64))
    image_failure_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class ArtifactPreparationAttempt(Base):
    __tablename__ = "artifact_preparation_attempts"
    __table_args__ = (
        CheckConstraint(
            "length(id) = 36",
            name="ck_artifact_preparation_attempt_id_length",
        ),
        CheckConstraint(
            "stage IN ('MODEL', 'IMAGE')",
            name="ck_artifact_preparation_attempt_stage",
        ),
        CheckConstraint(
            "attempt_no >= 1",
            name="ck_artifact_preparation_attempt_number_positive",
        ),
        CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', "
            "'CANCELED')",
            name="ck_artifact_preparation_attempt_status",
        ),
        CheckConstraint(
            "(status IN ('QUEUED', 'RUNNING') AND completed_at IS NULL) OR "
            "(status IN ('SUCCEEDED', 'FAILED', 'CANCELED') "
            "AND completed_at IS NOT NULL)",
            name="ck_artifact_preparation_attempt_completion",
        ),
        CheckConstraint(
            "failure_code IS NULL OR "
            "(status IN ('FAILED', 'CANCELED') AND length(failure_code) > 0 "
            "AND length(failure_code) <= 64)",
            name="ck_artifact_preparation_attempt_failure_code",
        ),
        UniqueConstraint(
            "preparation_node_id",
            "stage",
            "attempt_no",
            name="uq_artifact_preparation_attempts_node_stage_number",
        ),
        UniqueConstraint(
            "task_id",
            name="uq_artifact_preparation_attempts_task_id",
        ),
        Index(
            "ix_artifact_preparation_attempts_node_stage_status",
            "preparation_node_id",
            "stage",
            "status",
        ),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    preparation_node_id: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_preparation_nodes.id",
            ondelete="CASCADE",
            name="fk_artifact_preparation_attempts_preparation_node_id",
        ),
        nullable=False,
    )
    stage: Mapped[str] = mapped_column(String(10), nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    task_id: Mapped[str] = mapped_column(
        ForeignKey(
            "tasks.id",
            name="fk_artifact_preparation_attempts_task_id",
        ),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    failure_code: Mapped[str | None] = mapped_column(String(64))
    result: Mapped[dict | None] = mapped_column(JSON)
    download_progress: Mapped[dict | None] = mapped_column(
        JSON(none_as_null=True)
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class NodeArtifactCache(Base):
    __tablename__ = "node_artifact_caches"
    __table_args__ = (
        CheckConstraint(
            "length(id) = 36",
            name="ck_node_artifact_cache_id_length",
        ),
        CheckConstraint(
            "cache_kind IN ('FULL_SNAPSHOT', 'STAGE')",
            name="ck_node_artifact_cache_kind",
        ),
        CheckConstraint(
            "length(cache_identity_digest) = 71 "
            "AND cache_identity_digest LIKE 'sha256:%'",
            name="ck_node_artifact_cache_identity_sha256",
        ),
        CheckConstraint(
            "length(manifest_digest) = 71 "
            "AND manifest_digest LIKE 'sha256:%'",
            name="ck_node_artifact_cache_manifest_sha256",
        ),
        CheckConstraint(
            "length(source_manifest_digest) = 71 "
            "AND source_manifest_digest LIKE 'sha256:%'",
            name="ck_node_artifact_cache_source_sha256",
        ),
        CheckConstraint(
            "artifact_set_digest IS NULL OR "
            "(length(artifact_set_digest) = 71 "
            "AND artifact_set_digest LIKE 'sha256:%')",
            name="ck_node_artifact_cache_variant_sha256",
        ),
        CheckConstraint(
            "tensor_keys_digest IS NULL OR "
            "(length(tensor_keys_digest) = 71 "
            "AND tensor_keys_digest LIKE 'sha256:%')",
            name="ck_node_artifact_cache_tensor_keys_sha256",
        ),
        CheckConstraint(
            "(cache_kind = 'FULL_SNAPSHOT' "
            "AND cache_identity_digest = manifest_digest "
            "AND source_manifest_digest = manifest_digest "
            "AND artifact_set_digest IS NULL "
            "AND artifact_rank IS NULL "
            "AND pipeline_rank IS NULL "
            "AND tensor_rank IS NULL "
            "AND tensor_parallel_size IS NULL "
            "AND pipeline_parallel_size IS NULL "
            "AND tensor_keys_digest IS NULL) OR "
            "(cache_kind = 'STAGE' "
            "AND artifact_set_digest IS NOT NULL "
            "AND artifact_rank IS NOT NULL "
            "AND pipeline_rank IS NOT NULL "
            "AND tensor_rank IS NOT NULL "
            "AND tensor_parallel_size IS NOT NULL "
            "AND pipeline_parallel_size IS NOT NULL "
            "AND tensor_keys_digest IS NOT NULL "
            "AND tensor_parallel_size = 1 "
            "AND tensor_rank = 0 "
            "AND artifact_rank = pipeline_rank "
            "AND pipeline_rank >= 0 "
            "AND pipeline_rank < pipeline_parallel_size "
            "AND pipeline_parallel_size >= 1 "
            "AND pipeline_parallel_size <= 64)",
            name="ck_node_artifact_cache_identity_shape",
        ),
        CheckConstraint(
            "status IN ('READY', 'STALE', 'MISSING', 'CORRUPT', "
            "'QUARANTINED')",
            name="ck_node_artifact_cache_status",
        ),
        CheckConstraint(
            "(status = 'READY' AND reason_code = 'PREPARATION_SUCCEEDED') OR "
            "(status = 'STALE' AND reason_code IN ("
            "'PROBE_IDENTITY_MISMATCH', 'VARIANT_REVOKED', "
            "'QUARANTINE_REQUESTED', 'QUARANTINE_FAILED')) OR "
            "(status = 'MISSING' AND reason_code = 'PROBE_MISSING') OR "
            "(status = 'CORRUPT' AND reason_code IN ("
            "'PROBE_UNSAFE', 'PROBE_CORRUPT', 'VERIFICATION_FAILED')) OR "
            "(status = 'QUARANTINED' "
            "AND reason_code = 'QUARANTINE_SUCCEEDED')",
            name="ck_node_artifact_cache_status_reason",
        ),
        CheckConstraint(
            "verification_version IS NULL OR verification_version = 1",
            name="ck_node_artifact_cache_verification_version",
        ),
        CheckConstraint(
            "verified_size_bytes IS NULL OR verified_size_bytes > 0",
            name="ck_node_artifact_cache_verified_size_positive",
        ),
        CheckConstraint(
            "verified_file_count IS NULL OR verified_file_count > 0",
            name="ck_node_artifact_cache_verified_files_positive",
        ),
        CheckConstraint(
            "(last_ready_attempt_id IS NULL AND verified_at IS NULL "
            "AND verified_size_bytes IS NULL AND verified_file_count IS NULL "
            "AND verification_version IS NULL) OR "
            "(last_ready_attempt_id IS NOT NULL AND verified_at IS NOT NULL "
            "AND verified_size_bytes IS NOT NULL "
            "AND verified_file_count IS NOT NULL "
            "AND verification_version IS NOT NULL)",
            name="ck_node_artifact_cache_verification_shape",
        ),
        CheckConstraint(
            "status <> 'READY' OR last_ready_attempt_id IS NOT NULL",
            name="ck_node_artifact_cache_ready_evidence",
        ),
        CheckConstraint(
            "quarantine_request_id IS NULL OR length(quarantine_request_id) = 36",
            name="ck_node_artifact_cache_quarantine_request_length",
        ),
        CheckConstraint(
            "(status = 'QUARANTINED' AND quarantined_at IS NOT NULL "
            "AND quarantine_request_id IS NULL) OR "
            "(status <> 'QUARANTINED' AND quarantined_at IS NULL)",
            name="ck_node_artifact_cache_quarantine_shape",
        ),
        CheckConstraint(
            "event_sequence >= 0",
            name="ck_node_artifact_cache_event_sequence_nonnegative",
        ),
        ForeignKeyConstraint(
            ["artifact_set_digest", "source_manifest_digest"],
            [
                "stage_artifact_variants.artifact_set_digest",
                "stage_artifact_variants.source_manifest_digest",
            ],
            name="fk_node_artifact_cache_stage_source",
        ),
        ForeignKeyConstraint(
            [
                "artifact_set_digest",
                "tensor_parallel_size",
                "pipeline_parallel_size",
            ],
            [
                "stage_artifact_variants.artifact_set_digest",
                "stage_artifact_variants.tensor_parallel_size",
                "stage_artifact_variants.pipeline_parallel_size",
            ],
            name="fk_node_artifact_cache_stage_topology",
        ),
        ForeignKeyConstraint(
            [
                "artifact_set_digest",
                "artifact_rank",
                "manifest_digest",
                "tensor_keys_digest",
            ],
            [
                "stage_artifact_ranks.variant_id",
                "stage_artifact_ranks.rank",
                "stage_artifact_ranks.manifest_digest",
                "stage_artifact_ranks.tensor_keys_digest",
            ],
            name="fk_node_artifact_cache_stage_rank",
        ),
        UniqueConstraint(
            "node_id",
            "cache_identity_digest",
            name="uq_node_artifact_caches_node_identity",
        ),
        UniqueConstraint(
            "last_ready_attempt_id",
            name="uq_node_artifact_caches_ready_attempt",
        ),
        Index(
            "ix_node_artifact_caches_node_status",
            "node_id",
            "status",
        ),
        Index(
            "ix_node_artifact_caches_manifest_status",
            "manifest_digest",
            "status",
        ),
        Index(
            "ix_node_artifact_caches_variant_status",
            "artifact_set_digest",
            "status",
        ),
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    node_id: Mapped[str] = mapped_column(
        ForeignKey("nodes.id", name="fk_node_artifact_caches_node_id"),
        nullable=False,
    )
    cache_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    cache_identity_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    manifest_digest: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_manifests.digest",
            name="fk_node_artifact_caches_manifest_digest",
        ),
        nullable=False,
    )
    source_manifest_digest: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_manifests.digest",
            name="fk_node_artifact_caches_source_manifest_digest",
        ),
        nullable=False,
    )
    artifact_set_digest: Mapped[str | None] = mapped_column(String(71))
    artifact_rank: Mapped[int | None] = mapped_column(Integer)
    pipeline_rank: Mapped[int | None] = mapped_column(Integer)
    tensor_rank: Mapped[int | None] = mapped_column(Integer)
    tensor_parallel_size: Mapped[int | None] = mapped_column(Integer)
    pipeline_parallel_size: Mapped[int | None] = mapped_column(Integer)
    tensor_keys_digest: Mapped[str | None] = mapped_column(String(71))
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    last_ready_attempt_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "artifact_preparation_attempts.id",
            name="fk_node_artifact_caches_ready_attempt_id",
        )
    )
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    verified_size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    verified_file_count: Mapped[int | None] = mapped_column(Integer)
    verification_version: Mapped[int | None] = mapped_column(Integer)
    last_probe_observed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    quarantine_request_id: Mapped[str | None] = mapped_column(String(36))
    quarantined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    event_sequence: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class ArtifactCacheEvent(Base):
    __tablename__ = "artifact_cache_events"
    __table_args__ = (
        CheckConstraint(
            "length(id) = 36",
            name="ck_artifact_cache_event_id_length",
        ),
        CheckConstraint(
            "sequence > 0",
            name="ck_artifact_cache_event_sequence_positive",
        ),
        CheckConstraint(
            "(sequence = 1 AND previous_status IS NULL) OR "
            "(sequence > 1 AND previous_status IS NOT NULL)",
            name="ck_artifact_cache_event_previous_status",
        ),
        CheckConstraint(
            "previous_status IS NULL OR previous_status IN ("
            "'READY', 'STALE', 'MISSING', 'CORRUPT', 'QUARANTINED')",
            name="ck_artifact_cache_event_previous_status_value",
        ),
        CheckConstraint(
            "status IN ('READY', 'STALE', 'MISSING', 'CORRUPT', "
            "'QUARANTINED')",
            name="ck_artifact_cache_event_status",
        ),
        CheckConstraint(
            "reason_code IN ("
            "'PREPARATION_SUCCEEDED', 'PROBE_UNSAFE', 'PROBE_CORRUPT', "
            "'PROBE_IDENTITY_MISMATCH', 'PROBE_MISSING', "
            "'VARIANT_REVOKED', 'VERIFICATION_FAILED', "
            "'QUARANTINE_REQUESTED', 'QUARANTINE_SUCCEEDED', "
            "'QUARANTINE_FAILED')",
            name="ck_artifact_cache_event_reason",
        ),
        CheckConstraint(
            "source_kind IN ("
            "'PREPARATION', 'PROBE', 'VARIANT', 'VERIFICATION', "
            "'QUARANTINE')",
            name="ck_artifact_cache_event_source_kind",
        ),
        CheckConstraint(
            "length(source_id) > 0 AND length(source_id) <= 255",
            name="ck_artifact_cache_event_source_id",
        ),
        CheckConstraint(
            "evidence_kind IN ("
            "'PREPARATION_RESULT', 'PROBE_OBSERVATION', "
            "'STAGE_VARIANT_STATUS', 'RUNTIME_VERIFICATION', "
            "'QUARANTINE_REQUEST', 'QUARANTINE_RESULT')",
            name="ck_artifact_cache_event_evidence_kind",
        ),
        CheckConstraint(
            "length(evidence_digest) = 71 "
            "AND evidence_digest LIKE 'sha256:%'",
            name="ck_artifact_cache_event_evidence_sha256",
        ),
        CheckConstraint(
            "(source_kind = 'PREPARATION' "
            "AND reason_code = 'PREPARATION_SUCCEEDED' "
            "AND source_attempt_id IS NOT NULL "
            "AND source_task_id IS NOT NULL "
            "AND evidence_kind = 'PREPARATION_RESULT') OR "
            "(source_kind = 'PROBE' "
            "AND reason_code IN ("
            "'PROBE_UNSAFE', 'PROBE_CORRUPT', "
            "'PROBE_IDENTITY_MISMATCH', 'PROBE_MISSING') "
            "AND source_attempt_id IS NULL "
            "AND evidence_kind = 'PROBE_OBSERVATION') OR "
            "(source_kind = 'VARIANT' "
            "AND reason_code = 'VARIANT_REVOKED' "
            "AND source_attempt_id IS NULL "
            "AND source_task_id IS NULL "
            "AND evidence_kind = 'STAGE_VARIANT_STATUS') OR "
            "(source_kind = 'VERIFICATION' "
            "AND reason_code = 'VERIFICATION_FAILED' "
            "AND source_attempt_id IS NULL "
            "AND evidence_kind = 'RUNTIME_VERIFICATION') OR "
            "(source_kind = 'QUARANTINE' "
            "AND reason_code = 'QUARANTINE_REQUESTED' "
            "AND source_attempt_id IS NULL "
            "AND evidence_kind = 'QUARANTINE_REQUEST') OR "
            "(source_kind = 'QUARANTINE' "
            "AND reason_code IN ("
            "'QUARANTINE_SUCCEEDED', 'QUARANTINE_FAILED') "
            "AND source_attempt_id IS NULL "
            "AND evidence_kind = 'QUARANTINE_RESULT')",
            name="ck_artifact_cache_event_closed_source",
        ),
        UniqueConstraint(
            "cache_id",
            "sequence",
            name="uq_artifact_cache_events_cache_sequence",
        ),
        UniqueConstraint(
            "cache_id",
            "source_kind",
            "source_id",
            "reason_code",
            name="uq_artifact_cache_events_source_replay",
        ),
        Index(
            "ix_artifact_cache_events_cache_created",
            "cache_id",
            "created_at",
        ),
        Index(
            "ix_artifact_cache_events_source_task",
            "source_task_id",
        ),
        # Eliminate SQLite's hidden rowid replacement key.  Together with the
        # explicit replay-key INSERT guard, this prevents INSERT OR REPLACE
        # from bypassing append-only DELETE protection on external SQLite
        # connections where recursive_triggers may remain disabled.
        {"sqlite_with_rowid": False},
    )
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    cache_id: Mapped[str] = mapped_column(
        ForeignKey(
            "node_artifact_caches.id",
            name="fk_artifact_cache_events_cache_id",
        ),
        nullable=False,
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    previous_status: Mapped[str | None] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    source_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_attempt_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "artifact_preparation_attempts.id",
            name="fk_artifact_cache_events_source_attempt_id",
        )
    )
    source_task_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "tasks.id",
            name="fk_artifact_cache_events_source_task_id",
        )
    )
    evidence_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    evidence_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )


_ARTIFACT_CACHE_EVENT_APPEND_ONLY_MESSAGE = (
    "artifact_cache_events is append-only"
)
_ARTIFACT_CACHE_EVENT_POSTGRESQL_GUARD_FUNCTION = (
    "dure_artifact_cache_events_append_only_guard"
)


def _register_artifact_cache_event_append_only_ddl() -> None:
    """Install the same database guard for metadata-created test databases."""

    table = ArtifactCacheEvent.__table__
    for operation in ("UPDATE", "DELETE"):
        trigger_name = f"trg_artifact_cache_events_no_{operation.lower()}"
        event.listen(
            table,
            "after_create",
            DDL(
                f"""
CREATE TRIGGER {trigger_name}
BEFORE {operation} ON artifact_cache_events
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, '{_ARTIFACT_CACHE_EVENT_APPEND_ONLY_MESSAGE}');
END
"""
            ).execute_if(dialect="sqlite"),
        )
    event.listen(
        table,
        "after_create",
        DDL(
            f"""
CREATE TRIGGER trg_artifact_cache_events_no_replace
BEFORE INSERT ON artifact_cache_events
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM artifact_cache_events AS existing
    WHERE existing.id = NEW.id
       OR (existing.cache_id = NEW.cache_id
           AND existing.sequence = NEW.sequence)
       OR (existing.cache_id = NEW.cache_id
           AND existing.source_kind = NEW.source_kind
           AND existing.source_id = NEW.source_id
           AND existing.reason_code = NEW.reason_code)
)
BEGIN
    SELECT RAISE(ABORT, '{_ARTIFACT_CACHE_EVENT_APPEND_ONLY_MESSAGE}');
END
"""
        ).execute_if(dialect="sqlite"),
    )

    event.listen(
        table,
        "after_create",
        DDL(
            f"""
CREATE OR REPLACE FUNCTION {_ARTIFACT_CACHE_EVENT_POSTGRESQL_GUARD_FUNCTION}()
RETURNS trigger
LANGUAGE plpgsql
AS $dure$
BEGIN
    RAISE EXCEPTION '{_ARTIFACT_CACHE_EVENT_APPEND_ONLY_MESSAGE}'
        USING ERRCODE = '23514';
END;
$dure$
"""
        ).execute_if(dialect="postgresql"),
    )
    for operation in ("UPDATE", "DELETE"):
        trigger_name = f"trg_artifact_cache_events_no_{operation.lower()}"
        event.listen(
            table,
            "after_create",
            DDL(
                f"""
CREATE TRIGGER {trigger_name}
BEFORE {operation} ON artifact_cache_events
FOR EACH ROW
EXECUTE FUNCTION {_ARTIFACT_CACHE_EVENT_POSTGRESQL_GUARD_FUNCTION}()
"""
            ).execute_if(dialect="postgresql"),
        )
    event.listen(
        table,
        "after_create",
        DDL(
            f"""
CREATE TRIGGER trg_artifact_cache_events_no_truncate
BEFORE TRUNCATE ON artifact_cache_events
FOR EACH STATEMENT
EXECUTE FUNCTION {_ARTIFACT_CACHE_EVENT_POSTGRESQL_GUARD_FUNCTION}()
"""
        ).execute_if(dialect="postgresql"),
    )
    event.listen(
        table,
        "after_drop",
        DDL(
            "DROP FUNCTION IF EXISTS "
            f"{_ARTIFACT_CACHE_EVENT_POSTGRESQL_GUARD_FUNCTION}()"
        ).execute_if(dialect="postgresql"),
    )


_register_artifact_cache_event_append_only_ddl()


class BenchmarkRun(Base):
    __tablename__ = "benchmark_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PREPARED', 'QUEUED', 'SUCCEEDED', 'FAILED')",
            name="ck_benchmark_run_status",
        ),
        CheckConstraint(
            "workload_id IN ('short-chat-1k-128', 'long-chat-4k-256', "
            "'max-context', 'quality-eval')",
            name="ck_benchmark_run_workload",
        ),
        CheckConstraint("input_tokens > 0", name="ck_benchmark_run_input_positive"),
        CheckConstraint("output_tokens > 0", name="ck_benchmark_run_output_positive"),
        CheckConstraint("concurrency > 0", name="ck_benchmark_run_concurrency_positive"),
        CheckConstraint("warmup_requests >= 0", name="ck_benchmark_run_warmup_nonnegative"),
        CheckConstraint("request_count > 0", name="ck_benchmark_run_requests_positive"),
        CheckConstraint("duration_seconds > 0", name="ck_benchmark_run_duration_positive"),
        CheckConstraint(
            "length(inventory_fingerprint) = 71 AND inventory_fingerprint LIKE 'sha256:%'",
            name="ck_benchmark_run_inventory_fingerprint",
        ),
        CheckConstraint(
            "length(request_digest) = 71 AND request_digest LIKE 'sha256:%'",
            name="ck_benchmark_run_request_digest",
        ),
        CheckConstraint(
            "failure_code IS NULL OR failure_code IN ("
            "'BENCHMARK_EXECUTION_FAILED', 'BENCHMARK_PAYLOAD_REJECTED', "
            "'BENCHMARK_RUNTIME_UNAVAILABLE', 'BENCHMARK_ARTIFACT_UNAVAILABLE', "
            "'BENCHMARK_EVIDENCE_REJECTED', 'BENCHMARK_CANCELED')",
            name="ck_benchmark_run_failure_code",
        ),
        Index("ix_benchmark_runs_request_digest", "request_digest"),
        Index("ix_benchmark_runs_release_id", "release_id"),
        Index("ix_benchmark_runs_status", "status"),
        Index("ix_benchmark_runs_coordinator_node_id", "coordinator_node_id"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    request_id: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    request_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    release_id: Mapped[str] = mapped_column(ForeignKey("model_releases.id"), nullable=False)
    placement_id: Mapped[str] = mapped_column(ForeignKey("placement_profiles.id"), nullable=False)
    coordinator_node_id: Mapped[str] = mapped_column(ForeignKey("nodes.id"), nullable=False)
    node_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    inventory_fingerprint: Mapped[str] = mapped_column(String(71), nullable=False)
    suite_id: Mapped[str] = mapped_column(String(100), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    workload_id: Mapped[str] = mapped_column(String(64), nullable=False)
    dure_commit: Mapped[str] = mapped_column(String(64), nullable=False)
    model_id: Mapped[str] = mapped_column(String(100), nullable=False)
    repository: Mapped[str] = mapped_column(String(255), nullable=False)
    artifact_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_manifest_digest: Mapped[str] = mapped_column(String(71), nullable=False)
    quantization: Mapped[str] = mapped_column(String(40), nullable=False)
    runtime_image: Mapped[str] = mapped_column(String(512), nullable=False)
    input_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    concurrency: Mapped[int] = mapped_column(Integer, nullable=False)
    warmup_requests: Mapped[int] = mapped_column(Integer, nullable=False)
    request_count: Mapped[int] = mapped_column(Integer, nullable=False)
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="PREPARED", nullable=False)
    task_id: Mapped[str | None] = mapped_column(ForeignKey("tasks.id"), unique=True)
    evidence_id: Mapped[str | None] = mapped_column(
        ForeignKey("benchmark_evidence.id"), unique=True
    )
    failure_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class EnrollmentToken(Base):
    __tablename__ = "enrollment_tokens"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class NodeCredential(Base):
    __tablename__ = "node_credentials"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    node_id: Mapped[str] = mapped_column(ForeignKey("nodes.id", ondelete="CASCADE"), nullable=False)
    credential_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AuditEvent(Base):
    __tablename__ = "audit_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    actor: Mapped[str] = mapped_column(String(255), nullable=False)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    target: Mapped[str | None] = mapped_column(String(255))
    outcome: Mapped[str] = mapped_column(String(40), nullable=False)
    detail: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
