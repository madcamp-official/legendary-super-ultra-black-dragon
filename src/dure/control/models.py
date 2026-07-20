from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
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
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base
from ..task import TaskStatus, TaskType


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _deployment_lineage_default(context: Any) -> str:
    """Keep legacy/manual deployments in a one-record lineage by default."""
    return str(context.get_current_parameters()["id"])


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
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    plan: Mapped[dict] = mapped_column(JSON, nullable=False)
    accept_model_download: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    pull_image: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    status: Mapped[str] = mapped_column(String(40), default="CREATED", nullable=False)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


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
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


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
