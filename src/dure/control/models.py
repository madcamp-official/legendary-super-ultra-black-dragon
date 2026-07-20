from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
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


class Deployment(Base):
    __tablename__ = "deployments"
    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    plan: Mapped[dict] = mapped_column(JSON, nullable=False)
    accept_model_download: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    pull_image: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    status: Mapped[str] = mapped_column(String(40), default="CREATED", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ModelArtifact(Base):
    __tablename__ = "model_artifacts"
    __table_args__ = (
        UniqueConstraint("repository", "revision", "quantization"),
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
        Index("ix_model_releases_status", "status"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    artifact_id: Mapped[str] = mapped_column(ForeignKey("model_artifacts.id"), nullable=False)
    runtime_id: Mapped[str] = mapped_column(ForeignKey("runtime_releases.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="DRAFT", nullable=False)
    quality_rank: Mapped[int] = mapped_column(Integer, nullable=False)
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


class Task(Base):
    __tablename__ = "tasks"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    bulk_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    node_id: Mapped[str] = mapped_column(ForeignKey("nodes.id"), nullable=False, index=True)
    type: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default=TaskStatus.QUEUED.value, nullable=False)
    deployment_id: Mapped[str | None] = mapped_column(ForeignKey("deployments.id"))
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[dict | None] = mapped_column(JSON)
    error: Mapped[str | None] = mapped_column(Text)
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
