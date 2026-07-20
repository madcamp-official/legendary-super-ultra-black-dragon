"""Add the model registry and placement policy schema.

Revision ID: 0002
Revises: 0001
"""

from alembic import op
import sqlalchemy as sa


revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def _tables() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def upgrade() -> None:
    tables = _tables()
    if "model_artifacts" not in tables:
        op.create_table(
            "model_artifacts",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("model_id", sa.String(length=100), nullable=False),
            sa.Column("repository", sa.String(length=255), nullable=False),
            sa.Column("revision", sa.String(length=64), nullable=False),
            sa.Column("manifest_digest", sa.String(length=71), nullable=False, unique=True),
            sa.Column("quantization", sa.String(length=40), nullable=False),
            sa.Column("size_mib", sa.Integer(), nullable=False),
            sa.Column("default_max_model_len", sa.Integer(), nullable=False),
            sa.Column("layer_count", sa.Integer(), nullable=False),
            sa.Column("license_id", sa.String(length=100), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.CheckConstraint("size_mib > 0", name="ck_model_artifact_size_positive"),
            sa.CheckConstraint(
                "default_max_model_len > 0", name="ck_model_artifact_context_positive"
            ),
            sa.CheckConstraint("layer_count > 0", name="ck_model_artifact_layers_positive"),
            sa.UniqueConstraint("repository", "revision", "quantization"),
        )
        op.create_index("ix_model_artifacts_model_id", "model_artifacts", ["model_id"])

    if "runtime_releases" not in tables:
        op.create_table(
            "runtime_releases",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("version", sa.String(length=64), nullable=False),
            sa.Column("image", sa.String(length=512), nullable=False, unique=True),
            sa.Column("vllm_version", sa.String(length=64), nullable=False),
            sa.Column("cuda_version", sa.String(length=64), nullable=False),
            sa.Column("gpu_architectures", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        )

    if "model_releases" not in tables:
        op.create_table(
            "model_releases",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("artifact_id", sa.String(length=36), nullable=False),
            sa.Column("runtime_id", sa.String(length=36), nullable=False),
            sa.Column("status", sa.String(length=20), nullable=False),
            sa.Column("quality_rank", sa.Integer(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.CheckConstraint("quality_rank > 0", name="ck_model_release_quality_positive"),
            sa.CheckConstraint(
                "status IN ('DRAFT', 'VALIDATED', 'ACTIVE', 'DEPRECATED', 'REVOKED')",
                name="ck_model_release_status",
            ),
            sa.ForeignKeyConstraint(["artifact_id"], ["model_artifacts.id"]),
            sa.ForeignKeyConstraint(["runtime_id"], ["runtime_releases.id"]),
            sa.UniqueConstraint("artifact_id", "runtime_id"),
        )
        op.create_index("ix_model_releases_status", "model_releases", ["status"])

    if "placement_profiles" not in tables:
        op.create_table(
            "placement_profiles",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("release_id", sa.String(length=36), nullable=False),
            sa.Column("profile_id", sa.String(length=100), nullable=False),
            sa.Column("topology", sa.String(length=30), nullable=False),
            sa.Column("node_count", sa.Integer(), nullable=False),
            sa.Column("min_gpu_memory_mib", sa.Integer(), nullable=False),
            sa.Column("min_disk_free_mib", sa.Integer(), nullable=False),
            sa.Column("pipeline_parallel_size", sa.Integer(), nullable=False),
            sa.Column("tensor_parallel_size", sa.Integer(), nullable=False),
            sa.Column("requires_network_evidence", sa.Boolean(), nullable=False),
            sa.Column("requires_nccl", sa.Boolean(), nullable=False),
            sa.Column("min_bandwidth_mbps", sa.Integer()),
            sa.Column("max_rtt_ms", sa.Float()),
            sa.Column("max_packet_loss_pct", sa.Float()),
            sa.Column("max_ttft_p95_ms", sa.Float(), nullable=False),
            sa.Column("max_tpot_p95_ms", sa.Float(), nullable=False),
            sa.Column("max_e2e_p95_ms", sa.Float(), nullable=False),
            sa.Column("min_success_rate", sa.Float(), nullable=False),
            sa.Column("min_vram_headroom_pct", sa.Float(), nullable=False),
            sa.Column("min_throughput_tps", sa.Float(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.CheckConstraint("node_count > 0", name="ck_placement_node_count_positive"),
            sa.CheckConstraint(
                "min_gpu_memory_mib > 0", name="ck_placement_vram_positive"
            ),
            sa.CheckConstraint("min_disk_free_mib > 0", name="ck_placement_disk_positive"),
            sa.CheckConstraint(
                "pipeline_parallel_size > 0", name="ck_placement_pp_positive"
            ),
            sa.CheckConstraint(
                "tensor_parallel_size > 0", name="ck_placement_tp_positive"
            ),
            sa.CheckConstraint(
                "max_packet_loss_pct IS NULL OR "
                "(max_packet_loss_pct >= 0 AND max_packet_loss_pct <= 100)",
                name="ck_placement_packet_loss_range",
            ),
            sa.CheckConstraint(
                "min_success_rate >= 0 AND min_success_rate <= 1",
                name="ck_placement_success_rate_range",
            ),
            sa.CheckConstraint(
                "min_vram_headroom_pct >= 0 AND min_vram_headroom_pct <= 100",
                name="ck_placement_vram_headroom_range",
            ),
            sa.CheckConstraint("max_ttft_p95_ms > 0", name="ck_placement_ttft_positive"),
            sa.CheckConstraint("max_tpot_p95_ms > 0", name="ck_placement_tpot_positive"),
            sa.CheckConstraint("max_e2e_p95_ms > 0", name="ck_placement_e2e_positive"),
            sa.CheckConstraint(
                "min_throughput_tps > 0", name="ck_placement_throughput_positive"
            ),
            sa.CheckConstraint(
                "min_bandwidth_mbps IS NULL OR min_bandwidth_mbps > 0",
                name="ck_placement_bandwidth_positive",
            ),
            sa.CheckConstraint(
                "max_rtt_ms IS NULL OR max_rtt_ms >= 0",
                name="ck_placement_rtt_nonnegative",
            ),
            sa.ForeignKeyConstraint(
                ["release_id"], ["model_releases.id"], ondelete="CASCADE"
            ),
            sa.UniqueConstraint("release_id", "profile_id"),
        )


def downgrade() -> None:
    tables = _tables()
    for table in (
        "placement_profiles",
        "model_releases",
        "runtime_releases",
        "model_artifacts",
    ):
        if table in tables:
            op.drop_table(table)
