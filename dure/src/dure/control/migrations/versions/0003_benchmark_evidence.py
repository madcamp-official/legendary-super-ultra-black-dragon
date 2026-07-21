"""Add immutable benchmark evidence.

Revision ID: 0003
Revises: 0002
"""

from alembic import op
import sqlalchemy as sa


revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def _tables() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def upgrade() -> None:
    if "benchmark_evidence" not in _tables():
        _create_benchmark_evidence_table()

    model_release_columns = {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns("model_releases")
    }
    with op.batch_alter_table("model_releases") as batch:
        if "promotion_evidence_ids" not in model_release_columns:
            batch.add_column(sa.Column("promotion_evidence_ids", sa.JSON()))
        if "promotion_evidence_digest" not in model_release_columns:
            batch.add_column(
                sa.Column("promotion_evidence_digest", sa.String(length=71))
            )

    # 0002 allowed ACTIVE without benchmark evidence. Keeping those rows ACTIVE
    # would bypass the new gate, so require an explicit evidence-backed
    # requalification after upgrade.
    op.execute(
        sa.text(
            "UPDATE model_releases "
            "SET status = 'VALIDATED', updated_at = CURRENT_TIMESTAMP "
            "WHERE status = 'ACTIVE' "
            "AND (promotion_evidence_ids IS NULL "
            "OR promotion_evidence_digest IS NULL)"
        )
    )


def _create_benchmark_evidence_table() -> None:
    op.create_table(
        "benchmark_evidence",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("release_id", sa.String(length=36), nullable=False),
        sa.Column("placement_id", sa.String(length=36), nullable=False),
        sa.Column("registration_sequence", sa.Integer(), nullable=False),
        sa.Column("suite_id", sa.String(length=100), nullable=False),
        sa.Column("node_ids", sa.JSON(), nullable=False),
        sa.Column("inventory_fingerprint", sa.String(length=71), nullable=False),
        sa.Column("artifact_revision", sa.String(length=64), nullable=False),
        sa.Column("artifact_manifest_digest", sa.String(length=71), nullable=False),
        sa.Column("runtime_image", sa.String(length=512), nullable=False),
        sa.Column("dure_commit", sa.String(length=64), nullable=False),
        sa.Column("policy_version", sa.String(length=64), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        sa.Column("concurrency", sa.Integer(), nullable=False),
        sa.Column("warmup_requests", sa.Integer(), nullable=False),
        sa.Column("request_count", sa.Integer(), nullable=False),
        sa.Column("duration_seconds", sa.Float(), nullable=False),
        sa.Column("oom_count", sa.Integer(), nullable=False),
        sa.Column("crash_count", sa.Integer(), nullable=False),
        sa.Column("restart_count", sa.Integer(), nullable=False),
        sa.Column("ttft_p95_ms", sa.Float()),
        sa.Column("tpot_p95_ms", sa.Float()),
        sa.Column("e2e_p95_ms", sa.Float()),
        sa.Column("throughput_tps", sa.Float()),
        sa.Column("success_rate", sa.Float(), nullable=False),
        sa.Column("vram_headroom_pct", sa.Float(), nullable=False),
        sa.Column("quality_score", sa.Float(), nullable=False),
        sa.Column("network_bandwidth_mbps", sa.Float()),
        sa.Column("network_rtt_ms", sa.Float()),
        sa.Column("packet_loss_pct", sa.Float()),
        sa.Column("nccl_all_reduce_ok", sa.Boolean()),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("failure_codes", sa.JSON(), nullable=False),
        sa.Column("evidence_digest", sa.String(length=71), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "request_count > 0", name="ck_benchmark_request_count_positive"
        ),
        sa.CheckConstraint(
            "registration_sequence > 0",
            name="ck_benchmark_registration_sequence_positive",
        ),
        sa.CheckConstraint(
            "duration_seconds > 0", name="ck_benchmark_duration_positive"
        ),
        sa.CheckConstraint(
            "oom_count >= 0",
            name="ck_benchmark_oom_count_nonnegative",
        ),
        sa.CheckConstraint(
            "crash_count >= 0",
            name="ck_benchmark_crash_count_nonnegative",
        ),
        sa.CheckConstraint(
            "restart_count >= 0",
            name="ck_benchmark_restart_count_nonnegative",
        ),
        sa.CheckConstraint(
            "input_tokens > 0", name="ck_benchmark_input_tokens_positive"
        ),
        sa.CheckConstraint(
            "output_tokens > 0", name="ck_benchmark_output_tokens_positive"
        ),
        sa.CheckConstraint(
            "concurrency > 0", name="ck_benchmark_concurrency_positive"
        ),
        sa.CheckConstraint(
            "warmup_requests >= 0", name="ck_benchmark_warmup_nonnegative"
        ),
        sa.CheckConstraint(
            "ttft_p95_ms IS NULL OR ttft_p95_ms > 0",
            name="ck_benchmark_ttft_positive",
        ),
        sa.CheckConstraint(
            "tpot_p95_ms IS NULL OR tpot_p95_ms > 0",
            name="ck_benchmark_tpot_positive",
        ),
        sa.CheckConstraint(
            "e2e_p95_ms IS NULL OR e2e_p95_ms > 0",
            name="ck_benchmark_e2e_positive",
        ),
        sa.CheckConstraint(
            "throughput_tps IS NULL OR throughput_tps > 0",
            name="ck_benchmark_throughput_positive",
        ),
        sa.CheckConstraint(
            "success_rate >= 0 AND success_rate <= 1",
            name="ck_benchmark_success_rate_range",
        ),
        sa.CheckConstraint(
            "vram_headroom_pct >= 0 AND vram_headroom_pct <= 100",
            name="ck_benchmark_vram_headroom_range",
        ),
        sa.CheckConstraint(
            "quality_score >= 0 AND quality_score <= 1",
            name="ck_benchmark_quality_score_range",
        ),
        sa.CheckConstraint(
            "network_bandwidth_mbps IS NULL OR network_bandwidth_mbps > 0",
            name="ck_benchmark_bandwidth_positive",
        ),
        sa.CheckConstraint(
            "network_rtt_ms IS NULL OR network_rtt_ms >= 0",
            name="ck_benchmark_rtt_nonnegative",
        ),
        sa.CheckConstraint(
            "packet_loss_pct IS NULL OR (packet_loss_pct >= 0 AND packet_loss_pct <= 100)",
            name="ck_benchmark_packet_loss_range",
        ),
        sa.CheckConstraint(
            "status IN ('PASSED', 'FAILED')", name="ck_benchmark_status"
        ),
        sa.CheckConstraint(
            "length(inventory_fingerprint) = 71 AND inventory_fingerprint LIKE 'sha256:%'",
            name="ck_benchmark_inventory_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            "length(evidence_digest) = 71 AND evidence_digest LIKE 'sha256:%'",
            name="ck_benchmark_evidence_digest_sha256",
        ),
        sa.ForeignKeyConstraint(["release_id"], ["model_releases.id"]),
        sa.ForeignKeyConstraint(["placement_id"], ["placement_profiles.id"]),
        sa.UniqueConstraint("placement_id", "registration_sequence"),
    )
    op.create_index(
        "ix_benchmark_evidence_release_id", "benchmark_evidence", ["release_id"]
    )
    op.create_index(
        "ix_benchmark_evidence_placement_id", "benchmark_evidence", ["placement_id"]
    )
    op.create_index(
        "ix_benchmark_evidence_status", "benchmark_evidence", ["status"]
    )


def downgrade() -> None:
    if "benchmark_evidence" in _tables():
        op.drop_table("benchmark_evidence")
    if "model_releases" in _tables():
        model_release_columns = {
            column["name"]
            for column in sa.inspect(op.get_bind()).get_columns("model_releases")
        }
        with op.batch_alter_table("model_releases") as batch:
            if "promotion_evidence_digest" in model_release_columns:
                batch.drop_column("promotion_evidence_digest")
            if "promotion_evidence_ids" in model_release_columns:
                batch.drop_column("promotion_evidence_ids")
