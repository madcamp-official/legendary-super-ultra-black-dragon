"""Add prepared benchmark runs.

Revision ID: 0004
Revises: 0003
"""

from alembic import op
import sqlalchemy as sa


revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def _tables() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def upgrade() -> None:
    if "benchmark_runs" not in _tables():
        _create_benchmark_runs_table()

    evidence_columns = {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns("benchmark_evidence")
    }
    if "benchmark_run_id" not in evidence_columns:
        with op.batch_alter_table("benchmark_evidence") as batch:
            batch.add_column(sa.Column("benchmark_run_id", sa.String(length=36)))
            batch.create_check_constraint(
                "ck_benchmark_evidence_run_id_length",
                "benchmark_run_id IS NULL OR length(benchmark_run_id) = 36",
            )
    evidence_indexes = {
        index["name"]
        for index in sa.inspect(op.get_bind()).get_indexes("benchmark_evidence")
    }
    if "ux_benchmark_evidence_benchmark_run_id" not in evidence_indexes:
        op.create_index(
            "ux_benchmark_evidence_benchmark_run_id",
            "benchmark_evidence",
            ["benchmark_run_id"],
            unique=True,
        )


def _create_benchmark_runs_table() -> None:
    op.create_table(
        "benchmark_runs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("request_id", sa.String(length=36), nullable=False, unique=True),
        sa.Column("request_digest", sa.String(length=71), nullable=False),
        sa.Column("release_id", sa.String(length=36), nullable=False),
        sa.Column("placement_id", sa.String(length=36), nullable=False),
        sa.Column("coordinator_node_id", sa.String(length=36), nullable=False),
        sa.Column("node_ids", sa.JSON(), nullable=False),
        sa.Column("inventory_fingerprint", sa.String(length=71), nullable=False),
        sa.Column("suite_id", sa.String(length=100), nullable=False),
        sa.Column("policy_version", sa.String(length=64), nullable=False),
        sa.Column("workload_id", sa.String(length=64), nullable=False),
        sa.Column("dure_commit", sa.String(length=64), nullable=False),
        sa.Column("model_id", sa.String(length=100), nullable=False),
        sa.Column("repository", sa.String(length=255), nullable=False),
        sa.Column("artifact_revision", sa.String(length=64), nullable=False),
        sa.Column("artifact_manifest_digest", sa.String(length=71), nullable=False),
        sa.Column("quantization", sa.String(length=40), nullable=False),
        sa.Column("runtime_image", sa.String(length=512), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        sa.Column("concurrency", sa.Integer(), nullable=False),
        sa.Column("warmup_requests", sa.Integer(), nullable=False),
        sa.Column("request_count", sa.Integer(), nullable=False),
        sa.Column("duration_seconds", sa.Float(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("task_id", sa.String(length=36), unique=True),
        sa.Column("evidence_id", sa.String(length=36), unique=True),
        sa.Column("failure_code", sa.String(length=64)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('PREPARED', 'QUEUED', 'SUCCEEDED', 'FAILED')",
            name="ck_benchmark_run_status",
        ),
        sa.CheckConstraint(
            "workload_id IN ('short-chat-1k-128', 'long-chat-4k-256', "
            "'max-context', 'quality-eval')",
            name="ck_benchmark_run_workload",
        ),
        sa.CheckConstraint(
            "input_tokens > 0", name="ck_benchmark_run_input_positive"
        ),
        sa.CheckConstraint(
            "output_tokens > 0", name="ck_benchmark_run_output_positive"
        ),
        sa.CheckConstraint(
            "concurrency > 0", name="ck_benchmark_run_concurrency_positive"
        ),
        sa.CheckConstraint(
            "warmup_requests >= 0", name="ck_benchmark_run_warmup_nonnegative"
        ),
        sa.CheckConstraint(
            "request_count > 0", name="ck_benchmark_run_requests_positive"
        ),
        sa.CheckConstraint(
            "duration_seconds > 0", name="ck_benchmark_run_duration_positive"
        ),
        sa.CheckConstraint(
            "length(inventory_fingerprint) = 71 "
            "AND inventory_fingerprint LIKE 'sha256:%'",
            name="ck_benchmark_run_inventory_fingerprint",
        ),
        sa.CheckConstraint(
            "length(request_digest) = 71 AND request_digest LIKE 'sha256:%'",
            name="ck_benchmark_run_request_digest",
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR failure_code IN ("
            "'BENCHMARK_EXECUTION_FAILED', 'BENCHMARK_PAYLOAD_REJECTED', "
            "'BENCHMARK_RUNTIME_UNAVAILABLE', 'BENCHMARK_ARTIFACT_UNAVAILABLE', "
            "'BENCHMARK_EVIDENCE_REJECTED', 'BENCHMARK_CANCELED')",
            name="ck_benchmark_run_failure_code",
        ),
        sa.ForeignKeyConstraint(["release_id"], ["model_releases.id"]),
        sa.ForeignKeyConstraint(["placement_id"], ["placement_profiles.id"]),
        sa.ForeignKeyConstraint(["coordinator_node_id"], ["nodes.id"]),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.ForeignKeyConstraint(["evidence_id"], ["benchmark_evidence.id"]),
    )
    op.create_index(
        "ix_benchmark_runs_request_digest", "benchmark_runs", ["request_digest"]
    )
    op.create_index(
        "ix_benchmark_runs_release_id", "benchmark_runs", ["release_id"]
    )
    op.create_index("ix_benchmark_runs_status", "benchmark_runs", ["status"])
    op.create_index(
        "ix_benchmark_runs_coordinator_node_id",
        "benchmark_runs",
        ["coordinator_node_id"],
    )


def downgrade() -> None:
    if "benchmark_evidence" in _tables():
        evidence_indexes = {
            index["name"]
            for index in sa.inspect(op.get_bind()).get_indexes(
                "benchmark_evidence"
            )
        }
        if "ux_benchmark_evidence_benchmark_run_id" in evidence_indexes:
            op.drop_index(
                "ux_benchmark_evidence_benchmark_run_id",
                table_name="benchmark_evidence",
            )
        evidence_columns = {
            column["name"]
            for column in sa.inspect(op.get_bind()).get_columns(
                "benchmark_evidence"
            )
        }
        if "benchmark_run_id" in evidence_columns:
            evidence_checks = {
                check["name"]
                for check in sa.inspect(op.get_bind()).get_check_constraints(
                    "benchmark_evidence"
                )
            }
            with op.batch_alter_table("benchmark_evidence") as batch:
                if "ck_benchmark_evidence_run_id_length" in evidence_checks:
                    batch.drop_constraint(
                        "ck_benchmark_evidence_run_id_length", type_="check"
                    )
                batch.drop_column("benchmark_run_id")
    if "benchmark_runs" in _tables():
        op.drop_table("benchmark_runs")
