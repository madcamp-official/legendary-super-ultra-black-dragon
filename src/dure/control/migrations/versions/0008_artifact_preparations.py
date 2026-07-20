"""Track deployment-generation artifact preparation attempts.

Revision ID: 0008
Revises: 0007
"""

from alembic import op
import sqlalchemy as sa


revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def _inspector():
    return sa.inspect(op.get_bind())


def _tables() -> set[str]:
    return set(_inspector().get_table_names())


def upgrade() -> None:
    if "artifact_preparations" not in _tables():
        _create_artifact_preparations()
    if "artifact_preparation_nodes" not in _tables():
        _create_artifact_preparation_nodes()
    if "artifact_preparation_attempts" not in _tables():
        _create_artifact_preparation_attempts()


def _create_artifact_preparations() -> None:
    op.create_table(
        "artifact_preparations",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("request_id", sa.String(length=36), nullable=False),
        sa.Column("request_digest", sa.String(length=71), nullable=False),
        sa.Column("deployment_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("plan_snapshot", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "length(id) = 36",
            name="ck_artifact_preparation_id_length",
        ),
        sa.CheckConstraint(
            "length(request_id) = 36",
            name="ck_artifact_preparation_request_id_length",
        ),
        sa.CheckConstraint(
            "length(request_digest) = 71 AND request_digest LIKE 'sha256:%'",
            name="ck_artifact_preparation_request_digest_sha256",
        ),
        sa.CheckConstraint(
            "status IN ('PREPARED', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'PARTIAL_FAILED', 'FAILED')",
            name="ck_artifact_preparation_status",
        ),
        sa.ForeignKeyConstraint(
            ["deployment_id"],
            ["deployments.id"],
            ondelete="CASCADE",
            name="fk_artifact_preparations_deployment_id",
        ),
        sa.UniqueConstraint(
            "request_id",
            name="uq_artifact_preparations_request_id",
        ),
        sa.UniqueConstraint(
            "request_digest",
            name="uq_artifact_preparations_request_digest",
        ),
        sa.UniqueConstraint(
            "deployment_id",
            name="uq_artifact_preparations_deployment_id",
        ),
    )
    op.create_index(
        "ix_artifact_preparations_status",
        "artifact_preparations",
        ["status"],
    )


def _create_artifact_preparation_nodes() -> None:
    op.create_table(
        "artifact_preparation_nodes",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("preparation_id", sa.String(length=36), nullable=False),
        sa.Column("node_id", sa.String(length=36), nullable=False),
        sa.Column("model_manifest_digest", sa.String(length=71), nullable=False),
        sa.Column("runtime_image", sa.String(length=512), nullable=False),
        sa.Column("model_status", sa.String(length=20), nullable=False),
        sa.Column("image_status", sa.String(length=20), nullable=False),
        sa.Column("model_current_attempt", sa.Integer(), nullable=False),
        sa.Column("image_current_attempt", sa.Integer(), nullable=False),
        sa.Column("model_failure_code", sa.String(length=64)),
        sa.Column("image_failure_code", sa.String(length=64)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(id) = 36",
            name="ck_artifact_preparation_node_id_length",
        ),
        sa.CheckConstraint(
            "length(model_manifest_digest) = 71 "
            "AND model_manifest_digest LIKE 'sha256:%'",
            name="ck_artifact_preparation_node_manifest_digest_sha256",
        ),
        sa.CheckConstraint(
            "runtime_image LIKE '%@sha256:" + "_" * 64 + "'",
            name="ck_artifact_preparation_node_runtime_image_digest",
        ),
        sa.CheckConstraint(
            "model_status IN ('PREPARED', 'QUEUED', 'RUNNING', "
            "'SUCCEEDED', 'FAILED')",
            name="ck_artifact_preparation_node_model_status",
        ),
        sa.CheckConstraint(
            "image_status IN ('PREPARED', 'QUEUED', 'RUNNING', "
            "'SUCCEEDED', 'FAILED')",
            name="ck_artifact_preparation_node_image_status",
        ),
        sa.CheckConstraint(
            "model_current_attempt >= 0",
            name="ck_artifact_preparation_node_model_attempt_nonnegative",
        ),
        sa.CheckConstraint(
            "image_current_attempt >= 0",
            name="ck_artifact_preparation_node_image_attempt_nonnegative",
        ),
        sa.CheckConstraint(
            "(model_status = 'PREPARED' AND model_current_attempt = 0) OR "
            "(model_status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED') "
            "AND model_current_attempt >= 1)",
            name="ck_artifact_preparation_node_model_attempt_status",
        ),
        sa.CheckConstraint(
            "(image_status = 'PREPARED' AND image_current_attempt = 0) OR "
            "(image_status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED') "
            "AND image_current_attempt >= 1)",
            name="ck_artifact_preparation_node_image_attempt_status",
        ),
        sa.CheckConstraint(
            "model_failure_code IS NULL OR "
            "(model_status = 'FAILED' AND length(model_failure_code) > 0 "
            "AND length(model_failure_code) <= 64)",
            name="ck_artifact_preparation_node_model_failure_code",
        ),
        sa.CheckConstraint(
            "image_failure_code IS NULL OR "
            "(image_status = 'FAILED' AND length(image_failure_code) > 0 "
            "AND length(image_failure_code) <= 64)",
            name="ck_artifact_preparation_node_image_failure_code",
        ),
        sa.ForeignKeyConstraint(
            ["preparation_id"],
            ["artifact_preparations.id"],
            ondelete="CASCADE",
            name="fk_artifact_preparation_nodes_preparation_id",
        ),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["nodes.id"],
            name="fk_artifact_preparation_nodes_node_id",
        ),
        sa.ForeignKeyConstraint(
            ["model_manifest_digest"],
            ["artifact_manifests.digest"],
            name="fk_artifact_preparation_nodes_manifest_digest",
        ),
        sa.UniqueConstraint(
            "preparation_id",
            "node_id",
            name="uq_artifact_preparation_nodes_preparation_node",
        ),
    )
    op.create_index(
        "ix_artifact_preparation_nodes_node_id",
        "artifact_preparation_nodes",
        ["node_id"],
    )


def _create_artifact_preparation_attempts() -> None:
    op.create_table(
        "artifact_preparation_attempts",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("preparation_node_id", sa.String(length=36), nullable=False),
        sa.Column("stage", sa.String(length=10), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("failure_code", sa.String(length=64)),
        sa.Column("result", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "length(id) = 36",
            name="ck_artifact_preparation_attempt_id_length",
        ),
        sa.CheckConstraint(
            "stage IN ('MODEL', 'IMAGE')",
            name="ck_artifact_preparation_attempt_stage",
        ),
        sa.CheckConstraint(
            "attempt_no >= 1",
            name="ck_artifact_preparation_attempt_number_positive",
        ),
        sa.CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', "
            "'CANCELED')",
            name="ck_artifact_preparation_attempt_status",
        ),
        sa.CheckConstraint(
            "(status IN ('QUEUED', 'RUNNING') AND completed_at IS NULL) OR "
            "(status IN ('SUCCEEDED', 'FAILED', 'CANCELED') "
            "AND completed_at IS NOT NULL)",
            name="ck_artifact_preparation_attempt_completion",
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR "
            "(status IN ('FAILED', 'CANCELED') AND length(failure_code) > 0 "
            "AND length(failure_code) <= 64)",
            name="ck_artifact_preparation_attempt_failure_code",
        ),
        sa.ForeignKeyConstraint(
            ["preparation_node_id"],
            ["artifact_preparation_nodes.id"],
            ondelete="CASCADE",
            name="fk_artifact_preparation_attempts_preparation_node_id",
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            name="fk_artifact_preparation_attempts_task_id",
        ),
        sa.UniqueConstraint(
            "preparation_node_id",
            "stage",
            "attempt_no",
            name="uq_artifact_preparation_attempts_node_stage_number",
        ),
        sa.UniqueConstraint(
            "task_id",
            name="uq_artifact_preparation_attempts_task_id",
        ),
    )
    op.create_index(
        "ix_artifact_preparation_attempts_node_stage_status",
        "artifact_preparation_attempts",
        ["preparation_node_id", "stage", "status"],
    )


def _scalar_count(table: str) -> int:
    return int(
        op.get_bind()
        .execute(sa.text(f"SELECT COUNT(*) FROM {table}"))
        .scalar_one()
    )


def _refuse_destructive_downgrade() -> None:
    tables = _tables()
    populated = [
        table
        for table in (
            "artifact_preparation_attempts",
            "artifact_preparation_nodes",
            "artifact_preparations",
        )
        if table in tables and _scalar_count(table) > 0
    ]
    if populated:
        raise RuntimeError(
            "refusing to downgrade 0008 while artifact preparation data exists: "
            + ", ".join(populated)
        )


def downgrade() -> None:
    _refuse_destructive_downgrade()
    tables = _tables()
    for table in (
        "artifact_preparation_attempts",
        "artifact_preparation_nodes",
        "artifact_preparations",
    ):
        if table in tables:
            op.drop_table(table)
