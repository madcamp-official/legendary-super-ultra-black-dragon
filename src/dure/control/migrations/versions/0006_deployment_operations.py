"""Track deployment operations and verified generations.

Revision ID: 0006
Revises: 0005
"""

from alembic import op
import sqlalchemy as sa


revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def _inspector():
    return sa.inspect(op.get_bind())


def _tables() -> set[str]:
    return set(_inspector().get_table_names())


def _columns(table: str) -> set[str]:
    return {column["name"] for column in _inspector().get_columns(table)}


def _constraints(table: str, kind: str) -> set[str]:
    inspector = _inspector()
    readers = {
        "check": inspector.get_check_constraints,
        "foreignkey": inspector.get_foreign_keys,
        "unique": inspector.get_unique_constraints,
    }
    return {
        item["name"]
        for item in readers[kind](table)
        if item.get("name") is not None
    }


def _foreign_key_targets(
    table: str,
) -> set[tuple[tuple[str, ...], str, tuple[str, ...]]]:
    return {
        (
            tuple(item["constrained_columns"]),
            item["referred_table"],
            tuple(item["referred_columns"]),
        )
        for item in _inspector().get_foreign_keys(table)
    }


def upgrade() -> None:
    _upgrade_deployments()
    if "deployment_operations" not in _tables():
        _create_deployment_operations()
    if "deployment_operation_nodes" not in _tables():
        _create_deployment_operation_nodes()
    _upgrade_tasks()


def _upgrade_deployments() -> None:
    if "verified_at" not in _columns("deployments"):
        with op.batch_alter_table("deployments") as batch:
            batch.add_column(sa.Column("verified_at", sa.DateTime(timezone=True)))


def _create_deployment_operations() -> None:
    op.create_table(
        "deployment_operations",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("request_digest", sa.String(length=71), nullable=False),
        sa.Column("lineage_id", sa.String(length=255), nullable=False),
        sa.Column("deployment_id", sa.String(length=255), nullable=False),
        sa.Column("rollback_target_id", sa.String(length=255)),
        sa.Column("kind", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("phase", sa.String(length=20), nullable=False),
        sa.Column("node_ids", sa.JSON(), nullable=False),
        sa.Column("serve", sa.Boolean(), nullable=False),
        sa.Column("api", sa.Boolean(), nullable=False),
        sa.Column("active_lineage_id", sa.String(length=255)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "length(id) = 36",
            name="ck_deployment_operation_id_length",
        ),
        sa.CheckConstraint(
            "length(request_digest) = 71 AND request_digest LIKE 'sha256:%'",
            name="ck_deployment_operation_request_digest_sha256",
        ),
        sa.CheckConstraint(
            "kind IN ('APPLY', 'VERIFY', 'ROLLBACK')",
            name="ck_deployment_operation_kind",
        ),
        sa.CheckConstraint(
            "status IN ('PREPARED', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'PARTIAL_FAILED', 'FAILED')",
            name="ck_deployment_operation_status",
        ),
        sa.CheckConstraint(
            "phase IN ('APPLY', 'VERIFY', 'STOP_SOURCE', 'START_TARGET', "
            "'VERIFY_TARGET', 'START_API', 'VERIFY_API', 'COMPLETE')",
            name="ck_deployment_operation_phase",
        ),
        sa.CheckConstraint(
            "(kind = 'ROLLBACK' AND rollback_target_id IS NOT NULL) OR "
            "(kind IN ('APPLY', 'VERIFY') AND rollback_target_id IS NULL)",
            name="ck_deployment_operation_rollback_target",
        ),
        sa.ForeignKeyConstraint(
            ["deployment_id"],
            ["deployments.id"],
            name="fk_deployment_operations_deployment_id",
        ),
        sa.ForeignKeyConstraint(
            ["rollback_target_id"],
            ["deployments.id"],
            name="fk_deployment_operations_rollback_target_id",
        ),
        sa.UniqueConstraint(
            "request_digest",
            name="uq_deployment_operations_request_digest",
        ),
        sa.UniqueConstraint(
            "active_lineage_id",
            name="uq_deployment_operations_active_lineage_id",
        ),
    )


def _create_deployment_operation_nodes() -> None:
    op.create_table(
        "deployment_operation_nodes",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("operation_id", sa.String(length=36), nullable=False),
        sa.Column("node_id", sa.String(length=36), nullable=False),
        sa.Column("phase", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("failure_code", sa.String(length=64)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "length(id) = 36",
            name="ck_deployment_operation_node_id_length",
        ),
        sa.CheckConstraint(
            "phase IN ('APPLY', 'VERIFY', 'STOP_SOURCE', 'START_TARGET', "
            "'VERIFY_TARGET', 'START_API', 'VERIFY_API', 'COMPLETE')",
            name="ck_deployment_operation_node_phase",
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'FAILED', 'CANCELED')",
            name="ck_deployment_operation_node_status",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name="ck_deployment_operation_node_attempt_nonnegative",
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR "
            "(length(failure_code) > 0 AND length(failure_code) <= 64)",
            name="ck_deployment_operation_node_failure_code",
        ),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["deployment_operations.id"],
            ondelete="CASCADE",
            name="fk_deployment_operation_nodes_operation_id",
        ),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["nodes.id"],
            name="fk_deployment_operation_nodes_node_id",
        ),
        sa.UniqueConstraint(
            "operation_id",
            "node_id",
            "phase",
            name="uq_deployment_operation_nodes_operation_node_phase",
        ),
    )


def _upgrade_tasks() -> None:
    columns = _columns("tasks")
    additions = []
    if "operation_node_id" not in columns:
        additions.append(sa.Column("operation_node_id", sa.String(length=36)))
    if "operation_attempt" not in columns:
        additions.append(sa.Column("operation_attempt", sa.Integer()))
    if additions:
        with op.batch_alter_table("tasks") as batch:
            for column in additions:
                batch.add_column(column)

    foreign_key_targets = _foreign_key_targets("tasks")
    check_constraints = _constraints("tasks", "check")
    unique_constraints = _constraints("tasks", "unique")
    with op.batch_alter_table("tasks") as batch:
        if (
            ("operation_node_id",),
            "deployment_operation_nodes",
            ("id",),
        ) not in foreign_key_targets:
            batch.create_foreign_key(
                "fk_tasks_operation_node_id",
                "deployment_operation_nodes",
                ["operation_node_id"],
                ["id"],
            )
        if "ck_tasks_operation_binding" not in check_constraints:
            batch.create_check_constraint(
                "ck_tasks_operation_binding",
                "(operation_node_id IS NULL AND operation_attempt IS NULL) OR "
                "(operation_node_id IS NOT NULL AND operation_attempt IS NOT NULL)",
            )
        if "ck_tasks_operation_attempt_positive" not in check_constraints:
            batch.create_check_constraint(
                "ck_tasks_operation_attempt_positive",
                "operation_attempt IS NULL OR operation_attempt >= 1",
            )
        if "uq_tasks_operation_node_attempt" not in unique_constraints:
            batch.create_unique_constraint(
                "uq_tasks_operation_node_attempt",
                ["operation_node_id", "operation_attempt"],
            )


def _scalar_count(statement: str) -> int:
    return int(op.get_bind().execute(sa.text(statement)).scalar_one())


def _refuse_active_downgrade() -> None:
    tables = _tables()
    if "deployment_operations" in tables:
        active_operations = _scalar_count(
            "SELECT COUNT(*) FROM deployment_operations "
            "WHERE active_lineage_id IS NOT NULL "
            "OR status IN ('PREPARED', 'QUEUED', 'RUNNING')"
        )
        if active_operations:
            raise RuntimeError(
                "refusing to downgrade while deployment operations are active"
            )
    if (
        "tasks" in tables
        and "operation_node_id" in _columns("tasks")
    ):
        active_tasks = _scalar_count(
            "SELECT COUNT(*) FROM tasks "
            "WHERE operation_node_id IS NOT NULL "
            "AND status IN ('QUEUED', 'RUNNING')"
        )
        if active_tasks:
            raise RuntimeError(
                "refusing to downgrade while deployment operation tasks are active"
            )


def downgrade() -> None:
    _refuse_active_downgrade()
    if "tasks" in _tables():
        _downgrade_tasks()
    if "deployment_operation_nodes" in _tables():
        op.drop_table("deployment_operation_nodes")
    if "deployment_operations" in _tables():
        op.drop_table("deployment_operations")
    if "deployments" in _tables() and "verified_at" in _columns("deployments"):
        with op.batch_alter_table("deployments") as batch:
            batch.drop_column("verified_at")


def _downgrade_tasks() -> None:
    columns = _columns("tasks")
    if "operation_node_id" not in columns and "operation_attempt" not in columns:
        return
    if "operation_node_id" in columns or "operation_attempt" in columns:
        assignments = []
        if "operation_node_id" in columns:
            assignments.append("operation_node_id = NULL")
        if "operation_attempt" in columns:
            assignments.append("operation_attempt = NULL")
        op.execute(sa.text("UPDATE tasks SET " + ", ".join(assignments)))

    foreign_keys = _constraints("tasks", "foreignkey")
    check_constraints = _constraints("tasks", "check")
    unique_constraints = _constraints("tasks", "unique")
    with op.batch_alter_table("tasks") as batch:
        if "fk_tasks_operation_node_id" in foreign_keys:
            batch.drop_constraint(
                "fk_tasks_operation_node_id",
                type_="foreignkey",
            )
        for name in (
            "ck_tasks_operation_binding",
            "ck_tasks_operation_attempt_positive",
        ):
            if name in check_constraints:
                batch.drop_constraint(name, type_="check")
        if "uq_tasks_operation_node_attempt" in unique_constraints:
            batch.drop_constraint(
                "uq_tasks_operation_node_attempt",
                type_="unique",
            )
        for name in ("operation_attempt", "operation_node_id"):
            if name in columns:
                batch.drop_column(name)
