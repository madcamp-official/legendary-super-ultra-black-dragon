"""Persist per-deployment Fleet runtime state.

Revision ID: 0015
Revises: 0014
"""

import hashlib

from alembic import context, op
import sqlalchemy as sa


revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


FLEET_STATUS_CHECK = (
    "status IN ('ACCEPTED', 'PREPARING', 'PREPARED', 'APPLYING', "
    "'VERIFYING', 'ACTIVE', 'PARTIAL_FAILED', 'FAILED')"
)
RUNTIME_STATUS_CHECK = (
    "status IN ('ACCEPTED', 'PREPARING', 'PREPARED', "
    "'PREPARE_FAILED', 'APPLYING', 'VERIFYING', 'ACTIVE', "
    "'APPLY_FAILED', 'VERIFY_FAILED')"
)
RUNTIME_FAILURE_CHECK = (
    "(status NOT IN ('PREPARE_FAILED', 'APPLY_FAILED', 'VERIFY_FAILED') "
    "AND failure_phase IS NULL AND failure_code IS NULL) OR "
    "(status = 'PREPARE_FAILED' AND failure_phase = 'PREPARE' "
    "AND failure_code IS NOT NULL "
    "AND length(failure_code) BETWEEN 1 AND 64) OR "
    "(status = 'APPLY_FAILED' AND failure_phase = 'APPLY' "
    "AND failure_code IS NOT NULL "
    "AND length(failure_code) BETWEEN 1 AND 64) OR "
    "(status = 'VERIFY_FAILED' AND failure_phase = 'VERIFY' "
    "AND failure_code IS NOT NULL "
    "AND length(failure_code) BETWEEN 1 AND 64)"
)


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


def _runtime_id(fleet_id: str, deployment_id: str) -> str:
    # PostgreSQL의 기본 md5()와 같은 결정론적 포맷을 만드는 식별자용 해시다.
    digest = hashlib.md5(
        f"{fleet_id}:{deployment_id}".encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()
    return (
        f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-"
        f"{digest[16:20]}-{digest[20:]}"
    )


def _destructive_downgrade_lock_sql(dialect: str) -> tuple[str, ...]:
    if dialect != "postgresql":
        return ()
    return (
        "LOCK TABLE fleets, fleet_deployment_runtime "
        "IN ACCESS EXCLUSIVE MODE",
    )


def _inspector(bind):
    return None if context.is_offline_mode() else sa.inspect(bind)


def _table_names(inspector) -> set[str]:
    return set() if inspector is None else set(inspector.get_table_names())


def _column_map(inspector, table: str) -> dict[str, dict]:
    if inspector is None:
        return {}
    return {item["name"]: item for item in inspector.get_columns(table)}


def _check_map(inspector, table: str) -> dict[str, str]:
    if inspector is None:
        return {}
    return {
        item["name"]: str(item.get("sqltext") or "")
        for item in inspector.get_check_constraints(table)
        if item.get("name") is not None
    }


def _index_names(inspector, table: str) -> set[str]:
    if inspector is None:
        return set()
    return {
        item["name"]
        for item in inspector.get_indexes(table)
        if item.get("name") is not None
    }


def _has_expanded_fleet_status(checks: dict[str, str]) -> bool:
    expression = checks.get("ck_fleets_status", "").upper()
    return all(
        f"'{status}'" in expression
        for status in (
            "ACCEPTED",
            "PREPARING",
            "PREPARED",
            "APPLYING",
            "VERIFYING",
            "ACTIVE",
            "PARTIAL_FAILED",
            "FAILED",
        )
    )


def upgrade() -> None:
    bind = op.get_bind()
    inspector = _inspector(bind)

    _upgrade_fleets(inspector)

    inspector = _inspector(bind)
    if "fleet_deployment_runtime" not in _table_names(inspector):
        _create_fleet_deployment_runtime()
    else:
        _ensure_runtime_index(inspector)

    _backfill_accepted_deployments()


def _upgrade_fleets(inspector) -> None:
    offline = inspector is None
    columns = _column_map(inspector, "fleets")
    if offline or "updated_at" not in columns:
        op.add_column(
            "fleets",
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=True,
            ),
        )

    op.execute(
        sa.text(
            "UPDATE fleets SET updated_at = created_at "
            "WHERE updated_at IS NULL"
        )
    )

    if not offline:
        inspector = _inspector(op.get_bind())
        columns = _column_map(inspector, "fleets")
    checks = _check_map(inspector, "fleets")
    replace_status_check = offline or not _has_expanded_fleet_status(checks)
    make_updated_at_nonnull = offline or columns["updated_at"]["nullable"]

    if replace_status_check or make_updated_at_nonnull:
        with op.batch_alter_table("fleets") as batch:
            if make_updated_at_nonnull:
                batch.alter_column(
                    "updated_at",
                    existing_type=sa.DateTime(timezone=True),
                    nullable=False,
                )
            if replace_status_check:
                if offline or "ck_fleets_status" in checks:
                    batch.drop_constraint(
                        "ck_fleets_status", type_="check"
                    )
                batch.create_check_constraint(
                    "ck_fleets_status", FLEET_STATUS_CHECK
                )


def _create_fleet_deployment_runtime() -> None:
    op.create_table(
        "fleet_deployment_runtime",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("fleet_id", sa.String(length=36), nullable=False),
        sa.Column("deployment_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("preparation_id", sa.String(length=36)),
        sa.Column("current_operation_id", sa.String(length=36)),
        sa.Column("failure_phase", sa.String(length=16)),
        sa.Column("failure_code", sa.String(length=64)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False
        ),
        sa.CheckConstraint(
            _canonical_uuid_check(),
            name="ck_fleet_deployment_runtime_id_canonical_uuid",
        ),
        sa.CheckConstraint(
            RUNTIME_STATUS_CHECK,
            name="ck_fleet_deployment_runtime_status",
        ),
        sa.CheckConstraint(
            RUNTIME_FAILURE_CHECK,
            name="ck_fleet_deployment_runtime_failure",
        ),
        sa.ForeignKeyConstraint(
            ["fleet_id"],
            ["fleets.id"],
            ondelete="CASCADE",
            name="fk_fleet_deployment_runtime_fleet_id",
        ),
        sa.ForeignKeyConstraint(
            ["fleet_id", "deployment_id"],
            ["deployments.fleet_id", "deployments.id"],
            ondelete="CASCADE",
            name="fk_fleet_deployment_runtime_fleet_deployment",
        ),
        sa.ForeignKeyConstraint(
            ["preparation_id"],
            ["artifact_preparations.id"],
            name="fk_fleet_deployment_runtime_preparation_id",
        ),
        sa.ForeignKeyConstraint(
            ["current_operation_id"],
            ["deployment_operations.id"],
            name="fk_fleet_deployment_runtime_current_operation_id",
        ),
        sa.UniqueConstraint(
            "fleet_id",
            "deployment_id",
            name="uq_fleet_deployment_runtime_fleet_deployment",
        ),
        sa.UniqueConstraint(
            "preparation_id",
            name="uq_fleet_deployment_runtime_preparation",
        ),
        sa.UniqueConstraint(
            "current_operation_id",
            name="uq_fleet_deployment_runtime_current_operation",
        ),
    )
    _ensure_runtime_index(None)


def _ensure_runtime_index(inspector) -> None:
    if (
        "ix_fleet_deployment_runtime_fleet_status"
        not in _index_names(inspector, "fleet_deployment_runtime")
    ):
        op.create_index(
            "ix_fleet_deployment_runtime_fleet_status",
            "fleet_deployment_runtime",
            ["fleet_id", "status"],
        )


def _backfill_accepted_deployments() -> None:
    if context.is_offline_mode():
        dialect = context.get_context().dialect.name
        if dialect != "postgresql":
            raise RuntimeError(
                "offline upgrade 0015 backfill is supported only for "
                "PostgreSQL"
            )
        digest = "md5(f.id || ':' || d.id)"
        runtime_id = (
            f"substr({digest}, 1, 8) || '-' || "
            f"substr({digest}, 9, 4) || '-' || "
            f"substr({digest}, 13, 4) || '-' || "
            f"substr({digest}, 17, 4) || '-' || "
            f"substr({digest}, 21, 12)"
        )
        op.execute(
            sa.text(
                "INSERT INTO fleet_deployment_runtime "
                "(id, fleet_id, deployment_id, status, preparation_id, "
                "current_operation_id, failure_phase, failure_code, "
                "created_at, updated_at) "
                f"SELECT {runtime_id}, f.id, d.id, 'ACCEPTED', "
                "NULL, NULL, NULL, NULL, f.created_at, f.updated_at "
                "FROM deployments d "
                "JOIN fleets f ON f.id = d.fleet_id "
                "WHERE f.status = 'ACCEPTED' "
                "AND NOT EXISTS ("
                "SELECT 1 FROM fleet_deployment_runtime runtime "
                "WHERE runtime.deployment_id = d.id)"
            )
        )
        return

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT f.id AS fleet_id, d.id AS deployment_id, "
            "f.created_at AS created_at, f.updated_at AS updated_at "
            "FROM deployments d "
            "JOIN fleets f ON f.id = d.fleet_id "
            "WHERE f.status = 'ACCEPTED' "
            "AND NOT EXISTS ("
            "SELECT 1 FROM fleet_deployment_runtime runtime "
            "WHERE runtime.deployment_id = d.id)"
        )
    ).mappings().all()
    insert = sa.text(
        "INSERT INTO fleet_deployment_runtime "
        "(id, fleet_id, deployment_id, status, preparation_id, "
        "current_operation_id, failure_phase, failure_code, "
        "created_at, updated_at) VALUES "
        "(:id, :fleet_id, :deployment_id, 'ACCEPTED', NULL, NULL, "
        "NULL, NULL, :created_at, :updated_at)"
    )
    for row in rows:
        bind.execute(
            insert,
            {
                "id": _runtime_id(row["fleet_id"], row["deployment_id"]),
                "fleet_id": row["fleet_id"],
                "deployment_id": row["deployment_id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            },
        )


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            "offline downgrade 0015 is disabled because Fleet runtime "
            "data and lifecycle status must be checked under a database lock"
        )

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    for statement in _destructive_downgrade_lock_sql(bind.dialect.name):
        op.execute(statement)

    runtime_count = (
        int(
            bind.execute(
                sa.text(
                    "SELECT COUNT(*) FROM fleet_deployment_runtime"
                )
            ).scalar_one()
        )
        if "fleet_deployment_runtime" in tables
        else 0
    )
    nonaccepted_count = (
        int(
            bind.execute(
                sa.text(
                    "SELECT COUNT(*) FROM fleets "
                    "WHERE status <> 'ACCEPTED'"
                )
            ).scalar_one()
        )
        if "fleets" in tables
        else 0
    )
    if runtime_count or nonaccepted_count:
        raise RuntimeError(
            "refusing to downgrade 0015 while Fleet runtime rows or "
            "non-ACCEPTED Fleet lifecycle states exist"
        )

    if "fleet_deployment_runtime" in tables:
        op.drop_table("fleet_deployment_runtime")

    inspector = sa.inspect(bind)
    if "fleets" in set(inspector.get_table_names()):
        columns = _column_map(inspector, "fleets")
        checks = _check_map(inspector, "fleets")
        with op.batch_alter_table("fleets") as batch:
            if "ck_fleets_status" in checks:
                batch.drop_constraint("ck_fleets_status", type_="check")
            batch.create_check_constraint(
                "ck_fleets_status", "status = 'ACCEPTED'"
            )
            if "updated_at" in columns:
                batch.drop_column("updated_at")
