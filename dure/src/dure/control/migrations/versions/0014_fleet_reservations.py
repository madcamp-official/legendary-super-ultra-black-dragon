"""Persist accepted Fleets and atomic node/GPU reservations.

Revision ID: 0014
Revises: 0013
"""

from alembic import context, op
import sqlalchemy as sa


revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def _destructive_downgrade_lock_sql(dialect: str) -> tuple[str, ...]:
    if dialect != "postgresql":
        return ()
    return (
        "LOCK TABLE deployments, fleets, fleet_resource_reservations "
        "IN ACCESS EXCLUSIVE MODE",
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


def _inspector(bind):
    return None if context.is_offline_mode() else sa.inspect(bind)


def _table_names(inspector) -> set[str]:
    return set() if inspector is None else set(inspector.get_table_names())


def _column_names(inspector, table: str) -> set[str]:
    if inspector is None:
        return set()
    return {item["name"] for item in inspector.get_columns(table)}


def _constraint_names(inspector, table: str, kind: str) -> set[str]:
    if inspector is None:
        return set()
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


def _index_names(inspector, table: str) -> set[str]:
    if inspector is None:
        return set()
    return {
        item["name"]
        for item in inspector.get_indexes(table)
        if item.get("name") is not None
    }


def upgrade() -> None:
    bind = op.get_bind()
    inspector = _inspector(bind)
    tables = _table_names(inspector)

    if "fleets" not in tables:
        _create_fleets()

    inspector = _inspector(bind)
    _upgrade_deployments(inspector)

    inspector = _inspector(bind)
    tables = _table_names(inspector)
    if "fleet_resource_reservations" not in tables:
        _create_fleet_resource_reservations()
    else:
        _ensure_reservation_indexes(inspector)


def _create_fleets() -> None:
    op.create_table(
        "fleets",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "source_recommendation_id", sa.String(length=71), nullable=False
        ),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            _canonical_uuid_check(),
            name="ck_fleets_id_canonical_uuid",
        ),
        sa.CheckConstraint(
            "status = 'ACCEPTED'",
            name="ck_fleets_status",
        ),
        sa.ForeignKeyConstraint(
            ["source_recommendation_id"],
            ["fleet_recommendations.id"],
            name="fk_fleets_source_recommendation_id",
        ),
        sa.UniqueConstraint(
            "source_recommendation_id",
            name="uq_fleets_source_recommendation_id",
        ),
    )


def _upgrade_deployments(inspector) -> None:
    columns = _column_names(inspector, "deployments")
    additions = []
    if "fleet_id" not in columns:
        additions.append(sa.Column("fleet_id", sa.String(length=36)))
    if "fleet_candidate_id" not in columns:
        additions.append(
            sa.Column("fleet_candidate_id", sa.String(length=71))
        )
    if additions:
        with op.batch_alter_table("deployments") as batch:
            for column in additions:
                batch.add_column(column)

    inspector = _inspector(op.get_bind())
    foreign_keys = _constraint_names(inspector, "deployments", "foreignkey")
    checks = _constraint_names(inspector, "deployments", "check")
    uniques = _constraint_names(inspector, "deployments", "unique")
    with op.batch_alter_table("deployments") as batch:
        if "fk_deployments_fleet_id" not in foreign_keys:
            batch.create_foreign_key(
                "fk_deployments_fleet_id",
                "fleets",
                ["fleet_id"],
                ["id"],
            )
        if "ck_deployments_fleet_binding" not in checks:
            batch.create_check_constraint(
                "ck_deployments_fleet_binding",
                "(fleet_id IS NULL AND fleet_candidate_id IS NULL) OR "
                "(fleet_id IS NOT NULL AND fleet_candidate_id IS NOT NULL)",
            )
        if "ck_deployments_fleet_candidate_sha256" not in checks:
            batch.create_check_constraint(
                "ck_deployments_fleet_candidate_sha256",
                "fleet_candidate_id IS NULL OR "
                "(length(fleet_candidate_id) = 71 "
                "AND fleet_candidate_id LIKE 'sha256:%')",
            )
        if "uq_deployments_fleet_candidate_id" not in uniques:
            batch.create_unique_constraint(
                "uq_deployments_fleet_candidate_id",
                ["fleet_id", "fleet_candidate_id"],
            )
        if "uq_deployments_fleet_ownership" not in uniques:
            batch.create_unique_constraint(
                "uq_deployments_fleet_ownership",
                ["fleet_id", "id"],
            )


def _create_fleet_resource_reservations() -> None:
    op.create_table(
        "fleet_resource_reservations",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("fleet_id", sa.String(length=36), nullable=False),
        sa.Column("deployment_id", sa.String(length=255), nullable=False),
        sa.Column("node_id", sa.String(length=36), nullable=False),
        sa.Column("gpu_index", sa.Integer(), nullable=False),
        sa.Column("gpu_uuid", sa.String(length=128), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("released_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            _canonical_uuid_check(),
            name="ck_fleet_resource_reservation_id_canonical_uuid",
        ),
        sa.CheckConstraint(
            "gpu_index >= 0",
            name="ck_fleet_resource_reservation_gpu_index",
        ),
        sa.CheckConstraint(
            "gpu_uuid LIKE 'GPU-%' AND length(gpu_uuid) BETWEEN 5 AND 128",
            name="ck_fleet_resource_reservation_gpu_uuid",
        ),
        sa.CheckConstraint(
            "rank >= 0",
            name="ck_fleet_resource_reservation_rank",
        ),
        sa.ForeignKeyConstraint(
            ["fleet_id"],
            ["fleets.id"],
            ondelete="CASCADE",
            name="fk_fleet_resource_reservations_fleet_id",
        ),
        sa.ForeignKeyConstraint(
            ["fleet_id", "deployment_id"],
            ["deployments.fleet_id", "deployments.id"],
            name="fk_fleet_resource_reservations_fleet_deployment",
        ),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["nodes.id"],
            name="fk_fleet_resource_reservations_node_id",
        ),
        sa.UniqueConstraint(
            "fleet_id",
            "node_id",
            name="uq_fleet_resource_reservations_fleet_node",
        ),
        sa.UniqueConstraint(
            "fleet_id",
            "gpu_uuid",
            name="uq_fleet_resource_reservations_fleet_gpu_uuid",
        ),
        sa.UniqueConstraint(
            "fleet_id",
            "deployment_id",
            "rank",
            name="uq_fleet_resource_reservations_fleet_deployment_rank",
        ),
    )
    _ensure_reservation_indexes(None)


def _ensure_reservation_indexes(inspector) -> None:
    indexes = _index_names(
        inspector, "fleet_resource_reservations"
    )
    where = sa.text("released_at IS NULL")
    if "ux_fleet_resource_reservations_active_node" not in indexes:
        op.create_index(
            "ux_fleet_resource_reservations_active_node",
            "fleet_resource_reservations",
            ["node_id"],
            unique=True,
            sqlite_where=where,
            postgresql_where=where,
        )
    if "ux_fleet_resource_reservations_active_gpu_uuid" not in indexes:
        op.create_index(
            "ux_fleet_resource_reservations_active_gpu_uuid",
            "fleet_resource_reservations",
            ["gpu_uuid"],
            unique=True,
            sqlite_where=where,
            postgresql_where=where,
        )


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            "offline downgrade 0014 is disabled because Fleet reservation "
            "data must be checked under a database lock"
        )
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    for statement in _destructive_downgrade_lock_sql(bind.dialect.name):
        op.execute(statement)

    fleet_count = (
        int(
            bind.execute(sa.text("SELECT COUNT(*) FROM fleets")).scalar_one()
        )
        if "fleets" in tables
        else 0
    )
    reservation_count = (
        int(
            bind.execute(
                sa.text("SELECT COUNT(*) FROM fleet_resource_reservations")
            ).scalar_one()
        )
        if "fleet_resource_reservations" in tables
        else 0
    )
    deployment_columns = (
        _column_names(inspector, "deployments")
        if "deployments" in tables
        else set()
    )
    linked_predicates = [
        f"{name} IS NOT NULL"
        for name in ("fleet_id", "fleet_candidate_id")
        if name in deployment_columns
    ]
    linked_count = (
        int(
            bind.execute(
                sa.text(
                    "SELECT COUNT(*) FROM deployments WHERE "
                    + " OR ".join(linked_predicates)
                )
            ).scalar_one()
        )
        if linked_predicates
        else 0
    )
    if fleet_count or reservation_count or linked_count:
        raise RuntimeError(
            "refusing to downgrade 0014 while Fleet or reservation data exists"
        )

    if "fleet_resource_reservations" in tables:
        indexes = _index_names(inspector, "fleet_resource_reservations")
        for name in (
            "ux_fleet_resource_reservations_active_gpu_uuid",
            "ux_fleet_resource_reservations_active_node",
        ):
            if name in indexes:
                op.drop_index(
                    name, table_name="fleet_resource_reservations"
                )
        op.drop_table("fleet_resource_reservations")

    if "deployments" in tables:
        _downgrade_deployments(sa.inspect(bind))

    if "fleets" in tables:
        op.drop_table("fleets")


def _downgrade_deployments(inspector) -> None:
    foreign_keys = _constraint_names(inspector, "deployments", "foreignkey")
    checks = _constraint_names(inspector, "deployments", "check")
    uniques = _constraint_names(inspector, "deployments", "unique")
    columns = _column_names(inspector, "deployments")
    with op.batch_alter_table("deployments") as batch:
        if "fk_deployments_fleet_id" in foreign_keys:
            batch.drop_constraint(
                "fk_deployments_fleet_id", type_="foreignkey"
            )
        for name in (
            "ck_deployments_fleet_candidate_sha256",
            "ck_deployments_fleet_binding",
        ):
            if name in checks:
                batch.drop_constraint(name, type_="check")
        for name in (
            "uq_deployments_fleet_ownership",
            "uq_deployments_fleet_candidate_id",
        ):
            if name in uniques:
                batch.drop_constraint(name, type_="unique")
        for name in ("fleet_candidate_id", "fleet_id"):
            if name in columns:
                batch.drop_column(name)
