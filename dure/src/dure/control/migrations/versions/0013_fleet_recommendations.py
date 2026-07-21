"""Persist immutable multi-deployment Fleet recommendations.

Revision ID: 0013
Revises: 0012
"""

from alembic import context, op
import sqlalchemy as sa


revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def _destructive_downgrade_lock_sql(dialect: str) -> tuple[str, ...]:
    if dialect != "postgresql":
        return ()
    return ("LOCK TABLE fleet_recommendations IN ACCESS EXCLUSIVE MODE",)


def upgrade() -> None:
    if not context.is_offline_mode():
        tables = set(sa.inspect(op.get_bind()).get_table_names())
        if "fleet_recommendations" in tables:
            # Revision 0001 intentionally materializes current metadata for a
            # brand-new database. Historical upgrades reach 0013 without the
            # table and use the explicit DDL below.
            return
    op.create_table(
        "fleet_recommendations",
        sa.Column("id", sa.String(length=71), primary_key=True),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("objective", sa.String(length=40), nullable=False),
        sa.Column("selection_mode", sa.String(length=20), nullable=False),
        sa.Column("requested_node_ids", sa.JSON(), nullable=False),
        sa.Column("minimum_replicas", sa.JSON(), nullable=False),
        sa.Column("minimum_reserve_nodes", sa.Integer(), nullable=False),
        sa.Column("reserve_node_ids", sa.JSON(), nullable=False),
        sa.Column(
            "inventory_fingerprint", sa.String(length=71), nullable=False
        ),
        sa.Column(
            "source_inventory_fingerprint",
            sa.String(length=71),
            nullable=False,
        ),
        sa.Column("catalog_version", sa.String(length=71), nullable=False),
        sa.Column(
            "catalog_policy_version", sa.String(length=64), nullable=False
        ),
        sa.Column(
            "candidate_policy_version", sa.String(length=64), nullable=False
        ),
        sa.Column("scheduler_version", sa.String(length=64), nullable=False),
        sa.Column("recommendation_snapshot", sa.JSON(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False
        ),
        sa.CheckConstraint(
            "length(id) = 71 AND id LIKE 'sha256:%'",
            name="ck_fleet_recommendation_id_sha256",
        ),
        sa.CheckConstraint(
            "schema_version = 1",
            name="ck_fleet_recommendation_schema_version",
        ),
        sa.CheckConstraint(
            "objective = 'quality-first'",
            name="ck_fleet_recommendation_objective",
        ),
        sa.CheckConstraint(
            "selection_mode IN ('all_online', 'explicit_nodes')",
            name="ck_fleet_recommendation_selection_mode",
        ),
        sa.CheckConstraint(
            "minimum_reserve_nodes >= 0",
            name="ck_fleet_recommendation_reserve_nonnegative",
        ),
        sa.CheckConstraint(
            "length(inventory_fingerprint) = 71 "
            "AND inventory_fingerprint LIKE 'sha256:%'",
            name="ck_fleet_recommendation_inventory_sha256",
        ),
        sa.CheckConstraint(
            "length(source_inventory_fingerprint) = 71 "
            "AND source_inventory_fingerprint LIKE 'sha256:%'",
            name="ck_fleet_recommendation_source_inventory_sha256",
        ),
        sa.CheckConstraint(
            "length(catalog_version) = 71 "
            "AND catalog_version LIKE 'sha256:%'",
            name="ck_fleet_recommendation_catalog_version_sha256",
        ),
        sa.CheckConstraint(
            "length(catalog_policy_version) BETWEEN 1 AND 64",
            name="ck_fleet_recommendation_catalog_policy_version",
        ),
        sa.CheckConstraint(
            "length(candidate_policy_version) BETWEEN 1 AND 64",
            name="ck_fleet_recommendation_candidate_policy_version",
        ),
        sa.CheckConstraint(
            "length(scheduler_version) BETWEEN 1 AND 64",
            name="ck_fleet_recommendation_scheduler_version",
        ),
    )
    op.create_index(
        "ix_fleet_recommendations_created_at",
        "fleet_recommendations",
        ["created_at"],
    )


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            "offline downgrade 0013 is disabled because Fleet "
            "recommendation data must be checked under a database lock"
        )
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "fleet_recommendations" not in set(inspector.get_table_names()):
        return
    for statement in _destructive_downgrade_lock_sql(bind.dialect.name):
        op.execute(statement)
    record_count = int(
        bind.execute(
            sa.text("SELECT COUNT(*) FROM fleet_recommendations")
        ).scalar_one()
    )
    if record_count:
        raise RuntimeError(
            "refusing to downgrade 0013 while Fleet recommendations exist"
        )
    indexes = {
        item["name"]
        for item in sa.inspect(bind).get_indexes("fleet_recommendations")
    }
    if "ix_fleet_recommendations_created_at" in indexes:
        op.drop_index(
            "ix_fleet_recommendations_created_at",
            table_name="fleet_recommendations",
        )
    op.drop_table("fleet_recommendations")
