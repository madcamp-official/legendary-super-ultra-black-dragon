"""Persist recommendations and deployment generation lineage.

Revision ID: 0005
Revises: 0004
"""

from alembic import op
import sqlalchemy as sa


revision = "0005"
down_revision = "0004"
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
    if "deployment_recommendations" not in _tables():
        _create_recommendations_table()
    _upgrade_deployments()


def _create_recommendations_table() -> None:
    op.create_table(
        "deployment_recommendations",
        sa.Column("id", sa.String(length=71), primary_key=True),
        sa.Column("objective", sa.String(length=40), nullable=False),
        sa.Column("selection_mode", sa.String(length=20), nullable=False),
        sa.Column("requested_node_ids", sa.JSON(), nullable=False),
        sa.Column("catalog_version", sa.String(length=71), nullable=False),
        sa.Column("policy_version", sa.String(length=64), nullable=False),
        sa.Column("inventory_fingerprint", sa.String(length=71), nullable=False),
        sa.Column("recommendation_snapshot", sa.JSON(), nullable=False),
        sa.Column("inventory_snapshot", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(id) = 71 AND id LIKE 'sha256:%'",
            name="ck_deployment_recommendation_id_sha256",
        ),
        sa.CheckConstraint(
            "selection_mode IN ('all_online', 'explicit_nodes')",
            name="ck_deployment_recommendation_selection_mode",
        ),
    )
    op.create_index(
        "ix_deployment_recommendations_created_at",
        "deployment_recommendations",
        ["created_at"],
    )


def _upgrade_deployments() -> None:
    columns = _columns("deployments")
    additions = []
    if "lineage_id" not in columns:
        additions.append(sa.Column("lineage_id", sa.String(length=255)))
    if "previous_generation_id" not in columns:
        additions.append(
            sa.Column("previous_generation_id", sa.String(length=255))
        )
    if "source_recommendation_id" not in columns:
        additions.append(
            sa.Column("source_recommendation_id", sa.String(length=71))
        )
    if additions:
        with op.batch_alter_table("deployments") as batch:
            for column in additions:
                batch.add_column(column)

    op.execute(
        sa.text(
            "UPDATE deployments SET lineage_id = id WHERE lineage_id IS NULL"
        )
    )
    columns = _columns("deployments")
    if "lineage_id" in columns:
        lineage = next(
            item
            for item in _inspector().get_columns("deployments")
            if item["name"] == "lineage_id"
        )
        if lineage.get("nullable", True):
            with op.batch_alter_table("deployments") as batch:
                batch.alter_column(
                    "lineage_id",
                    existing_type=sa.String(length=255),
                    nullable=False,
                )

    foreign_key_targets = _foreign_key_targets("deployments")
    unique_constraints = _constraints("deployments", "unique")
    with op.batch_alter_table("deployments") as batch:
        if (
            ("previous_generation_id",),
            "deployments",
            ("id",),
        ) not in foreign_key_targets:
            batch.create_foreign_key(
                "fk_deployments_previous_generation_id",
                "deployments",
                ["previous_generation_id"],
                ["id"],
            )
        if (
            ("source_recommendation_id",),
            "deployment_recommendations",
            ("id",),
        ) not in foreign_key_targets:
            batch.create_foreign_key(
                "fk_deployments_source_recommendation_id",
                "deployment_recommendations",
                ["source_recommendation_id"],
                ["id"],
            )
        if "uq_deployments_lineage_generation" not in unique_constraints:
            batch.create_unique_constraint(
                "uq_deployments_lineage_generation",
                ["lineage_id", "generation"],
            )
        if "uq_deployments_previous_generation_id" not in unique_constraints:
            batch.create_unique_constraint(
                "uq_deployments_previous_generation_id",
                ["previous_generation_id"],
            )
        if "uq_deployments_source_recommendation_id" not in unique_constraints:
            batch.create_unique_constraint(
                "uq_deployments_source_recommendation_id",
                ["source_recommendation_id"],
            )
def downgrade() -> None:
    if "deployments" in _tables():
        foreign_keys = _constraints("deployments", "foreignkey")
        unique_constraints = _constraints("deployments", "unique")
        columns = _columns("deployments")
        with op.batch_alter_table("deployments") as batch:
            for name in (
                "fk_deployments_previous_generation_id",
                "fk_deployments_source_recommendation_id",
            ):
                if name in foreign_keys:
                    batch.drop_constraint(name, type_="foreignkey")
            for name in (
                "uq_deployments_lineage_generation",
                "uq_deployments_previous_generation_id",
                "uq_deployments_source_recommendation_id",
            ):
                if name in unique_constraints:
                    batch.drop_constraint(name, type_="unique")
            for name in (
                "source_recommendation_id",
                "previous_generation_id",
                "lineage_id",
            ):
                if name in columns:
                    batch.drop_column(name)
    if "deployment_recommendations" in _tables():
        op.drop_table("deployment_recommendations")
