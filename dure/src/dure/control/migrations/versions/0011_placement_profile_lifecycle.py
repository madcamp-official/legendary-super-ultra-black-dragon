"""Add placement-profile lifecycle and closed execution settings.

Revision ID: 0011
Revises: 0010
"""

from alembic import op
import sqlalchemy as sa


revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {
        item["name"] for item in inspector.get_columns("placement_profiles")
    }
    with op.batch_alter_table("placement_profiles") as batch:
        if "max_model_len" not in columns:
            batch.add_column(
                sa.Column(
                    "max_model_len",
                    sa.Integer(),
                    nullable=False,
                    server_default="8192",
                )
            )
        if "max_concurrency" not in columns:
            batch.add_column(
                sa.Column(
                    "max_concurrency",
                    sa.Integer(),
                    nullable=False,
                    server_default="1",
                )
            )
        if "origin" not in columns:
            batch.add_column(
                sa.Column(
                    "origin",
                    sa.String(length=20),
                    nullable=False,
                    server_default="MANUAL",
                )
            )
        if "status" not in columns:
            batch.add_column(
                sa.Column(
                    "status",
                    sa.String(length=20),
                    nullable=False,
                    server_default="ACTIVE",
                )
            )
        if "spec_digest" not in columns:
            batch.add_column(sa.Column("spec_digest", sa.String(length=71)))

    inspector = sa.inspect(op.get_bind())
    checks = {
        item["name"]
        for item in inspector.get_check_constraints("placement_profiles")
    }
    indexes = {
        item["name"] for item in inspector.get_indexes("placement_profiles")
    }
    with op.batch_alter_table("placement_profiles") as batch:
        if "ck_placement_context_positive" not in checks:
            batch.create_check_constraint(
                "ck_placement_context_positive", "max_model_len > 0"
            )
        if "ck_placement_concurrency_positive" not in checks:
            batch.create_check_constraint(
                "ck_placement_concurrency_positive", "max_concurrency > 0"
            )
        if "ck_placement_origin" not in checks:
            batch.create_check_constraint(
                "ck_placement_origin", "origin IN ('MANUAL', 'AUTO')"
            )
        if "ck_placement_status" not in checks:
            batch.create_check_constraint(
                "ck_placement_status",
                "status IN ('DRAFT', 'QUALIFYING', 'VALIDATED', 'ACTIVE', 'REVOKED')",
            )
        if "ck_placement_auto_tp1" not in checks:
            batch.create_check_constraint(
                "ck_placement_auto_tp1",
                "origin != 'AUTO' OR tensor_parallel_size = 1",
            )
        if "ck_placement_auto_pp_nodes" not in checks:
            batch.create_check_constraint(
                "ck_placement_auto_pp_nodes",
                "origin != 'AUTO' OR pipeline_parallel_size = node_count",
            )
        if "ix_placement_profiles_status" not in indexes:
            batch.create_index("ix_placement_profiles_status", ["status"])


def downgrade() -> None:
    bind = op.get_bind()
    auto_or_nonactive = int(
        bind.execute(
            sa.text(
                "SELECT COUNT(*) FROM placement_profiles "
                "WHERE origin <> 'MANUAL' OR status <> 'ACTIVE'"
            )
        ).scalar_one()
    )
    if auto_or_nonactive:
        raise RuntimeError(
            "refusing to downgrade 0011 while automatic or non-active "
            "placement profiles exist"
        )
    with op.batch_alter_table("placement_profiles") as batch:
        batch.drop_index("ix_placement_profiles_status")
        for name in (
            "ck_placement_auto_pp_nodes",
            "ck_placement_auto_tp1",
            "ck_placement_status",
            "ck_placement_origin",
            "ck_placement_concurrency_positive",
            "ck_placement_context_positive",
        ):
            batch.drop_constraint(name, type_="check")
        batch.drop_column("spec_digest")
        batch.drop_column("status")
        batch.drop_column("origin")
        batch.drop_column("max_concurrency")
        batch.drop_column("max_model_len")
