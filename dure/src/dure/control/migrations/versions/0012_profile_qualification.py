"""Persist exact GPU-bound placement qualification evidence.

Revision ID: 0012
Revises: 0011
"""

from alembic import context, op
import sqlalchemy as sa


revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def _destructive_downgrade_lock_sql(dialect: str) -> tuple[str, ...]:
    if dialect != "postgresql":
        return ()
    return (
        "LOCK TABLE placement_profiles, profile_qualification_runs, "
        "profile_qualification_bindings, profile_qualification_evidence "
        "IN ACCESS EXCLUSIVE MODE",
    )


def upgrade() -> None:
    bind = op.get_bind()
    offline = context.is_offline_mode()
    dialect = bind.dialect.name
    if offline:
        if dialect != "postgresql":
            raise RuntimeError(
                "offline upgrade 0012 is supported only for PostgreSQL"
            )
        op.execute(
            "LOCK TABLE placement_profiles IN ACCESS EXCLUSIVE MODE"
        )
        op.execute(
            sa.text(
                "DO $$ BEGIN "
                "IF EXISTS (SELECT 1 FROM placement_profiles "
                "WHERE origin = 'AUTO' AND status <> 'DRAFT') THEN "
                "RAISE EXCEPTION 'upgrade 0012 requires legacy AUTO "
                "placement profiles to remain DRAFT'; "
                "END IF; END $$"
            )
        )
    else:
        if dialect == "postgresql":
            op.execute(
                "LOCK TABLE placement_profiles IN ACCESS EXCLUSIVE MODE"
            )
        unsupported_auto = int(
            bind.execute(
                sa.text(
                    "SELECT COUNT(*) FROM placement_profiles "
                    "WHERE origin = 'AUTO' AND status <> 'DRAFT'"
                )
            ).scalar_one()
        )
        if unsupported_auto:
            raise RuntimeError(
                "refusing to upgrade 0012 while legacy AUTO placement "
                "profiles are not DRAFT"
            )
    inspector = None if offline else sa.inspect(bind)
    placement_columns = (
        {
            item["name"]
            for item in inspector.get_columns("placement_profiles")
        }
        if inspector is not None
        else set()
    )
    with op.batch_alter_table("placement_profiles") as batch:
        if "qualification_evidence_id" not in placement_columns:
            batch.add_column(
                sa.Column("qualification_evidence_id", sa.String(length=36))
            )
        if "qualified_at" not in placement_columns:
            batch.add_column(
                sa.Column("qualified_at", sa.DateTime(timezone=True))
            )
        if "activated_at" not in placement_columns:
            batch.add_column(
                sa.Column("activated_at", sa.DateTime(timezone=True))
            )

    # SQLite batch mode must materialize the new columns before rebuilding the
    # table for constraints that reference them. Keeping these operations in a
    # single batch creates a circular dependency in Alembic's batch planner.
    inspector = None if offline else sa.inspect(bind)
    placement_checks = (
        {
            item["name"]
            for item in inspector.get_check_constraints("placement_profiles")
        }
        if inspector is not None
        else set()
    )
    with op.batch_alter_table("placement_profiles") as batch:
        if "ck_placement_auto_evidence" not in placement_checks:
            batch.create_check_constraint(
                "ck_placement_auto_evidence",
                "origin != 'AUTO' OR status IN ('DRAFT', 'QUALIFYING') "
                "OR qualification_evidence_id IS NOT NULL",
            )
        if "ck_placement_auto_activation" not in placement_checks:
            batch.create_check_constraint(
                "ck_placement_auto_activation",
                "origin != 'AUTO' OR status != 'ACTIVE' "
                "OR activated_at IS NOT NULL",
            )
        if "ck_placement_auto_qualified_at" not in placement_checks:
            batch.create_check_constraint(
                "ck_placement_auto_qualified_at",
                "origin != 'AUTO' OR status NOT IN ('VALIDATED', 'ACTIVE') "
                "OR qualified_at IS NOT NULL",
            )

    inspector = None if offline else sa.inspect(bind)
    tables = set(inspector.get_table_names()) if inspector is not None else set()
    if "profile_qualification_runs" not in tables:
        op.create_table(
            "profile_qualification_runs",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("release_id", sa.String(length=36), nullable=False),
            sa.Column("placement_id", sa.String(length=36), nullable=False),
            sa.Column("status", sa.String(length=20), nullable=False),
            sa.Column("node_ids", sa.JSON(), nullable=False),
            sa.Column("rank_node_ids", sa.JSON(), nullable=False),
            sa.Column("gpu_bindings", sa.JSON(), nullable=False),
            sa.Column(
                "inventory_fingerprint", sa.String(length=71), nullable=False
            ),
            sa.Column(
                "profile_spec_digest", sa.String(length=71), nullable=False
            ),
            sa.Column("policy_version", sa.String(length=64), nullable=False),
            sa.Column("suite_id", sa.String(length=64), nullable=False),
            sa.Column("required_steps", sa.JSON(), nullable=False),
            sa.Column("workload", sa.JSON(), nullable=False),
            sa.Column(
                "workload_digest", sa.String(length=71), nullable=False
            ),
            sa.Column("max_model_len", sa.Integer(), nullable=False),
            sa.Column("max_concurrency", sa.Integer(), nullable=False),
            sa.Column("artifact_revision", sa.String(length=64), nullable=False),
            sa.Column(
                "artifact_manifest_digest", sa.String(length=71), nullable=False
            ),
            sa.Column("runtime_image", sa.String(length=512), nullable=False),
            sa.Column(
                "runtime_vllm_version", sa.String(length=64), nullable=False
            ),
            sa.Column("evidence_id", sa.String(length=36)),
            sa.Column("failure_code", sa.String(length=64)),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(["release_id"], ["model_releases.id"]),
            sa.ForeignKeyConstraint(
                ["placement_id"], ["placement_profiles.id"]
            ),
            sa.CheckConstraint(
                "status IN ('QUALIFYING', 'PASSED', 'FAILED', 'CANCELED')",
                name="ck_profile_qualification_run_status",
            ),
            sa.CheckConstraint(
                "length(inventory_fingerprint) = 71 "
                "AND inventory_fingerprint LIKE 'sha256:%'",
                name="ck_profile_qualification_run_inventory_sha256",
            ),
            sa.CheckConstraint(
                "length(profile_spec_digest) = 71 "
                "AND profile_spec_digest LIKE 'sha256:%'",
                name="ck_profile_qualification_run_spec_sha256",
            ),
            sa.CheckConstraint(
                "length(workload_digest) = 71 "
                "AND workload_digest LIKE 'sha256:%'",
                name="ck_profile_qualification_run_workload_sha256",
            ),
            sa.CheckConstraint(
                "max_model_len > 0",
                name="ck_profile_qualification_run_context_positive",
            ),
            sa.CheckConstraint(
                "max_concurrency > 0",
                name="ck_profile_qualification_run_concurrency_positive",
            ),
            sa.CheckConstraint(
                "(status = 'QUALIFYING' AND evidence_id IS NULL "
                "AND failure_code IS NULL) OR "
                "(status = 'PASSED' AND evidence_id IS NOT NULL "
                "AND failure_code IS NULL) OR "
                "(status IN ('FAILED', 'CANCELED') "
                "AND failure_code IS NOT NULL)",
                name="ck_profile_qualification_run_outcome",
            ),
        )
    inspector = None if offline else sa.inspect(bind)
    run_indexes = (
        {
            item["name"]
            for item in inspector.get_indexes("profile_qualification_runs")
        }
        if inspector is not None
        else set()
    )
    if "ix_profile_qualification_runs_placement" not in run_indexes:
        op.create_index(
            "ix_profile_qualification_runs_placement",
            "profile_qualification_runs",
            ["placement_id"],
        )
    if "ix_profile_qualification_runs_status" not in run_indexes:
        op.create_index(
            "ix_profile_qualification_runs_status",
            "profile_qualification_runs",
            ["status"],
        )

    inspector = None if offline else sa.inspect(bind)
    if "profile_qualification_bindings" not in (
        set(inspector.get_table_names()) if inspector is not None else set()
    ):
        op.create_table(
            "profile_qualification_bindings",
            sa.Column("run_id", sa.String(length=36), primary_key=True),
            sa.Column("rank", sa.Integer(), primary_key=True),
            sa.Column("node_id", sa.String(length=36), nullable=False),
            sa.Column("gpu_index", sa.Integer(), nullable=False),
            sa.Column("gpu_uuid", sa.String(length=128), nullable=False),
            sa.Column("memory_mib", sa.Integer(), nullable=False),
            sa.Column("compute_capability", sa.String(length=32)),
            sa.ForeignKeyConstraint(
                ["run_id"], ["profile_qualification_runs.id"]
            ),
            sa.ForeignKeyConstraint(["node_id"], ["nodes.id"]),
            sa.UniqueConstraint(
                "run_id",
                "node_id",
                name="uq_profile_qualification_binding_node",
            ),
            sa.UniqueConstraint(
                "run_id",
                "gpu_uuid",
                name="uq_profile_qualification_binding_gpu_uuid",
            ),
            sa.CheckConstraint(
                "rank >= 0",
                name="ck_profile_qualification_binding_rank",
            ),
            sa.CheckConstraint(
                "gpu_index >= 0",
                name="ck_profile_qualification_binding_gpu_index",
            ),
            sa.CheckConstraint(
                "gpu_uuid LIKE 'GPU-%'",
                name="ck_profile_qualification_binding_gpu_uuid",
            ),
            sa.CheckConstraint(
                "memory_mib > 0",
                name="ck_profile_qualification_binding_memory",
            ),
        )
    inspector = None if offline else sa.inspect(bind)
    binding_indexes = (
        {
            item["name"]
            for item in inspector.get_indexes(
                "profile_qualification_bindings"
            )
        }
        if inspector is not None
        else set()
    )
    if "ix_profile_qualification_bindings_node" not in binding_indexes:
        op.create_index(
            "ix_profile_qualification_bindings_node",
            "profile_qualification_bindings",
            ["node_id"],
        )

    inspector = None if offline else sa.inspect(bind)
    if "profile_qualification_evidence" not in (
        set(inspector.get_table_names()) if inspector is not None else set()
    ):
        op.create_table(
            "profile_qualification_evidence",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("run_id", sa.String(length=36), nullable=False),
            sa.Column("status", sa.String(length=20), nullable=False),
            sa.Column("steps", sa.JSON(), nullable=False),
            sa.Column("metrics", sa.JSON(), nullable=False),
            sa.Column("policy_version", sa.String(length=64), nullable=False),
            sa.Column("suite_id", sa.String(length=64), nullable=False),
            sa.Column(
                "workload_digest", sa.String(length=71), nullable=False
            ),
            sa.Column("executor_image", sa.String(length=512), nullable=False),
            sa.Column("dure_commit", sa.String(length=64), nullable=False),
            sa.Column("evidence_digest", sa.String(length=71), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(
                ["run_id"], ["profile_qualification_runs.id"]
            ),
            sa.UniqueConstraint("run_id"),
            sa.UniqueConstraint("evidence_digest"),
            sa.CheckConstraint(
                "status IN ('PASSED', 'FAILED')",
                name="ck_profile_qualification_evidence_status",
            ),
            sa.CheckConstraint(
                "length(evidence_digest) = 71 "
                "AND evidence_digest LIKE 'sha256:%'",
                name="ck_profile_qualification_evidence_sha256",
            ),
            sa.CheckConstraint(
                "executor_image LIKE '%@sha256:%'",
                name="ck_profile_qualification_executor_digest",
            ),
            sa.CheckConstraint(
                "length(workload_digest) = 71 "
                "AND workload_digest LIKE 'sha256:%'",
                name="ck_profile_qualification_evidence_workload_sha256",
            ),
        )
    inspector = None if offline else sa.inspect(bind)
    evidence_indexes = (
        {
            item["name"]
            for item in inspector.get_indexes(
                "profile_qualification_evidence"
            )
        }
        if inspector is not None
        else set()
    )
    if "ix_profile_qualification_evidence_run" not in evidence_indexes:
        op.create_index(
            "ix_profile_qualification_evidence_run",
            "profile_qualification_evidence",
            ["run_id"],
        )

    inspector = None if offline else sa.inspect(bind)
    placement_foreign_keys = (
        {
            item["name"]
            for item in inspector.get_foreign_keys("placement_profiles")
        }
        if inspector is not None
        else set()
    )
    with op.batch_alter_table("placement_profiles") as batch:
        if (
            "fk_placement_profiles_qualification_evidence"
            not in placement_foreign_keys
        ):
            batch.create_foreign_key(
                "fk_placement_profiles_qualification_evidence",
                "profile_qualification_evidence",
                ["qualification_evidence_id"],
                ["id"],
            )
    inspector = None if offline else sa.inspect(bind)
    run_foreign_keys = (
        {
            item["name"]
            for item in inspector.get_foreign_keys(
                "profile_qualification_runs"
            )
        }
        if inspector is not None
        else set()
    )
    with op.batch_alter_table("profile_qualification_runs") as batch:
        if "fk_profile_qualification_runs_evidence" not in run_foreign_keys:
            batch.create_foreign_key(
                "fk_profile_qualification_runs_evidence",
                "profile_qualification_evidence",
                ["evidence_id"],
                ["id"],
            )


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            "offline downgrade 0012 is disabled because qualification data "
            "must be checked under a database lock"
        )
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    for statement in _destructive_downgrade_lock_sql(bind.dialect.name):
        op.execute(statement)
    data_counts = {}
    for table_name in (
        "profile_qualification_runs",
        "profile_qualification_bindings",
        "profile_qualification_evidence",
    ):
        data_counts[table_name] = (
            int(
                bind.execute(
                    sa.text(f"SELECT COUNT(*) FROM {table_name}")
                ).scalar_one()
            )
            if table_name in tables
            else 0
        )
    placement_columns = {
        item["name"] for item in inspector.get_columns("placement_profiles")
    }
    linked_count = (
        int(
            bind.execute(
                sa.text(
                    "SELECT COUNT(*) FROM placement_profiles "
                    "WHERE qualification_evidence_id IS NOT NULL "
                    "OR qualified_at IS NOT NULL OR activated_at IS NOT NULL"
                )
            ).scalar_one()
        )
        if {
            "qualification_evidence_id",
            "qualified_at",
            "activated_at",
        }
        <= placement_columns
        else 0
    )
    if any(data_counts.values()) or linked_count:
        raise RuntimeError(
            "refusing to downgrade 0012 while profile qualification data exists"
        )
    placement_foreign_keys = {
        item["name"]
        for item in inspector.get_foreign_keys("placement_profiles")
    }
    if "fk_placement_profiles_qualification_evidence" in placement_foreign_keys:
        with op.batch_alter_table("placement_profiles") as batch:
            batch.drop_constraint(
                "fk_placement_profiles_qualification_evidence",
                type_="foreignkey",
            )
    if "profile_qualification_runs" in tables:
        inspector = sa.inspect(bind)
        run_foreign_keys = {
            item["name"]
            for item in inspector.get_foreign_keys(
                "profile_qualification_runs"
            )
        }
        if "fk_profile_qualification_runs_evidence" in run_foreign_keys:
            with op.batch_alter_table(
                "profile_qualification_runs"
            ) as batch:
                batch.drop_constraint(
                    "fk_profile_qualification_runs_evidence",
                    type_="foreignkey",
                )
    inspector = sa.inspect(bind)
    if "profile_qualification_evidence" in tables:
        evidence_indexes = {
            item["name"]
            for item in inspector.get_indexes("profile_qualification_evidence")
        }
        if "ix_profile_qualification_evidence_run" in evidence_indexes:
            op.drop_index(
                "ix_profile_qualification_evidence_run",
                table_name="profile_qualification_evidence",
            )
        op.drop_table("profile_qualification_evidence")
    if "profile_qualification_bindings" in tables:
        binding_indexes = {
            item["name"]
            for item in inspector.get_indexes(
                "profile_qualification_bindings"
            )
        }
        if "ix_profile_qualification_bindings_node" in binding_indexes:
            op.drop_index(
                "ix_profile_qualification_bindings_node",
                table_name="profile_qualification_bindings",
            )
        op.drop_table("profile_qualification_bindings")
    if "profile_qualification_runs" in tables:
        run_indexes = {
            item["name"]
            for item in inspector.get_indexes("profile_qualification_runs")
        }
        for name in (
            "ix_profile_qualification_runs_status",
            "ix_profile_qualification_runs_placement",
        ):
            if name in run_indexes:
                op.drop_index(name, table_name="profile_qualification_runs")
        op.drop_table("profile_qualification_runs")
    inspector = sa.inspect(bind)
    placement_checks = {
        item["name"]
        for item in inspector.get_check_constraints("placement_profiles")
    }
    placement_columns = {
        item["name"] for item in inspector.get_columns("placement_profiles")
    }
    with op.batch_alter_table("placement_profiles") as batch:
        for name in (
            "ck_placement_auto_qualified_at",
            "ck_placement_auto_activation",
            "ck_placement_auto_evidence",
        ):
            if name in placement_checks:
                batch.drop_constraint(name, type_="check")
        for name in (
            "activated_at",
            "qualified_at",
            "qualification_evidence_id",
        ):
            if name in placement_columns:
                batch.drop_column(name)
