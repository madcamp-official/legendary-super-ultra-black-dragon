"""Persist authoritative node artifact-cache state and append-only events.

Revision ID: 0010
Revises: 0009
"""

from alembic import op
import sqlalchemy as sa


revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


_APPEND_ONLY_MESSAGE = "artifact_cache_events is append-only"
_POSTGRESQL_GUARD_FUNCTION = "dure_artifact_cache_events_append_only_guard"
_APPEND_ONLY_ROW_OPERATIONS = ("UPDATE", "DELETE")
_SQLITE_NO_REPLACE_TRIGGER = "trg_artifact_cache_events_no_replace"
_POSTGRESQL_NO_TRUNCATE_TRIGGER = (
    "trg_artifact_cache_events_no_truncate"
)


def _append_only_guard_upgrade_sql(dialect_name: str) -> tuple[str, ...]:
    if dialect_name == "sqlite":
        row_guards = tuple(
            f"""
CREATE TRIGGER IF NOT EXISTS trg_artifact_cache_events_no_{operation.lower()}
BEFORE {operation} ON artifact_cache_events
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, '{_APPEND_ONLY_MESSAGE}');
END
""".strip()
            for operation in _APPEND_ONLY_ROW_OPERATIONS
        )
        # With SQLite's default recursive_triggers=OFF, INSERT OR REPLACE can
        # delete a conflicting row without firing its DELETE trigger.  Reject
        # every primary/replay-key collision before replacement is considered.
        return row_guards + (
            f"""
CREATE TRIGGER IF NOT EXISTS {_SQLITE_NO_REPLACE_TRIGGER}
BEFORE INSERT ON artifact_cache_events
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM artifact_cache_events AS existing
    WHERE existing.id = NEW.id
       OR (existing.cache_id = NEW.cache_id
           AND existing.sequence = NEW.sequence)
       OR (existing.cache_id = NEW.cache_id
           AND existing.source_kind = NEW.source_kind
           AND existing.source_id = NEW.source_id
           AND existing.reason_code = NEW.reason_code)
)
BEGIN
    SELECT RAISE(ABORT, '{_APPEND_ONLY_MESSAGE}');
END
""".strip(),
        )
    if dialect_name == "postgresql":
        statements = [
            f"""
CREATE OR REPLACE FUNCTION {_POSTGRESQL_GUARD_FUNCTION}()
RETURNS trigger
LANGUAGE plpgsql
AS $dure$
BEGIN
    RAISE EXCEPTION '{_APPEND_ONLY_MESSAGE}'
        USING ERRCODE = '23514';
END;
$dure$
""".strip()
        ]
        for operation in _APPEND_ONLY_ROW_OPERATIONS:
            trigger_name = (
                f"trg_artifact_cache_events_no_{operation.lower()}"
            )
            statements.extend(
                (
                    f"DROP TRIGGER IF EXISTS {trigger_name} "
                    "ON artifact_cache_events",
                    f"""
CREATE TRIGGER {trigger_name}
BEFORE {operation} ON artifact_cache_events
FOR EACH ROW
EXECUTE FUNCTION {_POSTGRESQL_GUARD_FUNCTION}()
""".strip(),
                )
            )
        statements.extend(
            (
                f"DROP TRIGGER IF EXISTS "
                f"{_POSTGRESQL_NO_TRUNCATE_TRIGGER} "
                "ON artifact_cache_events",
                f"""
CREATE TRIGGER {_POSTGRESQL_NO_TRUNCATE_TRIGGER}
BEFORE TRUNCATE ON artifact_cache_events
FOR EACH STATEMENT
EXECUTE FUNCTION {_POSTGRESQL_GUARD_FUNCTION}()
""".strip(),
            )
        )
        return tuple(statements)
    raise RuntimeError(
        "0010 append-only guard supports only SQLite and PostgreSQL, got: "
        + dialect_name
    )


def _append_only_guard_downgrade_sql(dialect_name: str) -> tuple[str, ...]:
    if dialect_name == "sqlite":
        return ()
    if dialect_name == "postgresql":
        return (
            "DROP FUNCTION IF EXISTS "
            f"{_POSTGRESQL_GUARD_FUNCTION}()",
        )
    raise RuntimeError(
        "0010 append-only guard supports only SQLite and PostgreSQL, got: "
        + dialect_name
    )


def _execute_guard_sql(statements: tuple[str, ...]) -> None:
    for statement in statements:
        op.execute(sa.text(statement))


def _install_append_only_guard() -> None:
    _execute_guard_sql(
        _append_only_guard_upgrade_sql(op.get_bind().dialect.name)
    )


def _remove_append_only_guard() -> None:
    _execute_guard_sql(
        _append_only_guard_downgrade_sql(op.get_bind().dialect.name)
    )


def _inspector():
    return sa.inspect(op.get_bind())


def _tables() -> set[str]:
    return set(_inspector().get_table_names())


def _unique_names(table: str) -> set[str]:
    if table not in _tables():
        return set()
    return {
        item["name"]
        for item in _inspector().get_unique_constraints(table)
        if item.get("name")
    }


def _column_names(table: str) -> set[str]:
    if table not in _tables():
        return set()
    return {item["name"] for item in _inspector().get_columns(table)}


def upgrade() -> None:
    if (
        "artifact_preparation_attempts" in _tables()
        and "download_progress"
        not in _column_names("artifact_preparation_attempts")
    ):
        with op.batch_alter_table("artifact_preparation_attempts") as batch:
            batch.add_column(
                sa.Column(
                    "download_progress",
                    sa.JSON(none_as_null=True),
                )
            )
    if (
        "stage_artifact_variants" in _tables()
        and "uq_stage_variant_set_source"
        not in _unique_names("stage_artifact_variants")
    ):
        with op.batch_alter_table("stage_artifact_variants") as batch:
            batch.create_unique_constraint(
                "uq_stage_variant_set_source",
                ["artifact_set_digest", "source_manifest_digest"],
            )
    if "node_artifact_caches" not in _tables():
        _create_node_artifact_caches()
    if "artifact_cache_events" not in _tables():
        _create_artifact_cache_events()
    _install_append_only_guard()


def _create_node_artifact_caches() -> None:
    op.create_table(
        "node_artifact_caches",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("node_id", sa.String(length=36), nullable=False),
        sa.Column("cache_kind", sa.String(length=20), nullable=False),
        sa.Column("cache_identity_digest", sa.String(length=71), nullable=False),
        sa.Column("manifest_digest", sa.String(length=71), nullable=False),
        sa.Column("source_manifest_digest", sa.String(length=71), nullable=False),
        sa.Column("artifact_set_digest", sa.String(length=71)),
        sa.Column("artifact_rank", sa.Integer()),
        sa.Column("pipeline_rank", sa.Integer()),
        sa.Column("tensor_rank", sa.Integer()),
        sa.Column("tensor_parallel_size", sa.Integer()),
        sa.Column("pipeline_parallel_size", sa.Integer()),
        sa.Column("tensor_keys_digest", sa.String(length=71)),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=False),
        sa.Column("last_ready_attempt_id", sa.String(length=36)),
        sa.Column("verified_at", sa.DateTime(timezone=True)),
        sa.Column("verified_size_bytes", sa.BigInteger()),
        sa.Column("verified_file_count", sa.Integer()),
        sa.Column("verification_version", sa.Integer()),
        sa.Column("last_probe_observed_at", sa.DateTime(timezone=True)),
        sa.Column("quarantine_request_id", sa.String(length=36)),
        sa.Column("quarantined_at", sa.DateTime(timezone=True)),
        sa.Column("event_sequence", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(id) = 36",
            name="ck_node_artifact_cache_id_length",
        ),
        sa.CheckConstraint(
            "cache_kind IN ('FULL_SNAPSHOT', 'STAGE')",
            name="ck_node_artifact_cache_kind",
        ),
        sa.CheckConstraint(
            "length(cache_identity_digest) = 71 "
            "AND cache_identity_digest LIKE 'sha256:%'",
            name="ck_node_artifact_cache_identity_sha256",
        ),
        sa.CheckConstraint(
            "length(manifest_digest) = 71 "
            "AND manifest_digest LIKE 'sha256:%'",
            name="ck_node_artifact_cache_manifest_sha256",
        ),
        sa.CheckConstraint(
            "length(source_manifest_digest) = 71 "
            "AND source_manifest_digest LIKE 'sha256:%'",
            name="ck_node_artifact_cache_source_sha256",
        ),
        sa.CheckConstraint(
            "artifact_set_digest IS NULL OR "
            "(length(artifact_set_digest) = 71 "
            "AND artifact_set_digest LIKE 'sha256:%')",
            name="ck_node_artifact_cache_variant_sha256",
        ),
        sa.CheckConstraint(
            "tensor_keys_digest IS NULL OR "
            "(length(tensor_keys_digest) = 71 "
            "AND tensor_keys_digest LIKE 'sha256:%')",
            name="ck_node_artifact_cache_tensor_keys_sha256",
        ),
        sa.CheckConstraint(
            "(cache_kind = 'FULL_SNAPSHOT' "
            "AND cache_identity_digest = manifest_digest "
            "AND source_manifest_digest = manifest_digest "
            "AND artifact_set_digest IS NULL "
            "AND artifact_rank IS NULL "
            "AND pipeline_rank IS NULL "
            "AND tensor_rank IS NULL "
            "AND tensor_parallel_size IS NULL "
            "AND pipeline_parallel_size IS NULL "
            "AND tensor_keys_digest IS NULL) OR "
            "(cache_kind = 'STAGE' "
            "AND artifact_set_digest IS NOT NULL "
            "AND artifact_rank IS NOT NULL "
            "AND pipeline_rank IS NOT NULL "
            "AND tensor_rank IS NOT NULL "
            "AND tensor_parallel_size IS NOT NULL "
            "AND pipeline_parallel_size IS NOT NULL "
            "AND tensor_keys_digest IS NOT NULL "
            "AND tensor_parallel_size = 1 "
            "AND tensor_rank = 0 "
            "AND artifact_rank = pipeline_rank "
            "AND pipeline_rank >= 0 "
            "AND pipeline_rank < pipeline_parallel_size "
            "AND pipeline_parallel_size >= 1 "
            "AND pipeline_parallel_size <= 64)",
            name="ck_node_artifact_cache_identity_shape",
        ),
        sa.CheckConstraint(
            "status IN ('READY', 'STALE', 'MISSING', 'CORRUPT', "
            "'QUARANTINED')",
            name="ck_node_artifact_cache_status",
        ),
        sa.CheckConstraint(
            "(status = 'READY' AND reason_code = 'PREPARATION_SUCCEEDED') OR "
            "(status = 'STALE' AND reason_code IN ("
            "'PROBE_IDENTITY_MISMATCH', 'VARIANT_REVOKED', "
            "'QUARANTINE_REQUESTED', 'QUARANTINE_FAILED')) OR "
            "(status = 'MISSING' AND reason_code = 'PROBE_MISSING') OR "
            "(status = 'CORRUPT' AND reason_code IN ("
            "'PROBE_UNSAFE', 'PROBE_CORRUPT', 'VERIFICATION_FAILED')) OR "
            "(status = 'QUARANTINED' "
            "AND reason_code = 'QUARANTINE_SUCCEEDED')",
            name="ck_node_artifact_cache_status_reason",
        ),
        sa.CheckConstraint(
            "verification_version IS NULL OR verification_version = 1",
            name="ck_node_artifact_cache_verification_version",
        ),
        sa.CheckConstraint(
            "verified_size_bytes IS NULL OR verified_size_bytes > 0",
            name="ck_node_artifact_cache_verified_size_positive",
        ),
        sa.CheckConstraint(
            "verified_file_count IS NULL OR verified_file_count > 0",
            name="ck_node_artifact_cache_verified_files_positive",
        ),
        sa.CheckConstraint(
            "(last_ready_attempt_id IS NULL AND verified_at IS NULL "
            "AND verified_size_bytes IS NULL AND verified_file_count IS NULL "
            "AND verification_version IS NULL) OR "
            "(last_ready_attempt_id IS NOT NULL AND verified_at IS NOT NULL "
            "AND verified_size_bytes IS NOT NULL "
            "AND verified_file_count IS NOT NULL "
            "AND verification_version IS NOT NULL)",
            name="ck_node_artifact_cache_verification_shape",
        ),
        sa.CheckConstraint(
            "status <> 'READY' OR last_ready_attempt_id IS NOT NULL",
            name="ck_node_artifact_cache_ready_evidence",
        ),
        sa.CheckConstraint(
            "quarantine_request_id IS NULL OR length(quarantine_request_id) = 36",
            name="ck_node_artifact_cache_quarantine_request_length",
        ),
        sa.CheckConstraint(
            "(status = 'QUARANTINED' AND quarantined_at IS NOT NULL "
            "AND quarantine_request_id IS NULL) OR "
            "(status <> 'QUARANTINED' AND quarantined_at IS NULL)",
            name="ck_node_artifact_cache_quarantine_shape",
        ),
        sa.CheckConstraint(
            "event_sequence >= 0",
            name="ck_node_artifact_cache_event_sequence_nonnegative",
        ),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["nodes.id"],
            name="fk_node_artifact_caches_node_id",
        ),
        sa.ForeignKeyConstraint(
            ["manifest_digest"],
            ["artifact_manifests.digest"],
            name="fk_node_artifact_caches_manifest_digest",
        ),
        sa.ForeignKeyConstraint(
            ["source_manifest_digest"],
            ["artifact_manifests.digest"],
            name="fk_node_artifact_caches_source_manifest_digest",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_set_digest", "source_manifest_digest"],
            [
                "stage_artifact_variants.artifact_set_digest",
                "stage_artifact_variants.source_manifest_digest",
            ],
            name="fk_node_artifact_cache_stage_source",
        ),
        sa.ForeignKeyConstraint(
            [
                "artifact_set_digest",
                "tensor_parallel_size",
                "pipeline_parallel_size",
            ],
            [
                "stage_artifact_variants.artifact_set_digest",
                "stage_artifact_variants.tensor_parallel_size",
                "stage_artifact_variants.pipeline_parallel_size",
            ],
            name="fk_node_artifact_cache_stage_topology",
        ),
        sa.ForeignKeyConstraint(
            [
                "artifact_set_digest",
                "artifact_rank",
                "manifest_digest",
                "tensor_keys_digest",
            ],
            [
                "stage_artifact_ranks.variant_id",
                "stage_artifact_ranks.rank",
                "stage_artifact_ranks.manifest_digest",
                "stage_artifact_ranks.tensor_keys_digest",
            ],
            name="fk_node_artifact_cache_stage_rank",
        ),
        sa.ForeignKeyConstraint(
            ["last_ready_attempt_id"],
            ["artifact_preparation_attempts.id"],
            name="fk_node_artifact_caches_ready_attempt_id",
        ),
        sa.UniqueConstraint(
            "node_id",
            "cache_identity_digest",
            name="uq_node_artifact_caches_node_identity",
        ),
        sa.UniqueConstraint(
            "last_ready_attempt_id",
            name="uq_node_artifact_caches_ready_attempt",
        ),
    )
    op.create_index(
        "ix_node_artifact_caches_node_status",
        "node_artifact_caches",
        ["node_id", "status"],
    )
    op.create_index(
        "ix_node_artifact_caches_manifest_status",
        "node_artifact_caches",
        ["manifest_digest", "status"],
    )
    op.create_index(
        "ix_node_artifact_caches_variant_status",
        "node_artifact_caches",
        ["artifact_set_digest", "status"],
    )


def _create_artifact_cache_events() -> None:
    op.create_table(
        "artifact_cache_events",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("cache_id", sa.String(length=36), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("previous_status", sa.String(length=20)),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=False),
        sa.Column("source_kind", sa.String(length=20), nullable=False),
        sa.Column("source_id", sa.String(length=255), nullable=False),
        sa.Column("source_attempt_id", sa.String(length=36)),
        sa.Column("source_task_id", sa.String(length=36)),
        sa.Column("evidence_kind", sa.String(length=32), nullable=False),
        sa.Column("evidence_digest", sa.String(length=71), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(id) = 36",
            name="ck_artifact_cache_event_id_length",
        ),
        sa.CheckConstraint(
            "sequence > 0",
            name="ck_artifact_cache_event_sequence_positive",
        ),
        sa.CheckConstraint(
            "(sequence = 1 AND previous_status IS NULL) OR "
            "(sequence > 1 AND previous_status IS NOT NULL)",
            name="ck_artifact_cache_event_previous_status",
        ),
        sa.CheckConstraint(
            "previous_status IS NULL OR previous_status IN ("
            "'READY', 'STALE', 'MISSING', 'CORRUPT', 'QUARANTINED')",
            name="ck_artifact_cache_event_previous_status_value",
        ),
        sa.CheckConstraint(
            "status IN ('READY', 'STALE', 'MISSING', 'CORRUPT', "
            "'QUARANTINED')",
            name="ck_artifact_cache_event_status",
        ),
        sa.CheckConstraint(
            "reason_code IN ("
            "'PREPARATION_SUCCEEDED', 'PROBE_UNSAFE', 'PROBE_CORRUPT', "
            "'PROBE_IDENTITY_MISMATCH', 'PROBE_MISSING', "
            "'VARIANT_REVOKED', 'VERIFICATION_FAILED', "
            "'QUARANTINE_REQUESTED', 'QUARANTINE_SUCCEEDED', "
            "'QUARANTINE_FAILED')",
            name="ck_artifact_cache_event_reason",
        ),
        sa.CheckConstraint(
            "source_kind IN ("
            "'PREPARATION', 'PROBE', 'VARIANT', 'VERIFICATION', "
            "'QUARANTINE')",
            name="ck_artifact_cache_event_source_kind",
        ),
        sa.CheckConstraint(
            "length(source_id) > 0 AND length(source_id) <= 255",
            name="ck_artifact_cache_event_source_id",
        ),
        sa.CheckConstraint(
            "evidence_kind IN ("
            "'PREPARATION_RESULT', 'PROBE_OBSERVATION', "
            "'STAGE_VARIANT_STATUS', 'RUNTIME_VERIFICATION', "
            "'QUARANTINE_REQUEST', 'QUARANTINE_RESULT')",
            name="ck_artifact_cache_event_evidence_kind",
        ),
        sa.CheckConstraint(
            "length(evidence_digest) = 71 "
            "AND evidence_digest LIKE 'sha256:%'",
            name="ck_artifact_cache_event_evidence_sha256",
        ),
        sa.CheckConstraint(
            "(source_kind = 'PREPARATION' "
            "AND reason_code = 'PREPARATION_SUCCEEDED' "
            "AND source_attempt_id IS NOT NULL "
            "AND source_task_id IS NOT NULL "
            "AND evidence_kind = 'PREPARATION_RESULT') OR "
            "(source_kind = 'PROBE' "
            "AND reason_code IN ("
            "'PROBE_UNSAFE', 'PROBE_CORRUPT', "
            "'PROBE_IDENTITY_MISMATCH', 'PROBE_MISSING') "
            "AND source_attempt_id IS NULL "
            "AND evidence_kind = 'PROBE_OBSERVATION') OR "
            "(source_kind = 'VARIANT' "
            "AND reason_code = 'VARIANT_REVOKED' "
            "AND source_attempt_id IS NULL "
            "AND source_task_id IS NULL "
            "AND evidence_kind = 'STAGE_VARIANT_STATUS') OR "
            "(source_kind = 'VERIFICATION' "
            "AND reason_code = 'VERIFICATION_FAILED' "
            "AND source_attempt_id IS NULL "
            "AND evidence_kind = 'RUNTIME_VERIFICATION') OR "
            "(source_kind = 'QUARANTINE' "
            "AND reason_code = 'QUARANTINE_REQUESTED' "
            "AND source_attempt_id IS NULL "
            "AND evidence_kind = 'QUARANTINE_REQUEST') OR "
            "(source_kind = 'QUARANTINE' "
            "AND reason_code IN ("
            "'QUARANTINE_SUCCEEDED', 'QUARANTINE_FAILED') "
            "AND source_attempt_id IS NULL "
            "AND evidence_kind = 'QUARANTINE_RESULT')",
            name="ck_artifact_cache_event_closed_source",
        ),
        sa.ForeignKeyConstraint(
            ["cache_id"],
            ["node_artifact_caches.id"],
            name="fk_artifact_cache_events_cache_id",
        ),
        sa.ForeignKeyConstraint(
            ["source_attempt_id"],
            ["artifact_preparation_attempts.id"],
            name="fk_artifact_cache_events_source_attempt_id",
        ),
        sa.ForeignKeyConstraint(
            ["source_task_id"],
            ["tasks.id"],
            name="fk_artifact_cache_events_source_task_id",
        ),
        sa.UniqueConstraint(
            "cache_id",
            "sequence",
            name="uq_artifact_cache_events_cache_sequence",
        ),
        sa.UniqueConstraint(
            "cache_id",
            "source_kind",
            "source_id",
            "reason_code",
            name="uq_artifact_cache_events_source_replay",
        ),
        sqlite_with_rowid=False,
    )
    op.create_index(
        "ix_artifact_cache_events_cache_created",
        "artifact_cache_events",
        ["cache_id", "created_at"],
    )
    op.create_index(
        "ix_artifact_cache_events_source_task",
        "artifact_cache_events",
        ["source_task_id"],
    )


def _scalar_count(table: str) -> int:
    return int(
        op.get_bind()
        .execute(sa.text(f"SELECT COUNT(*) FROM {table}"))
        .scalar_one()
    )


def _destructive_downgrade_lock_sql(
    dialect_name: str, tables: tuple[str, ...]
) -> tuple[str, ...]:
    if dialect_name == "sqlite":
        return ()
    if dialect_name == "postgresql":
        return (
            "LOCK TABLE "
            + ", ".join(tables)
            + " IN ACCESS EXCLUSIVE MODE",
        ) if tables else ()
    raise RuntimeError(
        "0010 destructive downgrade supports only SQLite and PostgreSQL, got: "
        + dialect_name
    )


def _lock_destructive_downgrade_inputs(tables: set[str]) -> None:
    lockable = tuple(
        table
        for table in (
            "artifact_preparation_attempts",
            "node_artifact_caches",
            "artifact_cache_events",
        )
        if table in tables
    )
    _execute_guard_sql(
        _destructive_downgrade_lock_sql(
            op.get_bind().dialect.name,
            lockable,
        )
    )


def _refuse_destructive_downgrade() -> None:
    tables = _tables()
    _lock_destructive_downgrade_inputs(tables)
    populated = [
        table
        for table in ("artifact_cache_events", "node_artifact_caches")
        if table in tables and _scalar_count(table) > 0
    ]
    if (
        "artifact_preparation_attempts" in tables
        and "download_progress"
        in _column_names("artifact_preparation_attempts")
        and int(
            op.get_bind()
            .execute(
                sa.text(
                    "SELECT COUNT(*) FROM artifact_preparation_attempts "
                    "WHERE download_progress IS NOT NULL "
                    "AND CAST(download_progress AS TEXT) <> 'null'"
                )
            )
            .scalar_one()
        )
        > 0
    ):
        populated.append("artifact_preparation_attempts.download_progress")
    if populated:
        raise RuntimeError(
            "refusing to downgrade 0010 while artifact cache lifecycle "
            "or download progress data exists: "
            + ", ".join(populated)
        )


def downgrade() -> None:
    _refuse_destructive_downgrade()
    tables = _tables()
    if "artifact_cache_events" in tables:
        op.drop_table("artifact_cache_events")
    _remove_append_only_guard()
    if "node_artifact_caches" in tables:
        op.drop_table("node_artifact_caches")
    if (
        "artifact_preparation_attempts" in _tables()
        and "download_progress"
        in _column_names("artifact_preparation_attempts")
    ):
        with op.batch_alter_table("artifact_preparation_attempts") as batch:
            batch.drop_column("download_progress")
    if (
        "stage_artifact_variants" in _tables()
        and "uq_stage_variant_set_source"
        in _unique_names("stage_artifact_variants")
    ):
        with op.batch_alter_table("stage_artifact_variants") as batch:
            batch.drop_constraint(
                "uq_stage_variant_set_source",
                type_="unique",
            )
