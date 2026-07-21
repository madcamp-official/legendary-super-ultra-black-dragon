"""Add immutable artifact manifests and content-addressed chunks.

Revision ID: 0007
Revises: 0006
"""

from alembic import op
import sqlalchemy as sa


revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


MODEL_ARTIFACT_IDENTITY_UNIQUE = "uq_model_artifacts_id_manifest_digest"


def _inspector():
    return sa.inspect(op.get_bind())


def _tables() -> set[str]:
    return set(_inspector().get_table_names())


def _unique_constraints(table: str) -> set[str]:
    return {
        item["name"]
        for item in _inspector().get_unique_constraints(table)
        if item.get("name") is not None
    }


def upgrade() -> None:
    _upgrade_model_artifact_identity()
    if "artifact_manifests" not in _tables():
        _create_artifact_manifests()
    if "artifact_manifest_files" not in _tables():
        _create_artifact_manifest_files()
    if "artifact_chunks" not in _tables():
        _create_artifact_chunks()
    if "artifact_file_chunks" not in _tables():
        _create_artifact_file_chunks()


def _upgrade_model_artifact_identity() -> None:
    if MODEL_ARTIFACT_IDENTITY_UNIQUE in _unique_constraints("model_artifacts"):
        return
    with op.batch_alter_table("model_artifacts") as batch:
        batch.create_unique_constraint(
            MODEL_ARTIFACT_IDENTITY_UNIQUE,
            ["id", "manifest_digest"],
        )


def _create_artifact_manifests() -> None:
    op.create_table(
        "artifact_manifests",
        sa.Column("digest", sa.String(length=71), primary_key=True),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("model_artifact_id", sa.String(length=36)),
        sa.Column("total_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("file_count", sa.Integer(), nullable=False),
        sa.Column("chunk_count", sa.Integer(), nullable=False),
        sa.Column("canonical_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(digest) = 71 AND digest LIKE 'sha256:%'",
            name="ck_artifact_manifest_digest_sha256",
        ),
        sa.CheckConstraint(
            "schema_version = 1",
            name="ck_artifact_manifest_schema_version",
        ),
        sa.CheckConstraint(
            "total_size_bytes > 0",
            name="ck_artifact_manifest_total_size_positive",
        ),
        sa.CheckConstraint(
            "file_count > 0",
            name="ck_artifact_manifest_file_count_positive",
        ),
        sa.CheckConstraint(
            "chunk_count > 0",
            name="ck_artifact_manifest_chunk_count_positive",
        ),
        sa.CheckConstraint(
            "length(canonical_json) > 0",
            name="ck_artifact_manifest_canonical_json_nonempty",
        ),
        sa.ForeignKeyConstraint(
            ["model_artifact_id", "digest"],
            ["model_artifacts.id", "model_artifacts.manifest_digest"],
            name="fk_artifact_manifests_model_artifact_identity",
        ),
    )
    op.create_index(
        "ix_artifact_manifests_model_artifact_id",
        "artifact_manifests",
        ["model_artifact_id"],
    )


def _create_artifact_manifest_files() -> None:
    op.create_table(
        "artifact_manifest_files",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("manifest_digest", sa.String(length=71), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("path", sa.String(length=1024), nullable=False),
        sa.Column("kind", sa.String(length=20), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("file_digest", sa.String(length=71), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(id) = 36",
            name="ck_artifact_manifest_file_id_length",
        ),
        sa.CheckConstraint(
            "ordinal >= 0",
            name="ck_artifact_manifest_file_ordinal_nonnegative",
        ),
        sa.CheckConstraint(
            "length(path) >= 1 AND length(path) <= 1024",
            name="ck_artifact_manifest_file_path_length",
        ),
        sa.CheckConstraint(
            "path NOT LIKE '/%' AND path <> '.' AND path <> '..' "
            "AND path NOT LIKE './%' AND path NOT LIKE '../%' "
            "AND path NOT LIKE '%/./%' AND path NOT LIKE '%/../%' "
            "AND path NOT LIKE '%/.' AND path NOT LIKE '%/..' "
            "AND path NOT LIKE '%//%' AND path NOT LIKE '%/'",
            name="ck_artifact_manifest_file_path_relative",
        ),
        sa.CheckConstraint(
            "kind = 'REGULAR'",
            name="ck_artifact_manifest_file_kind",
        ),
        sa.CheckConstraint(
            "size_bytes >= 0",
            name="ck_artifact_manifest_file_size_nonnegative",
        ),
        sa.CheckConstraint(
            "length(file_digest) = 71 AND file_digest LIKE 'sha256:%'",
            name="ck_artifact_manifest_file_digest_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["manifest_digest"],
            ["artifact_manifests.digest"],
            ondelete="CASCADE",
            name="fk_artifact_manifest_files_manifest_digest",
        ),
        sa.UniqueConstraint(
            "manifest_digest",
            "path",
            name="uq_artifact_manifest_files_manifest_path",
        ),
        sa.UniqueConstraint(
            "manifest_digest",
            "ordinal",
            name="uq_artifact_manifest_files_manifest_ordinal",
        ),
    )
    op.create_index(
        "ix_artifact_manifest_files_manifest_digest",
        "artifact_manifest_files",
        ["manifest_digest"],
    )


def _create_artifact_chunks() -> None:
    op.create_table(
        "artifact_chunks",
        sa.Column("digest", sa.String(length=71), primary_key=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(digest) = 71 AND digest LIKE 'sha256:%'",
            name="ck_artifact_chunk_digest_sha256",
        ),
        sa.CheckConstraint(
            "size_bytes > 0",
            name="ck_artifact_chunk_size_positive",
        ),
    )


def _create_artifact_file_chunks() -> None:
    op.create_table(
        "artifact_file_chunks",
        sa.Column("file_id", sa.String(length=36), primary_key=True),
        sa.Column("ordinal", sa.Integer(), primary_key=True),
        sa.Column("chunk_digest", sa.String(length=71), nullable=False),
        sa.Column("offset_bytes", sa.BigInteger(), nullable=False),
        sa.Column("length_bytes", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "ordinal >= 0",
            name="ck_artifact_file_chunk_ordinal_nonnegative",
        ),
        sa.CheckConstraint(
            "offset_bytes >= 0",
            name="ck_artifact_file_chunk_offset_nonnegative",
        ),
        sa.CheckConstraint(
            "length_bytes > 0",
            name="ck_artifact_file_chunk_length_positive",
        ),
        sa.ForeignKeyConstraint(
            ["file_id"],
            ["artifact_manifest_files.id"],
            ondelete="CASCADE",
            name="fk_artifact_file_chunks_file_id",
        ),
        sa.ForeignKeyConstraint(
            ["chunk_digest"],
            ["artifact_chunks.digest"],
            name="fk_artifact_file_chunks_chunk_digest",
        ),
        sa.UniqueConstraint(
            "file_id",
            "offset_bytes",
            name="uq_artifact_file_chunks_file_offset",
        ),
    )
    op.create_index(
        "ix_artifact_file_chunks_chunk_digest",
        "artifact_file_chunks",
        ["chunk_digest"],
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
            "artifact_file_chunks",
            "artifact_manifest_files",
            "artifact_chunks",
            "artifact_manifests",
        )
        if table in tables and _scalar_count(table) > 0
    ]
    if populated:
        raise RuntimeError(
            "refusing to downgrade 0007 while artifact manifest data exists: "
            + ", ".join(populated)
        )


def downgrade() -> None:
    _refuse_destructive_downgrade()
    tables = _tables()
    for table in (
        "artifact_file_chunks",
        "artifact_manifest_files",
        "artifact_chunks",
        "artifact_manifests",
    ):
        if table in tables:
            op.drop_table(table)
    if (
        "model_artifacts" in _tables()
        and MODEL_ARTIFACT_IDENTITY_UNIQUE
        in _unique_constraints("model_artifacts")
    ):
        with op.batch_alter_table("model_artifacts") as batch:
            batch.drop_constraint(
                MODEL_ARTIFACT_IDENTITY_UNIQUE,
                type_="unique",
            )
