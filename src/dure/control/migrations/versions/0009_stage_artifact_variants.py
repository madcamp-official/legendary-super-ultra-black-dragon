"""Register immutable vLLM stage artifact variants and validation evidence.

Revision ID: 0009
Revises: 0008
"""

from alembic import op
import sqlalchemy as sa


revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def _inspector():
    return sa.inspect(op.get_bind())


def _tables() -> set[str]:
    return set(_inspector().get_table_names())


def upgrade() -> None:
    if "stage_artifact_variants" not in _tables():
        _create_stage_artifact_variants()
    if "stage_artifact_ranks" not in _tables():
        _create_stage_artifact_ranks()
    if "stage_artifact_validation_evidence" not in _tables():
        _create_stage_artifact_validation_evidence()
    if "stage_artifact_validation_ranks" not in _tables():
        _create_stage_artifact_validation_ranks()


def _create_stage_artifact_variants() -> None:
    op.create_table(
        "stage_artifact_variants",
        sa.Column("artifact_set_digest", sa.String(length=71), primary_key=True),
        sa.Column("contract_identity_digest", sa.String(length=71), nullable=False),
        sa.Column("source_manifest_digest", sa.String(length=71), nullable=False),
        sa.Column("runtime_release_id", sa.String(length=36), nullable=False),
        sa.Column("runtime_image", sa.String(length=512), nullable=False),
        sa.Column("vllm_version", sa.String(length=64), nullable=False),
        sa.Column("exporter_build_digest", sa.String(length=71), nullable=False),
        sa.Column("architecture", sa.String(length=100), nullable=False),
        sa.Column("quantization", sa.String(length=40), nullable=False),
        sa.Column("tensor_parallel_size", sa.Integer(), nullable=False),
        sa.Column("pipeline_parallel_size", sa.Integer(), nullable=False),
        sa.Column("rank_count", sa.Integer(), nullable=False),
        sa.Column("loader_format", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("canonical_identity_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("validated_at", sa.DateTime(timezone=True)),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "length(artifact_set_digest) = 71 "
            "AND artifact_set_digest LIKE 'sha256:%'",
            name="ck_stage_variant_set_sha256",
        ),
        sa.CheckConstraint(
            "length(contract_identity_digest) = 71 "
            "AND contract_identity_digest LIKE 'sha256:%'",
            name="ck_stage_variant_contract_sha256",
        ),
        sa.CheckConstraint(
            "length(source_manifest_digest) = 71 "
            "AND source_manifest_digest LIKE 'sha256:%'",
            name="ck_stage_variant_source_sha256",
        ),
        sa.CheckConstraint(
            "runtime_image LIKE '%@sha256:" + "_" * 64 + "'",
            name="ck_stage_variant_runtime_digest",
        ),
        sa.CheckConstraint(
            "vllm_version = '0.9.0'",
            name="ck_stage_variant_vllm_version",
        ),
        sa.CheckConstraint(
            "length(exporter_build_digest) = 71 "
            "AND exporter_build_digest LIKE 'sha256:%'",
            name="ck_stage_variant_exporter_sha256",
        ),
        sa.CheckConstraint(
            "architecture = 'Qwen2ForCausalLM'",
            name="ck_stage_variant_architecture",
        ),
        sa.CheckConstraint(
            "quantization = 'awq'",
            name="ck_stage_variant_quantization",
        ),
        sa.CheckConstraint(
            "tensor_parallel_size = 1",
            name="ck_stage_variant_tp_supported",
        ),
        sa.CheckConstraint(
            "pipeline_parallel_size > 0 AND pipeline_parallel_size <= 64",
            name="ck_stage_variant_pp_range",
        ),
        sa.CheckConstraint(
            "rank_count = tensor_parallel_size * pipeline_parallel_size",
            name="ck_stage_variant_rank_count",
        ),
        sa.CheckConstraint(
            "loader_format = 'VLLM_SHARDED_STATE_V1'",
            name="ck_stage_variant_loader_format",
        ),
        sa.CheckConstraint(
            "status IN ('DRAFT', 'VALIDATED', 'REVOKED')",
            name="ck_stage_variant_status",
        ),
        sa.CheckConstraint(
            "length(canonical_identity_json) > 0",
            name="ck_stage_variant_identity_json_nonempty",
        ),
        sa.CheckConstraint(
            "(status = 'DRAFT' AND validated_at IS NULL AND revoked_at IS NULL) OR "
            "(status = 'VALIDATED' AND validated_at IS NOT NULL AND revoked_at IS NULL) OR "
            "(status = 'REVOKED' AND revoked_at IS NOT NULL)",
            name="ck_stage_variant_status_timestamps",
        ),
        sa.ForeignKeyConstraint(
            ["source_manifest_digest"],
            ["artifact_manifests.digest"],
            name="fk_stage_variant_source_manifest",
        ),
        sa.ForeignKeyConstraint(
            ["runtime_release_id"],
            ["runtime_releases.id"],
            name="fk_stage_variant_runtime_release",
        ),
        sa.UniqueConstraint(
            "artifact_set_digest",
            "tensor_parallel_size",
            "pipeline_parallel_size",
            name="uq_stage_variant_set_topology",
        ),
        sa.UniqueConstraint(
            "contract_identity_digest",
            name="uq_stage_variant_contract_identity",
        ),
    )
    op.create_index(
        "ix_stage_variants_source_manifest",
        "stage_artifact_variants",
        ["source_manifest_digest"],
    )
    op.create_index(
        "ix_stage_variants_runtime_release",
        "stage_artifact_variants",
        ["runtime_release_id"],
    )
    op.create_index(
        "ix_stage_variants_status",
        "stage_artifact_variants",
        ["status"],
    )


def _create_stage_artifact_ranks() -> None:
    op.create_table(
        "stage_artifact_ranks",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("variant_id", sa.String(length=71), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("pipeline_rank", sa.Integer(), nullable=False),
        sa.Column("tensor_rank", sa.Integer(), nullable=False),
        sa.Column("tensor_parallel_size", sa.Integer(), nullable=False),
        sa.Column("pipeline_parallel_size", sa.Integer(), nullable=False),
        sa.Column("manifest_digest", sa.String(length=71), nullable=False),
        sa.Column("tensor_key_count", sa.Integer(), nullable=False),
        sa.Column("tensor_keys_digest", sa.String(length=71), nullable=False),
        sa.Column("weight_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(id) = 36",
            name="ck_stage_rank_id_length",
        ),
        sa.CheckConstraint(
            "rank >= 0 AND rank = pipeline_rank * tensor_parallel_size + tensor_rank",
            name="ck_stage_rank_linear_coordinate",
        ),
        sa.CheckConstraint(
            "pipeline_rank >= 0 AND pipeline_rank < pipeline_parallel_size",
            name="ck_stage_rank_pipeline_range",
        ),
        sa.CheckConstraint(
            "tensor_rank >= 0 AND tensor_rank < tensor_parallel_size",
            name="ck_stage_rank_tensor_range",
        ),
        sa.CheckConstraint(
            "tensor_parallel_size = 1 AND pipeline_parallel_size > 0",
            name="ck_stage_rank_supported_topology",
        ),
        sa.CheckConstraint(
            "length(manifest_digest) = 71 AND manifest_digest LIKE 'sha256:%'",
            name="ck_stage_rank_manifest_sha256",
        ),
        sa.CheckConstraint(
            "tensor_key_count > 0",
            name="ck_stage_rank_tensor_count_positive",
        ),
        sa.CheckConstraint(
            "length(tensor_keys_digest) = 71 "
            "AND tensor_keys_digest LIKE 'sha256:%'",
            name="ck_stage_rank_tensor_keys_sha256",
        ),
        sa.CheckConstraint(
            "weight_size_bytes > 0",
            name="ck_stage_rank_weight_size_positive",
        ),
        sa.ForeignKeyConstraint(
            ["variant_id", "tensor_parallel_size", "pipeline_parallel_size"],
            [
                "stage_artifact_variants.artifact_set_digest",
                "stage_artifact_variants.tensor_parallel_size",
                "stage_artifact_variants.pipeline_parallel_size",
            ],
            ondelete="CASCADE",
            name="fk_stage_rank_variant_topology",
        ),
        sa.ForeignKeyConstraint(
            ["manifest_digest"],
            ["artifact_manifests.digest"],
            name="fk_stage_rank_manifest",
        ),
        sa.UniqueConstraint(
            "variant_id",
            "rank",
            name="uq_stage_rank_variant_rank",
        ),
        sa.UniqueConstraint(
            "variant_id",
            "pipeline_rank",
            "tensor_rank",
            name="uq_stage_rank_variant_coordinate",
        ),
        sa.UniqueConstraint(
            "variant_id",
            "manifest_digest",
            name="uq_stage_rank_variant_manifest",
        ),
        sa.UniqueConstraint(
            "variant_id",
            "rank",
            "manifest_digest",
            "tensor_keys_digest",
            name="uq_stage_rank_evidence_identity",
        ),
    )
    op.create_index(
        "ix_stage_ranks_manifest_digest",
        "stage_artifact_ranks",
        ["manifest_digest"],
    )


def _create_stage_artifact_validation_evidence() -> None:
    op.create_table(
        "stage_artifact_validation_evidence",
        sa.Column("identity_digest", sa.String(length=71), primary_key=True),
        sa.Column("variant_id", sa.String(length=71), nullable=False),
        sa.Column("validation_run_id", sa.String(length=36), nullable=False),
        sa.Column("registration_sequence", sa.Integer(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("validator_version", sa.String(length=64), nullable=False),
        sa.Column("validator_build_digest", sa.String(length=71), nullable=False),
        sa.Column("rank_count", sa.Integer(), nullable=False),
        sa.Column("failure_code", sa.String(length=64)),
        sa.Column("canonical_evidence_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(identity_digest) = 71 AND identity_digest LIKE 'sha256:%'",
            name="ck_stage_evidence_identity_sha256",
        ),
        sa.CheckConstraint(
            "length(validation_run_id) = 36",
            name="ck_stage_evidence_run_id_length",
        ),
        sa.CheckConstraint(
            "registration_sequence > 0",
            name="ck_stage_evidence_sequence_positive",
        ),
        sa.CheckConstraint(
            "schema_version = 1",
            name="ck_stage_evidence_schema_version",
        ),
        sa.CheckConstraint(
            "kind IN ('SYNTHETIC', 'GPU_EXPORT_LOAD')",
            name="ck_stage_evidence_kind",
        ),
        sa.CheckConstraint(
            "status IN ('PASSED', 'FAILED', 'NOT_RUN')",
            name="ck_stage_evidence_status",
        ),
        sa.CheckConstraint(
            "length(validator_version) > 0",
            name="ck_stage_evidence_validator_nonempty",
        ),
        sa.CheckConstraint(
            "length(validator_build_digest) = 71 "
            "AND validator_build_digest LIKE 'sha256:%'",
            name="ck_stage_evidence_validator_sha256",
        ),
        sa.CheckConstraint(
            "(status = 'PASSED' AND rank_count > 0 AND failure_code IS NULL) OR "
            "(status IN ('FAILED', 'NOT_RUN') AND rank_count >= 0 "
            "AND failure_code IS NOT NULL)",
            name="ck_stage_evidence_result_shape",
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR failure_code IN ("
            "'STAGE_EXPORT_FAILED', 'STAGE_LOAD_FAILED', "
            "'STAGE_TENSOR_COVERAGE_INVALID', 'STAGE_MANIFEST_MISMATCH', "
            "'STAGE_TOPOLOGY_MISMATCH', 'STAGE_GPU_NOT_AVAILABLE', "
            "'STAGE_VALIDATION_NOT_RUN')",
            name="ck_stage_evidence_failure_code",
        ),
        sa.CheckConstraint(
            "length(canonical_evidence_json) > 0",
            name="ck_stage_evidence_json_nonempty",
        ),
        sa.ForeignKeyConstraint(
            ["variant_id"],
            ["stage_artifact_variants.artifact_set_digest"],
            ondelete="CASCADE",
            name="fk_stage_evidence_variant",
        ),
        sa.UniqueConstraint(
            "variant_id",
            "registration_sequence",
            name="uq_stage_evidence_variant_sequence",
        ),
        sa.UniqueConstraint(
            "variant_id",
            "validation_run_id",
            name="uq_stage_evidence_variant_run",
        ),
        sa.UniqueConstraint(
            "identity_digest",
            "variant_id",
            name="uq_stage_evidence_identity_variant",
        ),
    )
    op.create_index(
        "ix_stage_evidence_variant_kind_sequence",
        "stage_artifact_validation_evidence",
        ["variant_id", "kind", "registration_sequence"],
    )


def _create_stage_artifact_validation_ranks() -> None:
    op.create_table(
        "stage_artifact_validation_ranks",
        sa.Column("evidence_id", sa.String(length=71), primary_key=True),
        sa.Column("rank", sa.Integer(), primary_key=True),
        sa.Column("variant_id", sa.String(length=71), nullable=False),
        sa.Column("manifest_digest", sa.String(length=71), nullable=False),
        sa.Column("tensor_keys_digest", sa.String(length=71), nullable=False),
        sa.Column("loaded_tensor_count", sa.Integer(), nullable=False),
        sa.Column("loaded_weight_size_bytes", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "rank >= 0",
            name="ck_stage_evidence_rank_nonnegative",
        ),
        sa.CheckConstraint(
            "length(manifest_digest) = 71 AND manifest_digest LIKE 'sha256:%'",
            name="ck_stage_evidence_rank_manifest_sha256",
        ),
        sa.CheckConstraint(
            "length(tensor_keys_digest) = 71 "
            "AND tensor_keys_digest LIKE 'sha256:%'",
            name="ck_stage_evidence_rank_keys_sha256",
        ),
        sa.CheckConstraint(
            "loaded_tensor_count > 0",
            name="ck_stage_evidence_rank_tensor_count",
        ),
        sa.CheckConstraint(
            "loaded_weight_size_bytes > 0",
            name="ck_stage_evidence_rank_weight_size",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_id", "variant_id"],
            [
                "stage_artifact_validation_evidence.identity_digest",
                "stage_artifact_validation_evidence.variant_id",
            ],
            ondelete="CASCADE",
            name="fk_stage_evidence_rank_evidence",
        ),
        sa.ForeignKeyConstraint(
            ["variant_id", "rank", "manifest_digest", "tensor_keys_digest"],
            [
                "stage_artifact_ranks.variant_id",
                "stage_artifact_ranks.rank",
                "stage_artifact_ranks.manifest_digest",
                "stage_artifact_ranks.tensor_keys_digest",
            ],
            name="fk_stage_evidence_rank_stage",
        ),
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
            "stage_artifact_validation_ranks",
            "stage_artifact_validation_evidence",
            "stage_artifact_ranks",
            "stage_artifact_variants",
        )
        if table in tables and _scalar_count(table) > 0
    ]
    if populated:
        raise RuntimeError(
            "refusing to downgrade 0009 while stage artifact registry data exists: "
            + ", ".join(populated)
        )


def downgrade() -> None:
    _refuse_destructive_downgrade()
    tables = _tables()
    for table in (
        "stage_artifact_validation_ranks",
        "stage_artifact_validation_evidence",
        "stage_artifact_ranks",
        "stage_artifact_variants",
    ):
        if table in tables:
            op.drop_table(table)
