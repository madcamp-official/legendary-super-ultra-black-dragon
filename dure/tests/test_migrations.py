from __future__ import annotations

import tempfile
import unittest
import uuid
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKeyConstraint,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    create_mock_engine,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError, OperationalError

from dure.control.db import Base, make_engine, make_session_factory
from dure.control.benchmark import register_benchmark_evidence
from dure.control.models import (
    ArtifactCacheEvent,
    ArtifactChunk,
    ArtifactFileChunk,
    ArtifactManifest,
    ArtifactManifestFile,
    ArtifactPreparation,
    ArtifactPreparationAttempt,
    ArtifactPreparationNode,
    BenchmarkEvidence,
    BenchmarkRun,
    Deployment,
    DeploymentOperation,
    DeploymentOperationNode,
    DeploymentRecommendationRecord,
    ModelArtifact,
    ModelRelease,
    NodeArtifactCache,
    PlacementProfileRecord,
    RuntimeRelease,
    StageArtifactRank,
    StageArtifactValidationEvidence,
    StageArtifactValidationRank,
    StageArtifactVariant,
    Task,
    utcnow,
)
from dure.control.service import (
    create_model_artifact,
    create_model_release,
    create_runtime_release,
    prepare_benchmark_run,
)

from .test_benchmark import _evidence_body, _node, _release


REGISTRY_TABLES = {
    "model_artifacts",
    "runtime_releases",
    "model_releases",
    "placement_profiles",
}
ARTIFACT_MANIFEST_TABLES = {
    "artifact_manifests",
    "artifact_manifest_files",
    "artifact_chunks",
    "artifact_file_chunks",
}
ARTIFACT_PREPARATION_TABLES = {
    "artifact_preparations",
    "artifact_preparation_nodes",
    "artifact_preparation_attempts",
}
STAGE_ARTIFACT_TABLES = {
    "stage_artifact_variants",
    "stage_artifact_ranks",
    "stage_artifact_validation_evidence",
    "stage_artifact_validation_ranks",
}
ARTIFACT_CACHE_TABLES = {
    "node_artifact_caches",
    "artifact_cache_events",
}
HEAD_TABLES = REGISTRY_TABLES | {
    *ARTIFACT_MANIFEST_TABLES,
    *ARTIFACT_PREPARATION_TABLES,
    *STAGE_ARTIFACT_TABLES,
    *ARTIFACT_CACHE_TABLES,
    "benchmark_evidence",
    "benchmark_runs",
    "deployment_operation_nodes",
    "deployment_operations",
    "deployment_recommendations",
}
STAGE_VARIANT_CHECKS = {
    "ck_stage_variant_architecture",
    "ck_stage_variant_contract_sha256",
    "ck_stage_variant_exporter_sha256",
    "ck_stage_variant_identity_json_nonempty",
    "ck_stage_variant_loader_format",
    "ck_stage_variant_pp_range",
    "ck_stage_variant_quantization",
    "ck_stage_variant_rank_count",
    "ck_stage_variant_runtime_digest",
    "ck_stage_variant_set_sha256",
    "ck_stage_variant_source_sha256",
    "ck_stage_variant_status",
    "ck_stage_variant_status_timestamps",
    "ck_stage_variant_tp_supported",
    "ck_stage_variant_vllm_version",
}
STAGE_RANK_CHECKS = {
    "ck_stage_rank_id_length",
    "ck_stage_rank_linear_coordinate",
    "ck_stage_rank_manifest_sha256",
    "ck_stage_rank_pipeline_range",
    "ck_stage_rank_supported_topology",
    "ck_stage_rank_tensor_count_positive",
    "ck_stage_rank_tensor_keys_sha256",
    "ck_stage_rank_tensor_range",
    "ck_stage_rank_weight_size_positive",
}
STAGE_EVIDENCE_CHECKS = {
    "ck_stage_evidence_failure_code",
    "ck_stage_evidence_identity_sha256",
    "ck_stage_evidence_json_nonempty",
    "ck_stage_evidence_kind",
    "ck_stage_evidence_result_shape",
    "ck_stage_evidence_run_id_length",
    "ck_stage_evidence_schema_version",
    "ck_stage_evidence_sequence_positive",
    "ck_stage_evidence_status",
    "ck_stage_evidence_validator_nonempty",
    "ck_stage_evidence_validator_sha256",
}
STAGE_EVIDENCE_RANK_CHECKS = {
    "ck_stage_evidence_rank_keys_sha256",
    "ck_stage_evidence_rank_manifest_sha256",
    "ck_stage_evidence_rank_nonnegative",
    "ck_stage_evidence_rank_tensor_count",
    "ck_stage_evidence_rank_weight_size",
}
NODE_ARTIFACT_CACHE_CHECKS = {
    "ck_node_artifact_cache_event_sequence_nonnegative",
    "ck_node_artifact_cache_id_length",
    "ck_node_artifact_cache_identity_sha256",
    "ck_node_artifact_cache_identity_shape",
    "ck_node_artifact_cache_kind",
    "ck_node_artifact_cache_manifest_sha256",
    "ck_node_artifact_cache_quarantine_request_length",
    "ck_node_artifact_cache_quarantine_shape",
    "ck_node_artifact_cache_ready_evidence",
    "ck_node_artifact_cache_source_sha256",
    "ck_node_artifact_cache_status",
    "ck_node_artifact_cache_status_reason",
    "ck_node_artifact_cache_tensor_keys_sha256",
    "ck_node_artifact_cache_variant_sha256",
    "ck_node_artifact_cache_verification_shape",
    "ck_node_artifact_cache_verification_version",
    "ck_node_artifact_cache_verified_files_positive",
    "ck_node_artifact_cache_verified_size_positive",
}
ARTIFACT_CACHE_EVENT_CHECKS = {
    "ck_artifact_cache_event_closed_source",
    "ck_artifact_cache_event_evidence_kind",
    "ck_artifact_cache_event_evidence_sha256",
    "ck_artifact_cache_event_id_length",
    "ck_artifact_cache_event_previous_status",
    "ck_artifact_cache_event_previous_status_value",
    "ck_artifact_cache_event_reason",
    "ck_artifact_cache_event_sequence_positive",
    "ck_artifact_cache_event_source_id",
    "ck_artifact_cache_event_source_kind",
    "ck_artifact_cache_event_status",
}
BENCHMARK_INDEXES = {
    "ix_benchmark_evidence_release_id",
    "ix_benchmark_evidence_placement_id",
    "ix_benchmark_evidence_status",
    "ux_benchmark_evidence_benchmark_run_id",
}
BENCHMARK_RUN_INDEXES = {
    "ix_benchmark_runs_request_digest",
    "ix_benchmark_runs_release_id",
    "ix_benchmark_runs_status",
    "ix_benchmark_runs_coordinator_node_id",
}
BENCHMARK_RUN_CHECKS = {
    "ck_benchmark_run_status",
    "ck_benchmark_run_workload",
    "ck_benchmark_run_input_positive",
    "ck_benchmark_run_output_positive",
    "ck_benchmark_run_concurrency_positive",
    "ck_benchmark_run_warmup_nonnegative",
    "ck_benchmark_run_requests_positive",
    "ck_benchmark_run_duration_positive",
    "ck_benchmark_run_inventory_fingerprint",
    "ck_benchmark_run_request_digest",
    "ck_benchmark_run_failure_code",
}
RECOMMENDATION_INDEXES = {"ix_deployment_recommendations_created_at"}
RECOMMENDATION_CHECKS = {
    "ck_deployment_recommendation_id_sha256",
    "ck_deployment_recommendation_selection_mode",
}
DEPLOYMENT_GENERATION_UNIQUES = {
    ("lineage_id", "generation"),
    ("previous_generation_id",),
    ("source_recommendation_id",),
}
DEPLOYMENT_OPERATION_CHECKS = {
    "ck_deployment_operation_id_length",
    "ck_deployment_operation_kind",
    "ck_deployment_operation_phase",
    "ck_deployment_operation_request_digest_sha256",
    "ck_deployment_operation_rollback_target",
    "ck_deployment_operation_status",
}
DEPLOYMENT_OPERATION_UNIQUES = {
    ("active_lineage_id",),
    ("request_digest",),
}
DEPLOYMENT_OPERATION_NODE_CHECKS = {
    "ck_deployment_operation_node_attempt_nonnegative",
    "ck_deployment_operation_node_failure_code",
    "ck_deployment_operation_node_id_length",
    "ck_deployment_operation_node_phase",
    "ck_deployment_operation_node_status",
}
TASK_OPERATION_CHECKS = {
    "ck_tasks_operation_attempt_positive",
    "ck_tasks_operation_binding",
}
ARTIFACT_MANIFEST_CHECKS = {
    "ck_artifact_manifest_canonical_json_nonempty",
    "ck_artifact_manifest_chunk_count_positive",
    "ck_artifact_manifest_digest_sha256",
    "ck_artifact_manifest_file_count_positive",
    "ck_artifact_manifest_schema_version",
    "ck_artifact_manifest_total_size_positive",
}
ARTIFACT_MANIFEST_FILE_CHECKS = {
    "ck_artifact_manifest_file_digest_sha256",
    "ck_artifact_manifest_file_id_length",
    "ck_artifact_manifest_file_kind",
    "ck_artifact_manifest_file_ordinal_nonnegative",
    "ck_artifact_manifest_file_path_length",
    "ck_artifact_manifest_file_path_relative",
    "ck_artifact_manifest_file_size_nonnegative",
}
ARTIFACT_CHUNK_CHECKS = {
    "ck_artifact_chunk_digest_sha256",
    "ck_artifact_chunk_size_positive",
}
ARTIFACT_FILE_CHUNK_CHECKS = {
    "ck_artifact_file_chunk_length_positive",
    "ck_artifact_file_chunk_offset_nonnegative",
    "ck_artifact_file_chunk_ordinal_nonnegative",
}
ARTIFACT_MANIFEST_FILE_UNIQUES = {
    ("manifest_digest", "ordinal"),
    ("manifest_digest", "path"),
}
ARTIFACT_FILE_CHUNK_UNIQUES = {("file_id", "offset_bytes")}
ARTIFACT_PREPARATION_CHECKS = {
    "ck_artifact_preparation_id_length",
    "ck_artifact_preparation_request_digest_sha256",
    "ck_artifact_preparation_request_id_length",
    "ck_artifact_preparation_status",
}
ARTIFACT_PREPARATION_NODE_CHECKS = {
    "ck_artifact_preparation_node_id_length",
    "ck_artifact_preparation_node_image_attempt_nonnegative",
    "ck_artifact_preparation_node_image_attempt_status",
    "ck_artifact_preparation_node_image_failure_code",
    "ck_artifact_preparation_node_image_status",
    "ck_artifact_preparation_node_manifest_digest_sha256",
    "ck_artifact_preparation_node_model_attempt_nonnegative",
    "ck_artifact_preparation_node_model_attempt_status",
    "ck_artifact_preparation_node_model_failure_code",
    "ck_artifact_preparation_node_model_status",
    "ck_artifact_preparation_node_runtime_image_digest",
}
ARTIFACT_PREPARATION_ATTEMPT_CHECKS = {
    "ck_artifact_preparation_attempt_completion",
    "ck_artifact_preparation_attempt_failure_code",
    "ck_artifact_preparation_attempt_id_length",
    "ck_artifact_preparation_attempt_number_positive",
    "ck_artifact_preparation_attempt_stage",
    "ck_artifact_preparation_attempt_status",
}


def config(url: str) -> Config:
    value = Config()
    value.set_main_option(
        "script_location",
        str(Path(__file__).parents[1] / "src" / "dure" / "control" / "migrations"),
    )
    value.set_main_option("sqlalchemy.url", url)
    return value


def true_0003_database(url: str) -> Config:
    """Build revision 0003 despite 0001's legacy current-metadata bootstrap."""
    migration_config = config(url)
    command.upgrade(migration_config, "0002")
    engine = make_engine(url)
    for table in (
        ArtifactPreparationAttempt.__table__,
        ArtifactPreparationNode.__table__,
        ArtifactPreparation.__table__,
    ):
        table.drop(engine, checkfirst=True)
    BenchmarkRun.__table__.drop(engine, checkfirst=True)
    BenchmarkEvidence.__table__.drop(engine, checkfirst=True)
    release_columns = {
        column["name"] for column in inspect(engine).get_columns("model_releases")
    }
    with engine.begin() as connection:
        if "promotion_evidence_digest" in release_columns:
            connection.execute(
                text(
                    "ALTER TABLE model_releases "
                    "DROP COLUMN promotion_evidence_digest"
                )
            )
        if "promotion_evidence_ids" in release_columns:
            connection.execute(
                text(
                    "ALTER TABLE model_releases "
                    "DROP COLUMN promotion_evidence_ids"
                )
            )
    engine.dispose()
    command.upgrade(migration_config, "0003")
    return migration_config


def true_0004_database(url: str) -> Config:
    """Use the 0005 downgrade to materialize the released 0004 schema exactly."""
    migration_config = config(url)
    command.upgrade(migration_config, "head")
    command.downgrade(migration_config, "0004")
    return migration_config


def true_0005_database(url: str) -> Config:
    """Materialize revision 0005 despite 0001's current-metadata bootstrap."""
    migration_config = config(url)
    command.upgrade(migration_config, "head")
    command.downgrade(migration_config, "0005")
    return migration_config


def true_0006_database(url: str) -> Config:
    """Materialize released 0006 without executing revision 0007."""
    migration_config = config(url)
    command.upgrade(migration_config, "0006")
    engine = make_engine(url)
    with engine.begin() as connection:
        for table in (
            ArtifactPreparationAttempt.__table__,
            ArtifactPreparationNode.__table__,
            ArtifactPreparation.__table__,
            ArtifactFileChunk.__table__,
            ArtifactManifestFile.__table__,
            ArtifactChunk.__table__,
            ArtifactManifest.__table__,
        ):
            table.drop(connection, checkfirst=True)
        operations = Operations(MigrationContext.configure(connection))
        with operations.batch_alter_table("model_artifacts") as batch:
            batch.drop_constraint(
                "uq_model_artifacts_id_manifest_digest",
                type_="unique",
            )
    engine.dispose()
    return migration_config


def true_0007_database(url: str) -> Config:
    """Materialize released 0007 without executing revision 0008."""
    migration_config = config(url)
    command.upgrade(migration_config, "0007")
    engine = make_engine(url)
    with engine.begin() as connection:
        for table in (
            ArtifactPreparationAttempt.__table__,
            ArtifactPreparationNode.__table__,
            ArtifactPreparation.__table__,
        ):
            table.drop(connection, checkfirst=True)
    engine.dispose()
    return migration_config


def true_0008_database(url: str) -> Config:
    """Materialize released 0008 without executing revision 0009."""
    migration_config = config(url)
    command.upgrade(migration_config, "0008")
    engine = make_engine(url)
    with engine.begin() as connection:
        for table in (
            StageArtifactValidationRank.__table__,
            StageArtifactValidationEvidence.__table__,
            StageArtifactRank.__table__,
            StageArtifactVariant.__table__,
        ):
            table.drop(connection, checkfirst=True)
    engine.dispose()
    return migration_config


def true_0009_database(url: str) -> Config:
    """Materialize released 0009 without executing revision 0010."""
    migration_config = config(url)
    command.upgrade(migration_config, "0009")
    engine = make_engine(url)
    with engine.begin() as connection:
        ArtifactCacheEvent.__table__.drop(connection, checkfirst=True)
        NodeArtifactCache.__table__.drop(connection, checkfirst=True)
        unique_names = {
            item["name"]
            for item in inspect(connection).get_unique_constraints(
                "stage_artifact_variants"
            )
        }
        if "uq_stage_variant_set_source" in unique_names:
            operations = Operations(MigrationContext.configure(connection))
            with operations.batch_alter_table(
                "stage_artifact_variants"
            ) as batch:
                batch.drop_constraint(
                    "uq_stage_variant_set_source", type_="unique"
                )
    engine.dispose()
    return migration_config


def _seed_stage_variant(session, *, suffix: str = "seed") -> tuple:
    artifact = ModelArtifact(
        id=str(uuid.uuid4()),
        model_id=f"stage-migration-{suffix}",
        repository=f"Example/StageMigration-{suffix}",
        revision="1" * 40,
        manifest_digest="sha256:" + "1" * 64,
        quantization="awq",
        size_mib=1,
        default_max_model_len=1024,
        layer_count=1,
        license_id="apache-2.0",
    )
    runtime = RuntimeRelease(
        id=str(uuid.uuid4()),
        version=f"stage-migration-{suffix}",
        image="registry.example/vllm@sha256:" + "2" * 64,
        vllm_version="0.9.0",
        cuda_version="12.4",
        gpu_architectures=["ampere"],
    )
    session.add_all((artifact, runtime))
    session.flush()
    source = ArtifactManifest(
        digest=artifact.manifest_digest,
        schema_version=1,
        model_artifact_id=artifact.id,
        total_size_bytes=1,
        file_count=1,
        chunk_count=1,
        canonical_json="{}",
    )
    stage = ArtifactManifest(
        digest="sha256:" + "3" * 64,
        schema_version=1,
        model_artifact_id=None,
        total_size_bytes=1,
        file_count=1,
        chunk_count=1,
        canonical_json="{}",
    )
    session.add_all((source, stage))
    session.flush()
    variant = StageArtifactVariant(
        artifact_set_digest="sha256:" + "4" * 64,
        contract_identity_digest="sha256:" + "5" * 64,
        source_manifest_digest=source.digest,
        runtime_release_id=runtime.id,
        runtime_image=runtime.image,
        vllm_version="0.9.0",
        exporter_build_digest="sha256:" + "6" * 64,
        architecture="Qwen2ForCausalLM",
        quantization="awq",
        tensor_parallel_size=1,
        pipeline_parallel_size=1,
        rank_count=1,
        loader_format="VLLM_SHARDED_STATE_V1",
        status="DRAFT",
        canonical_identity_json="{}",
    )
    session.add(variant)
    session.commit()
    return artifact, runtime, source, stage, variant


class MigrationTests(unittest.TestCase):
    def assert_sqlite_artifact_cache_event_guards(self, engine) -> None:
        if engine.dialect.name != "sqlite":
            return
        with engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT name, sql FROM sqlite_master "
                    "WHERE type = 'trigger' "
                    "AND tbl_name = 'artifact_cache_events'"
                )
            ).all()
        triggers = {name: sql for name, sql in rows}
        self.assertEqual(
            {
                "trg_artifact_cache_events_no_update",
                "trg_artifact_cache_events_no_delete",
                "trg_artifact_cache_events_no_replace",
            },
            set(triggers),
        )
        for operation in ("UPDATE", "DELETE"):
            sql = triggers[
                f"trg_artifact_cache_events_no_{operation.lower()}"
            ].upper()
            self.assertIn(f"BEFORE {operation}", sql)
            self.assertIn("RAISE(ABORT", sql)
            self.assertIn("ARTIFACT_CACHE_EVENTS IS APPEND-ONLY", sql)
        replace_sql = triggers[
            "trg_artifact_cache_events_no_replace"
        ].upper()
        self.assertIn("BEFORE INSERT", replace_sql)
        self.assertIn("EXISTING.ID = NEW.ID", replace_sql)
        self.assertIn("EXISTING.CACHE_ID = NEW.CACHE_ID", replace_sql)
        self.assertIn("RAISE(ABORT", replace_sql)
        with engine.connect() as connection:
            table_sql = connection.scalar(
                text(
                    "SELECT sql FROM sqlite_master "
                    "WHERE type = 'table' "
                    "AND name = 'artifact_cache_events'"
                )
            )
        self.assertIn("WITHOUT ROWID", table_sql.upper())

    def assert_artifact_manifest_head(self, inspector) -> None:
        expected_columns = {
            "artifact_manifests": {
                "digest",
                "schema_version",
                "model_artifact_id",
                "total_size_bytes",
                "file_count",
                "chunk_count",
                "canonical_json",
                "created_at",
            },
            "artifact_manifest_files": {
                "id",
                "manifest_digest",
                "ordinal",
                "path",
                "kind",
                "size_bytes",
                "file_digest",
                "created_at",
            },
            "artifact_chunks": {"digest", "size_bytes", "created_at"},
            "artifact_file_chunks": {
                "file_id",
                "ordinal",
                "chunk_digest",
                "offset_bytes",
                "length_bytes",
            },
        }
        for table_name, columns in expected_columns.items():
            migrated = {
                item["name"]: item
                for item in inspector.get_columns(table_name)
            }
            self.assertEqual(columns, set(migrated), table_name)
            self.assertEqual(
                columns,
                set(Base.metadata.tables[table_name].columns.keys()),
                f"Base.metadata {table_name}",
            )
        manifest_columns = {
            item["name"]: item
            for item in inspector.get_columns("artifact_manifests")
        }
        self.assertTrue(manifest_columns["model_artifact_id"]["nullable"])
        for name in expected_columns["artifact_manifests"] - {
            "model_artifact_id"
        }:
            self.assertFalse(manifest_columns[name]["nullable"], name)
        self.assertIsInstance(
            manifest_columns["total_size_bytes"]["type"], BigInteger
        )
        self.assertIsInstance(manifest_columns["canonical_json"]["type"], Text)
        self.assertEqual(
            inspector.get_pk_constraint("artifact_manifests")[
                "constrained_columns"
            ],
            ["digest"],
        )

        file_columns = {
            item["name"]: item
            for item in inspector.get_columns("artifact_manifest_files")
        }
        self.assertTrue(all(not item["nullable"] for item in file_columns.values()))
        self.assertEqual(file_columns["path"]["type"].length, 1024)
        self.assertIsInstance(file_columns["size_bytes"]["type"], BigInteger)
        self.assertEqual(
            inspector.get_pk_constraint("artifact_manifest_files")[
                "constrained_columns"
            ],
            ["id"],
        )

        chunk_columns = {
            item["name"]: item
            for item in inspector.get_columns("artifact_chunks")
        }
        self.assertTrue(all(not item["nullable"] for item in chunk_columns.values()))
        self.assertIsInstance(chunk_columns["size_bytes"]["type"], BigInteger)
        self.assertEqual(
            inspector.get_pk_constraint("artifact_chunks")[
                "constrained_columns"
            ],
            ["digest"],
        )

        file_chunk_columns = {
            item["name"]: item
            for item in inspector.get_columns("artifact_file_chunks")
        }
        self.assertTrue(
            all(not item["nullable"] for item in file_chunk_columns.values())
        )
        self.assertIsInstance(
            file_chunk_columns["offset_bytes"]["type"], BigInteger
        )
        self.assertIsInstance(
            file_chunk_columns["length_bytes"]["type"], BigInteger
        )
        self.assertEqual(
            inspector.get_pk_constraint("artifact_file_chunks")[
                "constrained_columns"
            ],
            ["file_id", "ordinal"],
        )

        expected_checks = {
            "artifact_manifests": ARTIFACT_MANIFEST_CHECKS,
            "artifact_manifest_files": ARTIFACT_MANIFEST_FILE_CHECKS,
            "artifact_chunks": ARTIFACT_CHUNK_CHECKS,
            "artifact_file_chunks": ARTIFACT_FILE_CHUNK_CHECKS,
        }
        for table_name, checks in expected_checks.items():
            self.assertEqual(
                checks,
                {
                    item["name"]
                    for item in inspector.get_check_constraints(table_name)
                },
                table_name,
            )
            self.assertEqual(
                checks,
                {
                    constraint.name
                    for constraint in Base.metadata.tables[
                        table_name
                    ].constraints
                    if isinstance(constraint, CheckConstraint)
                },
                f"Base.metadata {table_name}",
            )

        expected_unique_columns = {
            "artifact_manifest_files": ARTIFACT_MANIFEST_FILE_UNIQUES,
            "artifact_file_chunks": ARTIFACT_FILE_CHUNK_UNIQUES,
        }
        expected_unique_names = {
            "artifact_manifest_files": {
                ("manifest_digest", "ordinal"): (
                    "uq_artifact_manifest_files_manifest_ordinal"
                ),
                ("manifest_digest", "path"): (
                    "uq_artifact_manifest_files_manifest_path"
                ),
            },
            "artifact_file_chunks": {
                ("file_id", "offset_bytes"): (
                    "uq_artifact_file_chunks_file_offset"
                )
            },
        }
        for table_name, uniques in expected_unique_columns.items():
            migrated_uniques = {
                tuple(item["column_names"]): item["name"]
                for item in inspector.get_unique_constraints(table_name)
            }
            self.assertEqual(
                uniques,
                set(migrated_uniques),
                table_name,
            )
            self.assertEqual(
                expected_unique_names[table_name],
                migrated_uniques,
                table_name,
            )
            metadata_uniques = {
                tuple(column.name for column in constraint.columns): (
                    constraint.name
                )
                for constraint in Base.metadata.tables[
                    table_name
                ].constraints
                if isinstance(constraint, UniqueConstraint)
            }
            self.assertEqual(
                uniques,
                set(metadata_uniques),
                f"Base.metadata {table_name}",
            )
            self.assertEqual(
                expected_unique_names[table_name],
                metadata_uniques,
                f"Base.metadata {table_name}",
            )

        expected_indexes = {
            "artifact_manifests": {
                "ix_artifact_manifests_model_artifact_id"
            },
            "artifact_manifest_files": {
                "ix_artifact_manifest_files_manifest_digest"
            },
            "artifact_file_chunks": {
                "ix_artifact_file_chunks_chunk_digest"
            },
        }
        for table_name, indexes in expected_indexes.items():
            self.assertEqual(
                indexes,
                {
                    item["name"]
                    for item in inspector.get_indexes(table_name)
                },
                table_name,
            )
            self.assertEqual(
                indexes,
                {
                    index.name
                    for index in Base.metadata.tables[table_name].indexes
                },
                f"Base.metadata {table_name}",
            )

        expected_foreign_keys = {
            "artifact_manifests": {
                "fk_artifact_manifests_model_artifact_identity": (
                    ("model_artifact_id", "digest"),
                    "model_artifacts",
                    ("id", "manifest_digest"),
                )
            },
            "artifact_manifest_files": {
                "fk_artifact_manifest_files_manifest_digest": (
                    ("manifest_digest",),
                    "artifact_manifests",
                    ("digest",),
                )
            },
            "artifact_file_chunks": {
                "fk_artifact_file_chunks_file_id": (
                    ("file_id",),
                    "artifact_manifest_files",
                    ("id",),
                ),
                "fk_artifact_file_chunks_chunk_digest": (
                    ("chunk_digest",),
                    "artifact_chunks",
                    ("digest",),
                ),
            },
        }
        for table_name, expected in expected_foreign_keys.items():
            self.assertEqual(
                expected,
                {
                    item["name"]: (
                        tuple(item["constrained_columns"]),
                        item["referred_table"],
                        tuple(item["referred_columns"]),
                    )
                    for item in inspector.get_foreign_keys(table_name)
                },
                table_name,
            )
            self.assertEqual(
                set(expected),
                {
                    constraint.name
                    for constraint in Base.metadata.tables[
                        table_name
                    ].constraints
                    if isinstance(constraint, ForeignKeyConstraint)
                },
                f"Base.metadata {table_name}",
            )

        model_artifact_uniques = {
            tuple(item["column_names"]): item["name"]
            for item in inspector.get_unique_constraints("model_artifacts")
        }
        self.assertEqual(
            model_artifact_uniques[("id", "manifest_digest")],
            "uq_model_artifacts_id_manifest_digest",
        )
        self.assertIn(
            "uq_model_artifacts_id_manifest_digest",
            {
                constraint.name
                for constraint in ModelArtifact.__table__.constraints
                if isinstance(constraint, UniqueConstraint)
            },
        )

    def assert_artifact_preparation_head(self, inspector) -> None:
        expected_columns = {
            "artifact_preparations": {
                "id",
                "request_id",
                "request_digest",
                "deployment_id",
                "status",
                "plan_snapshot",
                "created_at",
                "updated_at",
                "completed_at",
            },
            "artifact_preparation_nodes": {
                "id",
                "preparation_id",
                "node_id",
                "model_manifest_digest",
                "runtime_image",
                "model_status",
                "image_status",
                "model_current_attempt",
                "image_current_attempt",
                "model_failure_code",
                "image_failure_code",
                "created_at",
                "updated_at",
            },
            "artifact_preparation_attempts": {
                "id",
                "preparation_node_id",
                "stage",
                "attempt_no",
                "task_id",
                "status",
                "failure_code",
                "result",
                "download_progress",
                "created_at",
                "updated_at",
                "completed_at",
            },
        }
        nullable_columns = {
            "artifact_preparations": {"completed_at"},
            "artifact_preparation_nodes": {
                "model_failure_code",
                "image_failure_code",
            },
            "artifact_preparation_attempts": {
                "failure_code",
                "result",
                "download_progress",
                "completed_at",
            },
        }
        for table_name, columns in expected_columns.items():
            migrated = {
                item["name"]: item for item in inspector.get_columns(table_name)
            }
            self.assertEqual(columns, set(migrated), table_name)
            self.assertEqual(
                columns,
                set(Base.metadata.tables[table_name].columns.keys()),
                f"Base.metadata {table_name}",
            )
            for name, column in migrated.items():
                self.assertEqual(
                    name in nullable_columns[table_name],
                    column["nullable"],
                    f"{table_name}.{name}",
                )
            self.assertEqual(
                inspector.get_pk_constraint(table_name)["constrained_columns"],
                ["id"],
                table_name,
            )
        self._assert_artifact_preparation_constraints(inspector)

    def assert_stage_artifact_head(self, inspector) -> None:
        expected_columns = {
            "stage_artifact_variants": {
                "artifact_set_digest",
                "contract_identity_digest",
                "source_manifest_digest",
                "runtime_release_id",
                "runtime_image",
                "vllm_version",
                "exporter_build_digest",
                "architecture",
                "quantization",
                "tensor_parallel_size",
                "pipeline_parallel_size",
                "rank_count",
                "loader_format",
                "status",
                "canonical_identity_json",
                "created_at",
                "updated_at",
                "validated_at",
                "revoked_at",
            },
            "stage_artifact_ranks": {
                "id",
                "variant_id",
                "rank",
                "pipeline_rank",
                "tensor_rank",
                "tensor_parallel_size",
                "pipeline_parallel_size",
                "manifest_digest",
                "tensor_key_count",
                "tensor_keys_digest",
                "weight_size_bytes",
                "created_at",
            },
            "stage_artifact_validation_evidence": {
                "identity_digest",
                "variant_id",
                "validation_run_id",
                "registration_sequence",
                "schema_version",
                "kind",
                "status",
                "validator_version",
                "validator_build_digest",
                "rank_count",
                "failure_code",
                "canonical_evidence_json",
                "created_at",
            },
            "stage_artifact_validation_ranks": {
                "evidence_id",
                "rank",
                "variant_id",
                "manifest_digest",
                "tensor_keys_digest",
                "loaded_tensor_count",
                "loaded_weight_size_bytes",
            },
        }
        for table_name, columns in expected_columns.items():
            self.assertEqual(
                columns,
                {item["name"] for item in inspector.get_columns(table_name)},
                table_name,
            )
            self.assertEqual(
                columns,
                set(Base.metadata.tables[table_name].columns.keys()),
                f"Base.metadata {table_name}",
            )

        expected_checks = {
            "stage_artifact_variants": STAGE_VARIANT_CHECKS,
            "stage_artifact_ranks": STAGE_RANK_CHECKS,
            "stage_artifact_validation_evidence": STAGE_EVIDENCE_CHECKS,
            "stage_artifact_validation_ranks": STAGE_EVIDENCE_RANK_CHECKS,
        }
        for table_name, checks in expected_checks.items():
            self.assertEqual(
                checks,
                {
                    item["name"]
                    for item in inspector.get_check_constraints(table_name)
                },
                table_name,
            )
            self.assertEqual(
                checks,
                {
                    constraint.name
                    for constraint in Base.metadata.tables[table_name].constraints
                    if isinstance(constraint, CheckConstraint)
                },
                f"Base.metadata {table_name}",
            )

        expected_uniques = {
            "stage_artifact_variants": {
                ("artifact_set_digest", "tensor_parallel_size", "pipeline_parallel_size"),
                ("artifact_set_digest", "source_manifest_digest"),
                ("contract_identity_digest",),
            },
            "stage_artifact_ranks": {
                ("variant_id", "rank"),
                ("variant_id", "pipeline_rank", "tensor_rank"),
                ("variant_id", "manifest_digest"),
                (
                    "variant_id",
                    "rank",
                    "manifest_digest",
                    "tensor_keys_digest",
                ),
            },
            "stage_artifact_validation_evidence": {
                ("variant_id", "registration_sequence"),
                ("variant_id", "validation_run_id"),
                ("identity_digest", "variant_id"),
            },
        }
        for table_name, uniques in expected_uniques.items():
            self.assertEqual(
                uniques,
                {
                    tuple(item["column_names"])
                    for item in inspector.get_unique_constraints(table_name)
                },
                table_name,
            )
            self.assertEqual(
                uniques,
                {
                    tuple(column.name for column in constraint.columns)
                    for constraint in Base.metadata.tables[table_name].constraints
                    if isinstance(constraint, UniqueConstraint)
                },
                f"Base.metadata {table_name}",
            )

        expected_indexes = {
            "stage_artifact_variants": {
                "ix_stage_variants_source_manifest",
                "ix_stage_variants_runtime_release",
                "ix_stage_variants_status",
            },
            "stage_artifact_ranks": {"ix_stage_ranks_manifest_digest"},
            "stage_artifact_validation_evidence": {
                "ix_stage_evidence_variant_kind_sequence"
            },
            "stage_artifact_validation_ranks": set(),
        }
        for table_name, indexes in expected_indexes.items():
            self.assertEqual(
                indexes,
                {item["name"] for item in inspector.get_indexes(table_name)},
                table_name,
            )
            self.assertEqual(
                indexes,
                {index.name for index in Base.metadata.tables[table_name].indexes},
                f"Base.metadata {table_name}",
            )

        expected_foreign_keys = {
            "stage_artifact_variants": {
                "fk_stage_variant_source_manifest": (
                    ("source_manifest_digest",),
                    "artifact_manifests",
                    ("digest",),
                ),
                "fk_stage_variant_runtime_release": (
                    ("runtime_release_id",),
                    "runtime_releases",
                    ("id",),
                ),
            },
            "stage_artifact_ranks": {
                "fk_stage_rank_variant_topology": (
                    (
                        "variant_id",
                        "tensor_parallel_size",
                        "pipeline_parallel_size",
                    ),
                    "stage_artifact_variants",
                    (
                        "artifact_set_digest",
                        "tensor_parallel_size",
                        "pipeline_parallel_size",
                    ),
                ),
                "fk_stage_rank_manifest": (
                    ("manifest_digest",),
                    "artifact_manifests",
                    ("digest",),
                ),
            },
            "stage_artifact_validation_evidence": {
                "fk_stage_evidence_variant": (
                    ("variant_id",),
                    "stage_artifact_variants",
                    ("artifact_set_digest",),
                ),
            },
            "stage_artifact_validation_ranks": {
                "fk_stage_evidence_rank_evidence": (
                    ("evidence_id", "variant_id"),
                    "stage_artifact_validation_evidence",
                    ("identity_digest", "variant_id"),
                ),
                "fk_stage_evidence_rank_stage": (
                    (
                        "variant_id",
                        "rank",
                        "manifest_digest",
                        "tensor_keys_digest",
                    ),
                    "stage_artifact_ranks",
                    (
                        "variant_id",
                        "rank",
                        "manifest_digest",
                        "tensor_keys_digest",
                    ),
                ),
            },
        }
        for table_name, foreign_keys in expected_foreign_keys.items():
            self.assertEqual(
                foreign_keys,
                {
                    item["name"]: (
                        tuple(item["constrained_columns"]),
                        item["referred_table"],
                        tuple(item["referred_columns"]),
                    )
                    for item in inspector.get_foreign_keys(table_name)
                },
                table_name,
            )
            self.assertEqual(
                set(foreign_keys),
                {
                    constraint.name
                    for constraint in Base.metadata.tables[table_name].constraints
                    if isinstance(constraint, ForeignKeyConstraint)
                },
                f"Base.metadata {table_name}",
            )
    def _assert_artifact_preparation_constraints(self, inspector) -> None:
        preparation_columns = {
            item["name"]: item
            for item in inspector.get_columns("artifact_preparations")
        }
        self.assertEqual(preparation_columns["request_id"]["type"].length, 36)
        self.assertEqual(
            preparation_columns["request_digest"]["type"].length,
            71,
        )
        node_columns = {
            item["name"]: item
            for item in inspector.get_columns("artifact_preparation_nodes")
        }
        self.assertEqual(node_columns["model_manifest_digest"]["type"].length, 71)
        self.assertEqual(node_columns["runtime_image"]["type"].length, 512)
        attempt_columns = {
            item["name"]: item
            for item in inspector.get_columns("artifact_preparation_attempts")
        }
        self.assertEqual(attempt_columns["stage"]["type"].length, 10)

        expected_checks = {
            "artifact_preparations": ARTIFACT_PREPARATION_CHECKS,
            "artifact_preparation_nodes": ARTIFACT_PREPARATION_NODE_CHECKS,
            "artifact_preparation_attempts": (
                ARTIFACT_PREPARATION_ATTEMPT_CHECKS
            ),
        }
        for table_name, checks in expected_checks.items():
            self.assertEqual(
                checks,
                {
                    item["name"]
                    for item in inspector.get_check_constraints(table_name)
                },
                table_name,
            )
            self.assertEqual(
                checks,
                {
                    constraint.name
                    for constraint in Base.metadata.tables[
                        table_name
                    ].constraints
                    if isinstance(constraint, CheckConstraint)
                },
                f"Base.metadata {table_name}",
            )

        expected_uniques = {
            "artifact_preparations": {
                ("request_id",): "uq_artifact_preparations_request_id",
                ("request_digest",): (
                    "uq_artifact_preparations_request_digest"
                ),
                ("deployment_id",): (
                    "uq_artifact_preparations_deployment_id"
                ),
            },
            "artifact_preparation_nodes": {
                ("preparation_id", "node_id"): (
                    "uq_artifact_preparation_nodes_preparation_node"
                ),
            },
            "artifact_preparation_attempts": {
                ("preparation_node_id", "stage", "attempt_no"): (
                    "uq_artifact_preparation_attempts_node_stage_number"
                ),
                ("task_id",): "uq_artifact_preparation_attempts_task_id",
            },
        }
        for table_name, uniques in expected_uniques.items():
            self.assertEqual(
                uniques,
                {
                    tuple(item["column_names"]): item["name"]
                    for item in inspector.get_unique_constraints(table_name)
                },
                table_name,
            )
            self.assertEqual(
                uniques,
                {
                    tuple(column.name for column in constraint.columns): (
                        constraint.name
                    )
                    for constraint in Base.metadata.tables[
                        table_name
                    ].constraints
                    if isinstance(constraint, UniqueConstraint)
                },
                f"Base.metadata {table_name}",
            )

        expected_indexes = {
            "artifact_preparations": {"ix_artifact_preparations_status"},
            "artifact_preparation_nodes": {
                "ix_artifact_preparation_nodes_node_id"
            },
            "artifact_preparation_attempts": {
                "ix_artifact_preparation_attempts_node_stage_status"
            },
        }
        for table_name, indexes in expected_indexes.items():
            self.assertEqual(
                indexes,
                {item["name"] for item in inspector.get_indexes(table_name)},
                table_name,
            )
            self.assertEqual(
                indexes,
                {
                    index.name
                    for index in Base.metadata.tables[table_name].indexes
                },
                f"Base.metadata {table_name}",
            )

        expected_foreign_keys = {
            "artifact_preparations": {
                "fk_artifact_preparations_deployment_id": (
                    ("deployment_id",),
                    "deployments",
                    ("id",),
                ),
            },
            "artifact_preparation_nodes": {
                "fk_artifact_preparation_nodes_preparation_id": (
                    ("preparation_id",),
                    "artifact_preparations",
                    ("id",),
                ),
                "fk_artifact_preparation_nodes_node_id": (
                    ("node_id",),
                    "nodes",
                    ("id",),
                ),
                "fk_artifact_preparation_nodes_manifest_digest": (
                    ("model_manifest_digest",),
                    "artifact_manifests",
                    ("digest",),
                ),
            },
            "artifact_preparation_attempts": {
                "fk_artifact_preparation_attempts_preparation_node_id": (
                    ("preparation_node_id",),
                    "artifact_preparation_nodes",
                    ("id",),
                ),
                "fk_artifact_preparation_attempts_task_id": (
                    ("task_id",),
                    "tasks",
                    ("id",),
                ),
            },
        }
        for table_name, foreign_keys in expected_foreign_keys.items():
            self.assertEqual(
                foreign_keys,
                {
                    item["name"]: (
                        tuple(item["constrained_columns"]),
                        item["referred_table"],
                        tuple(item["referred_columns"]),
                    )
                    for item in inspector.get_foreign_keys(table_name)
                },
                table_name,
            )
            self.assertEqual(
                set(foreign_keys),
                {
                    constraint.name
                    for constraint in Base.metadata.tables[
                        table_name
                    ].constraints
                    if isinstance(constraint, ForeignKeyConstraint)
                },
                f"Base.metadata {table_name}",
            )

    def assert_artifact_cache_head(self, inspector) -> None:
        expected_columns = {
            "node_artifact_caches": {
                "id",
                "node_id",
                "cache_kind",
                "cache_identity_digest",
                "manifest_digest",
                "source_manifest_digest",
                "artifact_set_digest",
                "artifact_rank",
                "pipeline_rank",
                "tensor_rank",
                "tensor_parallel_size",
                "pipeline_parallel_size",
                "tensor_keys_digest",
                "status",
                "reason_code",
                "last_ready_attempt_id",
                "verified_at",
                "verified_size_bytes",
                "verified_file_count",
                "verification_version",
                "last_probe_observed_at",
                "quarantine_request_id",
                "quarantined_at",
                "event_sequence",
                "created_at",
                "updated_at",
            },
            "artifact_cache_events": {
                "id",
                "cache_id",
                "sequence",
                "previous_status",
                "status",
                "reason_code",
                "source_kind",
                "source_id",
                "source_attempt_id",
                "source_task_id",
                "evidence_kind",
                "evidence_digest",
                "created_at",
            },
        }
        nullable = {
            "node_artifact_caches": {
                "artifact_set_digest",
                "artifact_rank",
                "pipeline_rank",
                "tensor_rank",
                "tensor_parallel_size",
                "pipeline_parallel_size",
                "tensor_keys_digest",
                "last_ready_attempt_id",
                "verified_at",
                "verified_size_bytes",
                "verified_file_count",
                "verification_version",
                "last_probe_observed_at",
                "quarantine_request_id",
                "quarantined_at",
            },
            "artifact_cache_events": {
                "previous_status",
                "source_attempt_id",
                "source_task_id",
            },
        }
        for table_name, columns in expected_columns.items():
            migrated = {
                item["name"]: item
                for item in inspector.get_columns(table_name)
            }
            self.assertEqual(columns, set(migrated), table_name)
            self.assertEqual(
                columns,
                set(Base.metadata.tables[table_name].columns.keys()),
                f"Base.metadata {table_name}",
            )
            for name, column in migrated.items():
                self.assertEqual(
                    name in nullable[table_name],
                    column["nullable"],
                    f"{table_name}.{name}",
                )

        expected_checks = {
            "node_artifact_caches": NODE_ARTIFACT_CACHE_CHECKS,
            "artifact_cache_events": ARTIFACT_CACHE_EVENT_CHECKS,
        }
        for table_name, checks in expected_checks.items():
            self.assertEqual(
                checks,
                {
                    item["name"]
                    for item in inspector.get_check_constraints(table_name)
                },
                table_name,
            )
            self.assertEqual(
                checks,
                {
                    item.name
                    for item in Base.metadata.tables[table_name].constraints
                    if isinstance(item, CheckConstraint)
                },
                f"Base.metadata {table_name}",
            )

        expected_uniques = {
            "node_artifact_caches": {
                ("node_id", "cache_identity_digest"),
                ("last_ready_attempt_id",),
            },
            "artifact_cache_events": {
                ("cache_id", "sequence"),
                ("cache_id", "source_kind", "source_id", "reason_code"),
            },
        }
        for table_name, uniques in expected_uniques.items():
            self.assertEqual(
                uniques,
                {
                    tuple(item["column_names"])
                    for item in inspector.get_unique_constraints(table_name)
                },
                table_name,
            )
            self.assertEqual(
                uniques,
                {
                    tuple(column.name for column in item.columns)
                    for item in Base.metadata.tables[table_name].constraints
                    if isinstance(item, UniqueConstraint)
                },
                f"Base.metadata {table_name}",
            )

        expected_indexes = {
            "node_artifact_caches": {
                "ix_node_artifact_caches_manifest_status",
                "ix_node_artifact_caches_node_status",
                "ix_node_artifact_caches_variant_status",
            },
            "artifact_cache_events": {
                "ix_artifact_cache_events_cache_created",
                "ix_artifact_cache_events_source_task",
            },
        }
        for table_name, indexes in expected_indexes.items():
            self.assertEqual(
                indexes,
                {item["name"] for item in inspector.get_indexes(table_name)},
                table_name,
            )
            self.assertEqual(
                indexes,
                {item.name for item in Base.metadata.tables[table_name].indexes},
                f"Base.metadata {table_name}",
            )

        expected_foreign_keys = {
            "node_artifact_caches": {
                "fk_node_artifact_caches_node_id",
                "fk_node_artifact_caches_manifest_digest",
                "fk_node_artifact_caches_source_manifest_digest",
                "fk_node_artifact_cache_stage_source",
                "fk_node_artifact_cache_stage_topology",
                "fk_node_artifact_cache_stage_rank",
                "fk_node_artifact_caches_ready_attempt_id",
            },
            "artifact_cache_events": {
                "fk_artifact_cache_events_cache_id",
                "fk_artifact_cache_events_source_attempt_id",
                "fk_artifact_cache_events_source_task_id",
            },
        }
        for table_name, names in expected_foreign_keys.items():
            self.assertEqual(
                names,
                {
                    item["name"]
                    for item in inspector.get_foreign_keys(table_name)
                },
                table_name,
            )
            self.assertEqual(
                names,
                {
                    item.name
                    for item in Base.metadata.tables[table_name].constraints
                    if isinstance(item, ForeignKeyConstraint)
                },
                f"Base.metadata {table_name}",
            )

    def assert_benchmark_head(self, url: str) -> None:
        engine = make_engine(url)
        inspector = inspect(engine)
        self.assertTrue(HEAD_TABLES <= set(inspector.get_table_names()))
        self.assert_artifact_manifest_head(inspector)
        self.assert_artifact_preparation_head(inspector)
        self.assert_stage_artifact_head(inspector)
        self.assert_artifact_cache_head(inspector)
        self.assertEqual(
            BENCHMARK_INDEXES,
            {item["name"] for item in inspector.get_indexes("benchmark_evidence")},
        )
        evidence_indexes = {
            item["name"]: item
            for item in inspector.get_indexes("benchmark_evidence")
        }
        self.assertTrue(
            evidence_indexes["ux_benchmark_evidence_benchmark_run_id"][
                "unique"
            ]
        )
        self.assertEqual(
            evidence_indexes["ux_benchmark_evidence_benchmark_run_id"][
                "column_names"
            ],
            ["benchmark_run_id"],
        )
        evidence_columns = {
            item["name"]: item
            for item in inspector.get_columns("benchmark_evidence")
        }
        self.assertIn("benchmark_run_id", evidence_columns)
        self.assertTrue(evidence_columns["benchmark_run_id"]["nullable"])
        self.assertEqual(evidence_columns["benchmark_run_id"]["type"].length, 36)
        evidence_foreign_keys = inspector.get_foreign_keys("benchmark_evidence")
        evidence_foreign_key_columns = {
            tuple(item["constrained_columns"])
            for item in evidence_foreign_keys
        }
        self.assertNotIn(("benchmark_run_id",), evidence_foreign_key_columns)
        self.assertNotIn(
            "benchmark_runs",
            {item["referred_table"] for item in evidence_foreign_keys},
        )
        evidence_checks = {
            item["name"]
            for item in inspector.get_check_constraints("benchmark_evidence")
        }
        self.assertIn("ck_benchmark_evidence_run_id_length", evidence_checks)
        self.assertEqual(
            BENCHMARK_RUN_INDEXES,
            {item["name"] for item in inspector.get_indexes("benchmark_runs")},
        )
        run_columns = {
            item["name"]: item
            for item in inspector.get_columns("benchmark_runs")
        }
        self.assertTrue(
            {
                "request_id",
                "request_digest",
                "task_id",
                "evidence_id",
                "failure_code",
            }
            <= set(run_columns)
        )
        for column in (
            "request_id",
            "request_digest",
            "release_id",
            "placement_id",
            "coordinator_node_id",
            "node_ids",
            "inventory_fingerprint",
            "suite_id",
            "policy_version",
            "workload_id",
            "dure_commit",
            "model_id",
            "repository",
            "artifact_revision",
            "artifact_manifest_digest",
            "quantization",
            "runtime_image",
            "input_tokens",
            "output_tokens",
            "concurrency",
            "warmup_requests",
            "request_count",
            "duration_seconds",
            "status",
            "created_at",
            "updated_at",
        ):
            self.assertFalse(run_columns[column]["nullable"], column)
        for column in ("task_id", "evidence_id", "failure_code"):
            self.assertTrue(run_columns[column]["nullable"], column)
        run_unique = {
            tuple(item["column_names"])
            for item in inspector.get_unique_constraints("benchmark_runs")
        }
        self.assertTrue(
            {("request_id",), ("task_id",), ("evidence_id",)} <= run_unique
        )
        run_foreign_keys = {
            tuple(item["constrained_columns"]): (
                item["referred_table"],
                tuple(item["referred_columns"]),
            )
            for item in inspector.get_foreign_keys("benchmark_runs")
        }
        self.assertEqual(
            run_foreign_keys[("evidence_id",)],
            ("benchmark_evidence", ("id",)),
        )
        self.assertEqual(
            [
                columns
                for columns, target in run_foreign_keys.items()
                if target[0] == "benchmark_evidence"
            ],
            [("evidence_id",)],
        )
        self.assertEqual(
            BENCHMARK_RUN_CHECKS,
            {
                item["name"]
                for item in inspector.get_check_constraints("benchmark_runs")
            },
        )
        release_columns = {
            item["name"] for item in inspector.get_columns("model_releases")
        }
        self.assertTrue(
            {"promotion_evidence_ids", "promotion_evidence_digest"}
            <= release_columns
        )
        recommendation_columns = {
            item["name"]: item
            for item in inspector.get_columns("deployment_recommendations")
        }
        self.assertEqual(
            {
                "id",
                "objective",
                "selection_mode",
                "requested_node_ids",
                "catalog_version",
                "policy_version",
                "inventory_fingerprint",
                "recommendation_snapshot",
                "inventory_snapshot",
                "created_at",
            },
            set(recommendation_columns),
        )
        self.assertTrue(
            all(not column["nullable"] for column in recommendation_columns.values())
        )
        self.assertEqual(
            RECOMMENDATION_INDEXES,
            {
                item["name"]
                for item in inspector.get_indexes("deployment_recommendations")
            },
        )
        self.assertEqual(
            RECOMMENDATION_CHECKS,
            {
                item["name"]
                for item in inspector.get_check_constraints(
                    "deployment_recommendations"
                )
            },
        )
        deployment_columns = {
            item["name"]: item for item in inspector.get_columns("deployments")
        }
        self.assertTrue(
            {
                "lineage_id",
                "previous_generation_id",
                "source_recommendation_id",
                "verified_at",
            }
            <= set(deployment_columns)
        )
        self.assertFalse(deployment_columns["lineage_id"]["nullable"])
        self.assertTrue(deployment_columns["previous_generation_id"]["nullable"])
        self.assertTrue(deployment_columns["source_recommendation_id"]["nullable"])
        self.assertTrue(deployment_columns["verified_at"]["nullable"])
        self.assertEqual(
            DEPLOYMENT_GENERATION_UNIQUES,
            {
                tuple(item["column_names"])
                for item in inspector.get_unique_constraints("deployments")
            },
        )
        deployment_foreign_keys = {
            tuple(item["constrained_columns"]): (
                item["referred_table"],
                tuple(item["referred_columns"]),
            )
            for item in inspector.get_foreign_keys("deployments")
        }
        self.assertEqual(
            deployment_foreign_keys[("previous_generation_id",)],
            ("deployments", ("id",)),
        )
        self.assertEqual(
            deployment_foreign_keys[("source_recommendation_id",)],
            ("deployment_recommendations", ("id",)),
        )

        operation_columns = {
            item["name"]: item
            for item in inspector.get_columns("deployment_operations")
        }
        self.assertEqual(
            {
                "id",
                "request_digest",
                "lineage_id",
                "deployment_id",
                "rollback_target_id",
                "kind",
                "status",
                "phase",
                "node_ids",
                "serve",
                "api",
                "active_lineage_id",
                "created_at",
                "updated_at",
                "completed_at",
            },
            set(operation_columns),
        )
        for name in (
            "id",
            "request_digest",
            "lineage_id",
            "deployment_id",
            "kind",
            "status",
            "phase",
            "node_ids",
            "serve",
            "api",
            "created_at",
            "updated_at",
        ):
            self.assertFalse(operation_columns[name]["nullable"], name)
        for name in ("rollback_target_id", "active_lineage_id", "completed_at"):
            self.assertTrue(operation_columns[name]["nullable"], name)
        operation_checks = {
            item["name"]: item["sqltext"]
            for item in inspector.get_check_constraints(
                "deployment_operations"
            )
        }
        self.assertEqual(DEPLOYMENT_OPERATION_CHECKS, set(operation_checks))
        for phase in ("START_API", "VERIFY_API"):
            self.assertIn(
                phase,
                operation_checks["ck_deployment_operation_phase"],
            )
        self.assertEqual(
            DEPLOYMENT_OPERATION_UNIQUES,
            {
                tuple(item["column_names"])
                for item in inspector.get_unique_constraints(
                    "deployment_operations"
                )
            },
        )
        operation_foreign_keys = {
            tuple(item["constrained_columns"]): (
                item["referred_table"],
                tuple(item["referred_columns"]),
            )
            for item in inspector.get_foreign_keys("deployment_operations")
        }
        self.assertEqual(
            operation_foreign_keys,
            {
                ("deployment_id",): ("deployments", ("id",)),
                ("rollback_target_id",): ("deployments", ("id",)),
            },
        )

        operation_node_columns = {
            item["name"]: item
            for item in inspector.get_columns("deployment_operation_nodes")
        }
        self.assertEqual(
            {
                "id",
                "operation_id",
                "node_id",
                "phase",
                "status",
                "attempt_count",
                "failure_code",
                "created_at",
                "updated_at",
                "completed_at",
            },
            set(operation_node_columns),
        )
        for name in (
            "id",
            "operation_id",
            "node_id",
            "phase",
            "status",
            "attempt_count",
            "created_at",
            "updated_at",
        ):
            self.assertFalse(operation_node_columns[name]["nullable"], name)
        for name in ("failure_code", "completed_at"):
            self.assertTrue(operation_node_columns[name]["nullable"], name)
        operation_node_checks = {
            item["name"]: item["sqltext"]
            for item in inspector.get_check_constraints(
                "deployment_operation_nodes"
            )
        }
        self.assertEqual(
            DEPLOYMENT_OPERATION_NODE_CHECKS,
            set(operation_node_checks),
        )
        for phase in ("START_API", "VERIFY_API"):
            self.assertIn(
                phase,
                operation_node_checks[
                    "ck_deployment_operation_node_phase"
                ],
            )
        self.assertEqual(
            {("operation_id", "node_id", "phase")},
            {
                tuple(item["column_names"])
                for item in inspector.get_unique_constraints(
                    "deployment_operation_nodes"
                )
            },
        )
        operation_node_foreign_keys = {
            tuple(item["constrained_columns"]): (
                item["referred_table"],
                tuple(item["referred_columns"]),
            )
            for item in inspector.get_foreign_keys(
                "deployment_operation_nodes"
            )
        }
        self.assertEqual(
            operation_node_foreign_keys,
            {
                ("operation_id",): ("deployment_operations", ("id",)),
                ("node_id",): ("nodes", ("id",)),
            },
        )

        task_columns = {
            item["name"]: item for item in inspector.get_columns("tasks")
        }
        self.assertTrue(
            {"operation_node_id", "operation_attempt"} <= set(task_columns)
        )
        self.assertTrue(task_columns["operation_node_id"]["nullable"])
        self.assertTrue(task_columns["operation_attempt"]["nullable"])
        task_checks = {
            item["name"] for item in inspector.get_check_constraints("tasks")
        }
        self.assertTrue(TASK_OPERATION_CHECKS <= task_checks)
        task_uniques = {
            tuple(item["column_names"])
            for item in inspector.get_unique_constraints("tasks")
        }
        self.assertIn(("operation_node_id", "operation_attempt"), task_uniques)
        task_foreign_keys = {
            tuple(item["constrained_columns"]): (
                item["referred_table"],
                tuple(item["referred_columns"]),
            )
            for item in inspector.get_foreign_keys("tasks")
        }
        self.assertEqual(
            task_foreign_keys[("operation_node_id",)],
            ("deployment_operation_nodes", ("id",)),
        )
        self.assert_sqlite_artifact_cache_event_guards(engine)
        engine.dispose()

    def test_postgresql_append_only_ddl_and_cleanup_are_deterministic(self):
        statements: list[str] = []
        dialect = postgresql.dialect()

        def record(statement, *_multiparams, **_params) -> None:
            statements.append(str(statement.compile(dialect=dialect)))

        engine = create_mock_engine("postgresql://", record)
        Base.metadata.create_all(
            engine,
            tables=[ArtifactCacheEvent.__table__],
            checkfirst=False,
        )
        metadata_create_sql = "\n".join(statements)
        self.assertIn(
            "CREATE OR REPLACE FUNCTION "
            "dure_artifact_cache_events_append_only_guard()",
            metadata_create_sql,
        )
        self.assertIn("USING ERRCODE = '23514'", metadata_create_sql)
        for operation in ("UPDATE", "DELETE"):
            self.assertIn(
                f"CREATE TRIGGER "
                f"trg_artifact_cache_events_no_{operation.lower()}",
                metadata_create_sql,
            )
            self.assertIn(
                f"BEFORE {operation} ON artifact_cache_events",
                metadata_create_sql,
            )
        self.assertIn(
            "CREATE TRIGGER trg_artifact_cache_events_no_truncate",
            metadata_create_sql,
        )
        self.assertIn(
            "BEFORE TRUNCATE ON artifact_cache_events",
            metadata_create_sql,
        )

        statements.clear()
        Base.metadata.drop_all(
            engine,
            tables=[ArtifactCacheEvent.__table__],
            checkfirst=False,
        )
        self.assertIn(
            "DROP FUNCTION IF EXISTS "
            "dure_artifact_cache_events_append_only_guard()",
            "\n".join(statements),
        )

        revision_module = ScriptDirectory.from_config(
            config("sqlite://")
        ).get_revision("0010").module
        migration_create_sql = "\n".join(
            revision_module._append_only_guard_upgrade_sql("postgresql")
        )
        self.assertIn(
            "CREATE OR REPLACE FUNCTION "
            "dure_artifact_cache_events_append_only_guard()",
            migration_create_sql,
        )
        self.assertIn("USING ERRCODE = '23514'", migration_create_sql)
        for operation in ("UPDATE", "DELETE"):
            trigger_name = (
                f"trg_artifact_cache_events_no_{operation.lower()}"
            )
            self.assertIn(
                f"DROP TRIGGER IF EXISTS {trigger_name} "
                "ON artifact_cache_events",
                migration_create_sql,
            )
            self.assertIn(
                f"CREATE TRIGGER {trigger_name}", migration_create_sql
            )
            self.assertIn(
                f"BEFORE {operation} ON artifact_cache_events",
                migration_create_sql,
            )
        self.assertIn(
            "DROP TRIGGER IF EXISTS "
            "trg_artifact_cache_events_no_truncate "
            "ON artifact_cache_events",
            migration_create_sql,
        )
        self.assertIn(
            "CREATE TRIGGER trg_artifact_cache_events_no_truncate",
            migration_create_sql,
        )
        self.assertIn(
            "BEFORE TRUNCATE ON artifact_cache_events",
            migration_create_sql,
        )
        self.assertEqual(
            (
                "LOCK TABLE artifact_preparation_attempts, "
                "node_artifact_caches, artifact_cache_events "
                "IN ACCESS EXCLUSIVE MODE",
            ),
            revision_module._destructive_downgrade_lock_sql(
                "postgresql",
                (
                    "artifact_preparation_attempts",
                    "node_artifact_caches",
                    "artifact_cache_events",
                ),
            ),
        )
        self.assertEqual(
            (
                "DROP FUNCTION IF EXISTS "
                "dure_artifact_cache_events_append_only_guard()",
            ),
            revision_module._append_only_guard_downgrade_sql("postgresql"),
        )

    def test_migration_history_has_single_0010_head(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'heads.db'}"
            heads = ScriptDirectory.from_config(config(url)).get_heads()

            self.assertEqual(heads, ["0010"])

    def test_empty_database_upgrades_to_benchmark_head(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'empty.db'}"

            command.upgrade(config(url), "head")

            self.assert_benchmark_head(url)

    def test_true_0009_upgrade_and_empty_round_trip_preserve_stage_registry(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'cache-upgrade.db'}"
            migration_config = true_0009_database(url)
            engine = make_engine(url)
            self.assertFalse(
                ARTIFACT_CACHE_TABLES & set(inspect(engine).get_table_names())
            )
            factory = make_session_factory(engine)
            with factory() as session:
                _artifact, _runtime, _source, _stage, variant = _seed_stage_variant(
                    session, suffix="cache-upgrade"
                )
                variant_id = variant.artifact_set_digest
            engine.dispose()

            command.upgrade(migration_config, "head")
            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                self.assertIsNotNone(session.get(StageArtifactVariant, variant_id))
            engine.dispose()

            command.downgrade(migration_config, "0009")
            engine = make_engine(url)
            self.assertFalse(
                ARTIFACT_CACHE_TABLES & set(inspect(engine).get_table_names())
            )
            self.assertNotIn(
                "uq_stage_variant_set_source",
                {
                    item["name"]
                    for item in inspect(engine).get_unique_constraints(
                        "stage_artifact_variants"
                    )
                },
            )
            factory = make_session_factory(engine)
            with factory() as session:
                self.assertIsNotNone(session.get(StageArtifactVariant, variant_id))
            engine.dispose()

            command.upgrade(migration_config, "head")
            self.assert_benchmark_head(url)

    def test_0010_downgrade_rejects_persisted_cache_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'cache-downgrade.db'}"
            migration_config = config(url)
            command.upgrade(migration_config, "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                node = _node(session, "cache-downgrade")
                artifact = ModelArtifact(
                    model_id="cache-downgrade",
                    repository="Example/CacheDowngrade",
                    revision="d" * 40,
                    manifest_digest="sha256:" + "e" * 64,
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1024,
                    layer_count=1,
                    license_id="apache-2.0",
                )
                session.add(artifact)
                session.flush()
                session.add(
                    ArtifactManifest(
                        digest=artifact.manifest_digest,
                        schema_version=1,
                        model_artifact_id=artifact.id,
                        total_size_bytes=1,
                        file_count=1,
                        chunk_count=1,
                        canonical_json="{}",
                    )
                )
                session.flush()
                session.add(
                    NodeArtifactCache(
                        node_id=node.id,
                        cache_kind="FULL_SNAPSHOT",
                        cache_identity_digest=artifact.manifest_digest,
                        manifest_digest=artifact.manifest_digest,
                        source_manifest_digest=artifact.manifest_digest,
                        status="MISSING",
                        reason_code="PROBE_MISSING",
                        event_sequence=0,
                    )
                )
                session.commit()
            engine.dispose()

            with self.assertRaisesRegex(
                RuntimeError,
                "refusing to downgrade 0010",
            ):
                command.downgrade(migration_config, "0009")

            engine = make_engine(url)
            self.assertTrue(
                ARTIFACT_CACHE_TABLES <= set(inspect(engine).get_table_names())
            )
            engine.dispose()

    def test_0010_downgrade_rejects_non_null_download_progress(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'progress-downgrade.db'}"
            migration_config = config(url)
            command.upgrade(migration_config, "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            deployment_id = str(uuid.uuid4())
            manifest_digest = "sha256:" + "7" * 64
            with factory() as session:
                node = _node(session, "progress-downgrade")
                deployment = Deployment(
                    id=deployment_id,
                    lineage_id=deployment_id,
                    generation=1,
                    plan={
                        "deployment_id": deployment_id,
                        "generation": 1,
                    },
                    accept_model_download=False,
                    pull_image=False,
                    status="CREATED",
                )
                manifest = ArtifactManifest(
                    digest=manifest_digest,
                    schema_version=1,
                    model_artifact_id=None,
                    total_size_bytes=1,
                    file_count=1,
                    chunk_count=1,
                    canonical_json="{}",
                )
                session.add_all([deployment, manifest])
                session.flush()
                preparation = ArtifactPreparation(
                    request_id=str(uuid.uuid4()),
                    request_digest="sha256:" + "8" * 64,
                    deployment_id=deployment_id,
                    status="QUEUED",
                    plan_snapshot={
                        "deployment_id": deployment_id,
                        "generation": 1,
                    },
                )
                session.add(preparation)
                session.flush()
                preparation_node = ArtifactPreparationNode(
                    preparation_id=preparation.id,
                    node_id=node.id,
                    model_manifest_digest=manifest_digest,
                    runtime_image=(
                        "registry.example/runtime@sha256:" + "9" * 64
                    ),
                    model_status="QUEUED",
                    image_status="PREPARED",
                    model_current_attempt=1,
                    image_current_attempt=0,
                )
                session.add(preparation_node)
                session.flush()
                task = Task(
                    bulk_id=preparation.id,
                    node_id=node.id,
                    type="PREPARE_MODEL",
                    deployment_id=deployment_id,
                    status="QUEUED",
                    payload={},
                )
                session.add(task)
                session.flush()
                attempt = ArtifactPreparationAttempt(
                    preparation_node_id=preparation_node.id,
                    stage="MODEL",
                    attempt_no=1,
                    task_id=task.id,
                    status="QUEUED",
                    download_progress={
                        "downloaded_bytes": 1,
                        "expected_bytes": 1,
                    },
                )
                session.add(attempt)
                session.commit()
                attempt_id = attempt.id
            engine.dispose()

            with self.assertRaisesRegex(
                RuntimeError,
                "artifact_preparation_attempts.download_progress",
            ):
                command.downgrade(migration_config, "0009")

            engine = make_engine(url)
            self.assertIn(
                "download_progress",
                {
                    item["name"]
                    for item in inspect(engine).get_columns(
                        "artifact_preparation_attempts"
                    )
                },
            )
            factory = make_session_factory(engine)
            with factory() as session:
                attempt = session.get(
                    ArtifactPreparationAttempt, attempt_id
                )
                self.assertIsNotNone(attempt)
                attempt.download_progress = None
                session.commit()
                storage_kind = session.scalar(
                    text(
                        "SELECT typeof(download_progress) "
                        "FROM artifact_preparation_attempts "
                        "WHERE id = :attempt_id"
                    ),
                    {"attempt_id": attempt_id},
                )
                self.assertEqual(storage_kind, "null")
                # Draft databases created before none_as_null=True may contain
                # a JSON literal null.  It is absence, not lifecycle data.
                session.execute(
                    text(
                        "UPDATE artifact_preparation_attempts "
                        "SET download_progress = 'null' "
                        "WHERE id = :attempt_id"
                    ),
                    {"attempt_id": attempt_id},
                )
                session.commit()
                self.assertEqual(
                    session.scalar(
                        text(
                            "SELECT typeof(download_progress) "
                            "FROM artifact_preparation_attempts "
                            "WHERE id = :attempt_id"
                        ),
                        {"attempt_id": attempt_id},
                    ),
                    "text",
                )
            engine.dispose()

            command.downgrade(migration_config, "0009")
            engine = make_engine(url)
            self.assertNotIn(
                "download_progress",
                {
                    item["name"]
                    for item in inspect(engine).get_columns(
                        "artifact_preparation_attempts"
                    )
                },
            )
            self.assertFalse(
                ARTIFACT_CACHE_TABLES
                & set(inspect(engine).get_table_names())
            )
            engine.dispose()

            command.upgrade(migration_config, "head")
            self.assert_benchmark_head(url)

    def test_0010_rejects_invalid_cache_shape_ready_evidence_and_event_reason(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'cache-constraints.db'}"
            command.upgrade(config(url), "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                node = _node(session, "cache-constraints")
                artifact = ModelArtifact(
                    model_id="cache-constraints",
                    repository="Example/CacheConstraints",
                    revision="c" * 40,
                    manifest_digest="sha256:" + "d" * 64,
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1024,
                    layer_count=1,
                    license_id="apache-2.0",
                )
                session.add(artifact)
                session.flush()
                manifest = ArtifactManifest(
                    digest=artifact.manifest_digest,
                    schema_version=1,
                    model_artifact_id=artifact.id,
                    total_size_bytes=1,
                    file_count=1,
                    chunk_count=1,
                    canonical_json="{}",
                )
                session.add(manifest)
                session.commit()

                session.add(
                    NodeArtifactCache(
                        node_id=node.id,
                        cache_kind="FULL_SNAPSHOT",
                        cache_identity_digest="sha256:" + "e" * 64,
                        manifest_digest=manifest.digest,
                        source_manifest_digest=manifest.digest,
                        status="MISSING",
                        reason_code="PROBE_MISSING",
                        event_sequence=0,
                    )
                )
                with self.assertRaises(IntegrityError):
                    session.commit()
                session.rollback()

                session.add(
                    NodeArtifactCache(
                        node_id=node.id,
                        cache_kind="FULL_SNAPSHOT",
                        cache_identity_digest=manifest.digest,
                        manifest_digest=manifest.digest,
                        source_manifest_digest=manifest.digest,
                        status="READY",
                        reason_code="PREPARATION_SUCCEEDED",
                        event_sequence=0,
                    )
                )
                with self.assertRaises(IntegrityError):
                    session.commit()
                session.rollback()

                cache = NodeArtifactCache(
                    node_id=node.id,
                    cache_kind="FULL_SNAPSHOT",
                    cache_identity_digest=manifest.digest,
                    manifest_digest=manifest.digest,
                    source_manifest_digest=manifest.digest,
                    status="MISSING",
                    reason_code="PROBE_MISSING",
                    event_sequence=0,
                )
                session.add(cache)
                session.commit()
                session.add(
                    ArtifactCacheEvent(
                        cache_id=cache.id,
                        sequence=1,
                        previous_status=None,
                        status="MISSING",
                        reason_code="ARBITRARY_REMOTE_REASON",
                        source_kind="PROBE",
                        source_id="scan-1",
                        evidence_kind="PROBE_OBSERVATION",
                        evidence_digest="sha256:" + "f" * 64,
                    )
                )
                with self.assertRaises(IntegrityError):
                    session.commit()
                session.rollback()
            engine.dispose()

    def test_0010_migration_allows_insert_but_rejects_raw_event_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'cache-append-only.db'}"
            command.upgrade(config(url), "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                node = _node(session, "cache-append-only")
                artifact = ModelArtifact(
                    model_id="cache-append-only",
                    repository="Example/CacheAppendOnly",
                    revision="a" * 40,
                    manifest_digest="sha256:" + "b" * 64,
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1024,
                    layer_count=1,
                    license_id="apache-2.0",
                )
                session.add(artifact)
                session.flush()
                manifest = ArtifactManifest(
                    digest=artifact.manifest_digest,
                    schema_version=1,
                    model_artifact_id=artifact.id,
                    total_size_bytes=1,
                    file_count=1,
                    chunk_count=1,
                    canonical_json="{}",
                )
                session.add(manifest)
                session.flush()
                cache = NodeArtifactCache(
                    node_id=node.id,
                    cache_kind="FULL_SNAPSHOT",
                    cache_identity_digest=manifest.digest,
                    manifest_digest=manifest.digest,
                    source_manifest_digest=manifest.digest,
                    status="MISSING",
                    reason_code="PROBE_MISSING",
                    event_sequence=1,
                )
                session.add(cache)
                session.flush()
                event = ArtifactCacheEvent(
                    cache_id=cache.id,
                    sequence=1,
                    previous_status=None,
                    status="MISSING",
                    reason_code="PROBE_MISSING",
                    source_kind="PROBE",
                    source_id="migration-scan-1",
                    evidence_kind="PROBE_OBSERVATION",
                    evidence_digest="sha256:" + "c" * 64,
                )
                session.add(event)
                session.commit()
                event_id = event.id

                replay = session.scalar(
                    select(ArtifactCacheEvent).where(
                        ArtifactCacheEvent.cache_id == cache.id,
                        ArtifactCacheEvent.source_kind == "PROBE",
                        ArtifactCacheEvent.source_id == "migration-scan-1",
                        ArtifactCacheEvent.reason_code == "PROBE_MISSING",
                    )
                )
                self.assertEqual(replay.id, event_id)
                with self.assertRaisesRegex(
                    OperationalError, "no such column: rowid"
                ):
                    session.execute(
                        text(
                            "SELECT rowid FROM artifact_cache_events "
                            "WHERE id = :event_id"
                        ),
                        {"event_id": event_id},
                    )
                session.rollback()

                with self.assertRaisesRegex(IntegrityError, "append-only"):
                    session.execute(
                        text(
                            "UPDATE artifact_cache_events "
                            "SET source_id = :source_id WHERE id = :event_id"
                        ),
                        {
                            "source_id": "raw-update-must-fail",
                            "event_id": event_id,
                        },
                    )
                session.rollback()

                with self.assertRaisesRegex(IntegrityError, "append-only"):
                    session.execute(
                        text(
                            "DELETE FROM artifact_cache_events "
                            "WHERE id = :event_id"
                        ),
                        {"event_id": event_id},
                    )
                session.rollback()

                with self.assertRaisesRegex(IntegrityError, "append-only"):
                    session.execute(
                        text(
                            "INSERT OR REPLACE INTO artifact_cache_events ("
                            "id, cache_id, sequence, previous_status, status, "
                            "reason_code, source_kind, source_id, "
                            "source_attempt_id, source_task_id, evidence_kind, "
                            "evidence_digest, created_at) "
                            "SELECT id, cache_id, sequence, previous_status, "
                            "status, reason_code, source_kind, :source_id, "
                            "source_attempt_id, source_task_id, evidence_kind, "
                            "evidence_digest, created_at "
                            "FROM artifact_cache_events WHERE id = :event_id"
                        ),
                        {
                            "source_id": "raw-replace-must-fail",
                            "event_id": event_id,
                        },
                    )
                session.rollback()

                preserved = session.get(ArtifactCacheEvent, event_id)
                self.assertIsNotNone(preserved)
                self.assertEqual(preserved.source_id, "migration-scan-1")
                self.assertEqual(
                    session.scalar(
                        select(func.count()).select_from(ArtifactCacheEvent)
                    ),
                    1,
                )
            engine.dispose()

    def test_true_0008_upgrade_and_empty_round_trip_preserve_registry_records(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'stage-upgrade.db'}"
            migration_config = true_0008_database(url)
            engine = make_engine(url)
            self.assertFalse(
                STAGE_ARTIFACT_TABLES & set(inspect(engine).get_table_names())
            )
            factory = make_session_factory(engine)
            with factory() as session:
                artifact = ModelArtifact(
                    id=str(uuid.uuid4()),
                    model_id="stage-upgrade-source",
                    repository="Example/StageUpgrade",
                    revision="a" * 40,
                    manifest_digest="sha256:" + "b" * 64,
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1024,
                    layer_count=1,
                    license_id="apache-2.0",
                )
                runtime = RuntimeRelease(
                    id=str(uuid.uuid4()),
                    version="stage-upgrade-runtime",
                    image="registry.example/vllm@sha256:" + "c" * 64,
                    vllm_version="0.9.0",
                    cuda_version="12.4",
                    gpu_architectures=["ampere"],
                )
                session.add_all((artifact, runtime))
                session.commit()
                artifact_id = artifact.id
                runtime_id = runtime.id
            engine.dispose()

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                self.assertIsNotNone(session.get(ModelArtifact, artifact_id))
                self.assertIsNotNone(session.get(RuntimeRelease, runtime_id))
            engine.dispose()

            command.downgrade(migration_config, "0008")

            engine = make_engine(url)
            self.assertFalse(
                STAGE_ARTIFACT_TABLES & set(inspect(engine).get_table_names())
            )
            factory = make_session_factory(engine)
            with factory() as session:
                self.assertIsNotNone(session.get(ModelArtifact, artifact_id))
                self.assertIsNotNone(session.get(RuntimeRelease, runtime_id))
            engine.dispose()

            command.upgrade(migration_config, "head")
            self.assert_benchmark_head(url)

    def test_0009_database_rejects_invalid_topology_duplicates_and_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'stage-constraints.db'}"
            command.upgrade(config(url), "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                _artifact, _runtime, _source, stage, variant = _seed_stage_variant(
                    session,
                    suffix="constraints",
                )
                invalid_rank = StageArtifactRank(
                    variant_id=variant.artifact_set_digest,
                    rank=1,
                    pipeline_rank=0,
                    tensor_rank=0,
                    tensor_parallel_size=1,
                    pipeline_parallel_size=1,
                    manifest_digest=stage.digest,
                    tensor_key_count=1,
                    tensor_keys_digest="sha256:" + "7" * 64,
                    weight_size_bytes=1,
                )
                session.add(invalid_rank)
                with self.assertRaises(IntegrityError):
                    session.commit()
                session.rollback()

                session.add(
                    StageArtifactVariant(
                        artifact_set_digest="sha256:" + "b" * 64,
                        contract_identity_digest="sha256:" + "c" * 64,
                        source_manifest_digest=variant.source_manifest_digest,
                        runtime_release_id=variant.runtime_release_id,
                        runtime_image=variant.runtime_image,
                        vllm_version="0.9.0",
                        exporter_build_digest="sha256:" + "d" * 64,
                        architecture="Qwen2ForCausalLM",
                        quantization="awq",
                        tensor_parallel_size=1,
                        pipeline_parallel_size=1,
                        rank_count=1,
                        loader_format="VLLM_SHARDED_STATE_V1",
                        status="ACTIVE",
                        canonical_identity_json="{}",
                    )
                )
                with self.assertRaises(IntegrityError):
                    session.commit()
                session.rollback()

                rank = StageArtifactRank(
                    variant_id=variant.artifact_set_digest,
                    rank=0,
                    pipeline_rank=0,
                    tensor_rank=0,
                    tensor_parallel_size=1,
                    pipeline_parallel_size=1,
                    manifest_digest=stage.digest,
                    tensor_key_count=1,
                    tensor_keys_digest="sha256:" + "7" * 64,
                    weight_size_bytes=1,
                )
                session.add(rank)
                session.commit()
                session.add(
                    StageArtifactRank(
                        variant_id=variant.artifact_set_digest,
                        rank=0,
                        pipeline_rank=0,
                        tensor_rank=0,
                        tensor_parallel_size=1,
                        pipeline_parallel_size=1,
                        manifest_digest=stage.digest,
                        tensor_key_count=1,
                        tensor_keys_digest="sha256:" + "7" * 64,
                        weight_size_bytes=1,
                    )
                )
                with self.assertRaises(IntegrityError):
                    session.commit()
                session.rollback()

                invalid_evidence = StageArtifactValidationEvidence(
                    identity_digest="sha256:" + "8" * 64,
                    variant_id=variant.artifact_set_digest,
                    validation_run_id=str(uuid.uuid4()),
                    registration_sequence=1,
                    schema_version=1,
                    kind="GPU_EXPORT_LOAD",
                    status="PASSED",
                    validator_version="validator-1",
                    validator_build_digest="sha256:" + "9" * 64,
                    rank_count=0,
                    failure_code=None,
                    canonical_evidence_json="{}",
                )
                session.add(invalid_evidence)
                with self.assertRaises(IntegrityError):
                    session.commit()
                session.rollback()

                evidence = StageArtifactValidationEvidence(
                    identity_digest="sha256:" + "8" * 64,
                    variant_id=variant.artifact_set_digest,
                    validation_run_id=str(uuid.uuid4()),
                    registration_sequence=1,
                    schema_version=1,
                    kind="GPU_EXPORT_LOAD",
                    status="PASSED",
                    validator_version="validator-1",
                    validator_build_digest="sha256:" + "9" * 64,
                    rank_count=1,
                    failure_code=None,
                    canonical_evidence_json="{}",
                )
                session.add(evidence)
                session.commit()
                session.add(
                    StageArtifactValidationRank(
                        evidence_id=evidence.identity_digest,
                        rank=0,
                        variant_id=variant.artifact_set_digest,
                        manifest_digest=stage.digest,
                        tensor_keys_digest="sha256:" + "a" * 64,
                        loaded_tensor_count=1,
                        loaded_weight_size_bytes=1,
                    )
                )
                with self.assertRaises(IntegrityError):
                    session.commit()
                session.rollback()
            engine.dispose()

    def test_0009_downgrade_rejects_registered_stage_variant(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'stage-downgrade.db'}"
            migration_config = config(url)
            command.upgrade(migration_config, "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                _seed_stage_variant(session, suffix="downgrade")
            engine.dispose()

            with self.assertRaisesRegex(
                RuntimeError,
                "refusing to downgrade 0009",
            ):
                command.downgrade(migration_config, "0008")

            engine = make_engine(url)
            self.assertTrue(
                STAGE_ARTIFACT_TABLES <= set(inspect(engine).get_table_names())
            )
            engine.dispose()

    def test_true_0007_upgrade_and_empty_round_trip_preserve_generation(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'preparation-upgrade.db'}"
            migration_config = true_0007_database(url)
            engine = make_engine(url)
            self.assertFalse(
                ARTIFACT_PREPARATION_TABLES
                & set(inspect(engine).get_table_names())
            )
            factory = make_session_factory(engine)
            deployment_id = "preparation-upgrade-generation"
            manifest_digest = "sha256:" + "1" * 64
            with factory() as session:
                session.add(
                    Deployment(
                        id=deployment_id,
                        lineage_id=deployment_id,
                        generation=1,
                        plan={"deployment_id": deployment_id, "generation": 1},
                        accept_model_download=False,
                        pull_image=False,
                        status="CREATED",
                    )
                )
                session.add(
                    ArtifactManifest(
                        digest=manifest_digest,
                        schema_version=1,
                        model_artifact_id=None,
                        total_size_bytes=1,
                        file_count=1,
                        chunk_count=1,
                        canonical_json="{}",
                    )
                )
                session.commit()
            engine.dispose()

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                self.assertIsNotNone(session.get(Deployment, deployment_id))
                self.assertIsNotNone(
                    session.get(ArtifactManifest, manifest_digest)
                )
            engine.dispose()

            command.downgrade(migration_config, "0007")

            engine = make_engine(url)
            self.assertFalse(
                ARTIFACT_PREPARATION_TABLES
                & set(inspect(engine).get_table_names())
            )
            factory = make_session_factory(engine)
            with factory() as session:
                self.assertIsNotNone(session.get(Deployment, deployment_id))
                self.assertIsNotNone(
                    session.get(ArtifactManifest, manifest_digest)
                )
            engine.dispose()

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)

    def test_0008_database_rejects_invalid_states_stages_and_attempts(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'preparation-constraints.db'}"
            command.upgrade(config(url), "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            manifest_digest = "sha256:" + "2" * 64
            deployment_id = "preparation-constraints-generation"
            invalid_deployment_id = "preparation-invalid-generation"
            image = "registry.example/runtime@sha256:" + "3" * 64
            with factory() as session:
                node = _node(session, "preparation-constraints")
                invalid_node = _node(session, "preparation-invalid")
                for current_deployment_id in (
                    deployment_id,
                    invalid_deployment_id,
                ):
                    session.add(
                        Deployment(
                            id=current_deployment_id,
                            lineage_id=current_deployment_id,
                            generation=1,
                            plan={
                                "deployment_id": current_deployment_id,
                                "generation": 1,
                            },
                            accept_model_download=False,
                            pull_image=False,
                            status="CREATED",
                        )
                    )
                session.add(
                    ArtifactManifest(
                        digest=manifest_digest,
                        schema_version=1,
                        model_artifact_id=None,
                        total_size_bytes=1,
                        file_count=1,
                        chunk_count=1,
                        canonical_json="{}",
                    )
                )
                session.flush()
                preparation = ArtifactPreparation(
                    request_id=str(uuid.uuid4()),
                    request_digest="sha256:" + "4" * 64,
                    deployment_id=deployment_id,
                    status="QUEUED",
                    plan_snapshot={
                        "deployment_id": deployment_id,
                        "generation": 1,
                    },
                )
                session.add(preparation)
                session.flush()
                preparation_node = ArtifactPreparationNode(
                    preparation_id=preparation.id,
                    node_id=node.id,
                    model_manifest_digest=manifest_digest,
                    runtime_image=image,
                    model_status="QUEUED",
                    image_status="PREPARED",
                    model_current_attempt=1,
                    image_current_attempt=0,
                )
                session.add(preparation_node)
                tasks = []
                for ordinal in range(7):
                    task = Task(
                        bulk_id=str(uuid.uuid4()),
                        node_id=node.id,
                        type="PROBE",
                        status="QUEUED",
                        payload={"ordinal": ordinal},
                    )
                    session.add(task)
                    tasks.append(task)
                session.flush()
                session.add(
                    ArtifactPreparationAttempt(
                        preparation_node_id=preparation_node.id,
                        stage="MODEL",
                        attempt_no=1,
                        task_id=tasks[0].id,
                        status="QUEUED",
                    )
                )
                session.commit()
                preparation_id = preparation.id
                preparation_node_id = preparation_node.id
                invalid_node_id = invalid_node.id
                task_ids = [task.id for task in tasks]

            now = utcnow()

            def assert_insert_rejected(table, values) -> None:
                with self.assertRaises(IntegrityError):
                    with engine.begin() as connection:
                        connection.execute(table.insert().values(**values))

            preparation_values = {
                "id": str(uuid.uuid4()),
                "request_id": str(uuid.uuid4()),
                "request_digest": "sha256:" + "5" * 64,
                "deployment_id": invalid_deployment_id,
                "status": "UNKNOWN",
                "plan_snapshot": {"deployment_id": invalid_deployment_id},
                "created_at": now,
                "updated_at": now,
            }
            assert_insert_rejected(
                ArtifactPreparation.__table__,
                preparation_values,
            )

            node_values = {
                "id": str(uuid.uuid4()),
                "preparation_id": preparation_id,
                "node_id": invalid_node_id,
                "model_manifest_digest": manifest_digest,
                "runtime_image": image,
                "model_status": "PREPARED",
                "image_status": "PREPARED",
                "model_current_attempt": 1,
                "image_current_attempt": 0,
                "created_at": now,
                "updated_at": now,
            }
            assert_insert_rejected(
                ArtifactPreparationNode.__table__,
                node_values,
            )

            attempt_values = {
                "id": str(uuid.uuid4()),
                "preparation_node_id": preparation_node_id,
                "stage": "UNKNOWN",
                "attempt_no": 2,
                "task_id": task_ids[1],
                "status": "QUEUED",
                "created_at": now,
                "updated_at": now,
            }
            assert_insert_rejected(
                ArtifactPreparationAttempt.__table__,
                attempt_values,
            )
            assert_insert_rejected(
                ArtifactPreparationAttempt.__table__,
                {
                    **attempt_values,
                    "id": str(uuid.uuid4()),
                    "stage": "MODEL",
                    "attempt_no": 0,
                    "task_id": task_ids[2],
                },
            )
            assert_insert_rejected(
                ArtifactPreparationAttempt.__table__,
                {
                    **attempt_values,
                    "id": str(uuid.uuid4()),
                    "stage": "MODEL",
                    "attempt_no": 2,
                    "task_id": task_ids[3],
                    "status": "SUCCEEDED",
                    "completed_at": None,
                },
            )
            assert_insert_rejected(
                ArtifactPreparationAttempt.__table__,
                {
                    **attempt_values,
                    "id": str(uuid.uuid4()),
                    "stage": "IMAGE",
                    "attempt_no": 1,
                    "task_id": task_ids[4],
                    "status": "RUNNING",
                    "completed_at": now,
                },
            )
            assert_insert_rejected(
                ArtifactPreparationAttempt.__table__,
                {
                    **attempt_values,
                    "id": str(uuid.uuid4()),
                    "stage": "IMAGE",
                    "attempt_no": 1,
                    "task_id": task_ids[0],
                },
            )
            assert_insert_rejected(
                ArtifactPreparationAttempt.__table__,
                {
                    **attempt_values,
                    "id": str(uuid.uuid4()),
                    "stage": "MODEL",
                    "attempt_no": 1,
                    "task_id": task_ids[5],
                },
            )
            assert_insert_rejected(
                ArtifactPreparationAttempt.__table__,
                {
                    **attempt_values,
                    "id": str(uuid.uuid4()),
                    "stage": "IMAGE",
                    "attempt_no": 2,
                    "task_id": str(uuid.uuid4()),
                },
            )
            engine.dispose()

    def test_0008_downgrade_rejects_preparation_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'preparation-downgrade.db'}"
            migration_config = config(url)
            command.upgrade(migration_config, "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            deployment_id = "preparation-downgrade-generation"
            with factory() as session:
                session.add(
                    Deployment(
                        id=deployment_id,
                        lineage_id=deployment_id,
                        generation=1,
                        plan={"deployment_id": deployment_id, "generation": 1},
                        accept_model_download=False,
                        pull_image=False,
                        status="CREATED",
                    )
                )
                session.flush()
                session.add(
                    ArtifactPreparation(
                        request_id=str(uuid.uuid4()),
                        request_digest="sha256:" + "6" * 64,
                        deployment_id=deployment_id,
                        status="PREPARED",
                        plan_snapshot={
                            "deployment_id": deployment_id,
                            "generation": 1,
                        },
                    )
                )
                session.commit()
            engine.dispose()

            with self.assertRaisesRegex(
                RuntimeError,
                "artifact preparation data exists",
            ):
                command.downgrade(migration_config, "0007")

            engine = make_engine(url)
            self.assertTrue(
                ARTIFACT_PREPARATION_TABLES
                <= set(inspect(engine).get_table_names())
            )
            with engine.connect() as connection:
                self.assertEqual(
                    connection.scalar(
                        text("SELECT version_num FROM alembic_version")
                    ),
                    "0008",
                )
                self.assertEqual(
                    connection.scalar(
                        text("SELECT COUNT(*) FROM artifact_preparations")
                    ),
                    1,
                )
            engine.dispose()

    def test_true_0006_upgrade_and_empty_round_trip_preserve_legacy_artifact(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'artifact-upgrade.db'}"
            migration_config = true_0006_database(url)
            engine = make_engine(url)
            inspector = inspect(engine)
            self.assertFalse(
                ARTIFACT_MANIFEST_TABLES & set(inspector.get_table_names())
            )
            self.assertNotIn(
                "uq_model_artifacts_id_manifest_digest",
                {
                    item["name"]
                    for item in inspector.get_unique_constraints(
                        "model_artifacts"
                    )
                },
            )
            factory = make_session_factory(engine)
            manifest_digest = "sha256:" + "7" * 64
            with factory() as session:
                artifact = ModelArtifact(
                    model_id="legacy-artifact",
                    repository="Example/LegacyArtifact",
                    revision="a" * 40,
                    manifest_digest=manifest_digest,
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1,
                    layer_count=1,
                    license_id="apache-2.0",
                )
                session.add(artifact)
                session.commit()
                artifact_id = artifact.id
            engine.dispose()

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                preserved = session.get(ModelArtifact, artifact_id)
                self.assertEqual(preserved.manifest_digest, manifest_digest)
                self.assertEqual(
                    session.scalar(
                        select(func.count()).select_from(ArtifactManifest)
                    ),
                    0,
                )
            engine.dispose()

            command.downgrade(migration_config, "0006")

            engine = make_engine(url)
            inspector = inspect(engine)
            self.assertFalse(
                ARTIFACT_MANIFEST_TABLES & set(inspector.get_table_names())
            )
            factory = make_session_factory(engine)
            with factory() as session:
                preserved = session.get(ModelArtifact, artifact_id)
                self.assertEqual(preserved.manifest_digest, manifest_digest)
            engine.dispose()

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                preserved = session.get(ModelArtifact, artifact_id)
                self.assertEqual(preserved.manifest_digest, manifest_digest)
                self.assertEqual(
                    session.scalar(
                        select(func.count()).select_from(ArtifactManifest)
                    ),
                    0,
                )
            engine.dispose()

    def test_0007_composite_identity_rejects_mismatch_but_allows_generic_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'artifact-identity.db'}"
            command.upgrade(config(url), "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            source_digest = "sha256:" + "8" * 64
            with factory() as session:
                artifact = ModelArtifact(
                    model_id="identity-artifact",
                    repository="Example/IdentityArtifact",
                    revision="b" * 40,
                    manifest_digest=source_digest,
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1,
                    layer_count=1,
                    license_id="apache-2.0",
                )
                session.add(artifact)
                session.commit()
                artifact_id = artifact.id

            mismatched_digest = "sha256:" + "9" * 64
            with engine.connect() as connection:
                self.assertEqual(
                    connection.exec_driver_sql(
                        "PRAGMA foreign_keys"
                    ).scalar_one(),
                    1,
                )
                with self.assertRaises(IntegrityError):
                    connection.execute(
                        ArtifactManifest.__table__.insert().values(
                            digest=mismatched_digest,
                            schema_version=1,
                            model_artifact_id=artifact_id,
                            total_size_bytes=1,
                            file_count=1,
                            chunk_count=1,
                            canonical_json="{}",
                            created_at=utcnow(),
                        )
                    )
                connection.rollback()

            generic_digest = "sha256:" + "a" * 64
            with factory() as session:
                session.add(
                    ArtifactManifest(
                        digest=generic_digest,
                        schema_version=1,
                        model_artifact_id=None,
                        total_size_bytes=1,
                        file_count=1,
                        chunk_count=1,
                        canonical_json="{}",
                    )
                )
                session.commit()
                self.assertIsNotNone(
                    session.get(ArtifactManifest, generic_digest)
                )
            engine.dispose()

    def test_0007_downgrade_rejects_registered_manifest_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'artifact-downgrade.db'}"
            migration_config = config(url)
            command.upgrade(migration_config, "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            manifest_digest = "sha256:" + "b" * 64
            chunk_digest = "sha256:" + "c" * 64
            file_digest = "sha256:" + "d" * 64
            with factory() as session:
                artifact = ModelArtifact(
                    model_id="downgrade-artifact",
                    repository="Example/DowngradeArtifact",
                    revision="c" * 40,
                    manifest_digest=manifest_digest,
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1,
                    layer_count=1,
                    license_id="apache-2.0",
                )
                session.add(artifact)
                session.flush()
                session.add(
                    ArtifactManifest(
                        digest=manifest_digest,
                        schema_version=1,
                        model_artifact_id=artifact.id,
                        total_size_bytes=16,
                        file_count=1,
                        chunk_count=1,
                        canonical_json="{}",
                    )
                )
                session.add(ArtifactChunk(digest=chunk_digest, size_bytes=16))
                session.flush()
                manifest_file = ArtifactManifestFile(
                    manifest_digest=manifest_digest,
                    ordinal=0,
                    path="weights/model.safetensors",
                    kind="REGULAR",
                    size_bytes=16,
                    file_digest=file_digest,
                )
                session.add(manifest_file)
                session.flush()
                session.add(
                    ArtifactFileChunk(
                        file_id=manifest_file.id,
                        ordinal=0,
                        chunk_digest=chunk_digest,
                        offset_bytes=0,
                        length_bytes=16,
                    )
                )
                session.commit()
            engine.dispose()

            with self.assertRaisesRegex(
                RuntimeError,
                "artifact manifest data exists",
            ):
                command.downgrade(migration_config, "0006")

            engine = make_engine(url)
            self.assertTrue(
                ARTIFACT_MANIFEST_TABLES <= set(inspect(engine).get_table_names())
            )
            with engine.connect() as connection:
                self.assertEqual(
                    connection.scalar(text("SELECT version_num FROM alembic_version")),
                    "0007",
                )
                for table_name in ARTIFACT_MANIFEST_TABLES:
                    self.assertEqual(
                        connection.scalar(
                            text(f"SELECT COUNT(*) FROM {table_name}")
                        ),
                        1,
                        table_name,
                    )
            engine.dispose()

    def test_legacy_0001_database_upgrades_to_benchmark_head(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'legacy.db'}"
            engine = make_engine(url)
            Base.metadata.create_all(engine)
            for table in (
                ArtifactPreparationAttempt.__table__,
                ArtifactPreparationNode.__table__,
                ArtifactPreparation.__table__,
                ArtifactFileChunk.__table__,
                ArtifactManifestFile.__table__,
                ArtifactChunk.__table__,
                ArtifactManifest.__table__,
                DeploymentOperationNode.__table__,
                DeploymentOperation.__table__,
                BenchmarkRun.__table__,
                BenchmarkEvidence.__table__,
                DeploymentRecommendationRecord.__table__,
                PlacementProfileRecord.__table__,
                ModelRelease.__table__,
                RuntimeRelease.__table__,
                ModelArtifact.__table__,
            ):
                table.drop(engine)
            engine.dispose()
            migration_config = config(url)
            command.stamp(migration_config, "0001")

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)

    def test_0002_database_upgrades_to_benchmark_head(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'registry.db'}"
            engine = make_engine(url)
            Base.metadata.create_all(engine)
            factory = make_session_factory(engine)
            with factory() as session:
                artifact = create_model_artifact(
                    session,
                    model_id="migration-model",
                    repository="Example/Migration",
                    revision="a" * 40,
                    manifest_digest="sha256:" + "b" * 64,
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1,
                    layer_count=1,
                    license_id="apache-2.0",
                )
                runtime = create_runtime_release(
                    session,
                    version="migration-runtime",
                    image="registry.example/runtime@sha256:" + "c" * 64,
                    vllm_version="0.9.0",
                    cuda_version="12.8",
                    gpu_architectures=["ampere"],
                )
                release = create_model_release(
                    session,
                    artifact_id=artifact.id,
                    runtime_id=runtime.id,
                    quality_rank=1,
                )
                release_id = release.id
            for table in (
                ArtifactPreparationAttempt.__table__,
                ArtifactPreparationNode.__table__,
                ArtifactPreparation.__table__,
            ):
                table.drop(engine, checkfirst=True)
            BenchmarkRun.__table__.drop(engine)
            BenchmarkEvidence.__table__.drop(engine)
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE model_releases SET status = 'ACTIVE' "
                        "WHERE id = :release_id"
                    ),
                    {"release_id": release_id},
                )
                connection.execute(
                    text("ALTER TABLE model_releases DROP COLUMN promotion_evidence_digest")
                )
                connection.execute(
                    text("ALTER TABLE model_releases DROP COLUMN promotion_evidence_ids")
                )
            engine.dispose()
            migration_config = config(url)
            command.stamp(migration_config, "0002")

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)
            engine = make_engine(url)
            with engine.connect() as connection:
                preserved = connection.execute(
                    text(
                        "SELECT id, status FROM model_releases "
                        "WHERE id = :release_id"
                    ),
                    {"release_id": release_id},
                ).one()
            self.assertEqual(tuple(preserved), (release_id, "VALIDATED"))
            engine.dispose()

    def test_0003_database_preserves_existing_evidence_when_adding_runs(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'evidence.db'}"
            migration_config = true_0003_database(url)
            engine = make_engine(url)
            before = inspect(engine)
            with engine.connect() as connection:
                self.assertEqual(
                    connection.scalar(text("SELECT version_num FROM alembic_version")),
                    "0003",
                )
            self.assertNotIn("benchmark_runs", before.get_table_names())
            self.assertNotIn(
                "benchmark_run_id",
                {
                    column["name"]
                    for column in before.get_columns("benchmark_evidence")
                },
            )
            self.assertEqual(
                {
                    "ix_benchmark_evidence_release_id",
                    "ix_benchmark_evidence_placement_id",
                    "ix_benchmark_evidence_status",
                },
                {
                    index["name"]
                    for index in before.get_indexes("benchmark_evidence")
                },
            )
            factory = make_session_factory(engine)
            with factory() as session:
                node = _node(session, "migration-0003")
                artifact, runtime, release, placements = _release(
                    session, "migration-0003"
                )
                body = _evidence_body(
                    session,
                    artifact,
                    runtime,
                    release,
                    placements[0],
                    [node],
                )
            legacy_id = str(uuid.uuid4())
            legacy_digest = "sha256:" + "e" * 64
            legacy_evidence = Table(
                "benchmark_evidence", MetaData(), autoload_with=engine
            )
            with engine.begin() as connection:
                connection.execute(
                    legacy_evidence.insert().values(
                        **body,
                        id=legacy_id,
                        registration_sequence=1,
                        status="PASSED",
                        failure_codes=[],
                        evidence_digest=legacy_digest,
                        created_at=utcnow(),
                    )
                )
            engine.dispose()

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                preserved = session.get(BenchmarkEvidence, legacy_id)
                self.assertIsNotNone(preserved)
                self.assertEqual(
                    (preserved.id, preserved.evidence_digest, preserved.status),
                    (legacy_id, legacy_digest, "PASSED"),
                )
                self.assertIsNone(preserved.benchmark_run_id)
                self.assertEqual(
                    session.scalar(select(func.count()).select_from(BenchmarkRun)),
                    0,
                )
            engine.dispose()

    def test_true_0004_database_upgrades_and_backfills_legacy_deployment(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'recommendation-upgrade.db'}"
            migration_config = true_0004_database(url)
            engine = make_engine(url)
            inspector = inspect(engine)
            self.assertNotIn(
                "deployment_recommendations",
                inspector.get_table_names(),
            )
            self.assertFalse(
                {
                    "lineage_id",
                    "previous_generation_id",
                    "source_recommendation_id",
                }
                & {
                    item["name"]
                    for item in inspector.get_columns("deployments")
                }
            )
            legacy = Table("deployments", MetaData(), autoload_with=engine)
            legacy_plan = {
                "deployment_id": "legacy-deployment",
                "generation": 0,
                "model": {"model_id": "legacy-model"},
            }
            with engine.begin() as connection:
                connection.execute(
                    legacy.insert().values(
                        id="legacy-deployment",
                        generation=0,
                        plan=legacy_plan,
                        accept_model_download=False,
                        pull_image=False,
                        status="CREATED",
                        created_at=utcnow(),
                    )
                )
            engine.dispose()

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                restored = session.get(Deployment, "legacy-deployment")
                self.assertIsNotNone(restored)
                self.assertEqual(restored.lineage_id, restored.id)
                self.assertIsNone(restored.previous_generation_id)
                self.assertIsNone(restored.source_recommendation_id)
                self.assertEqual(restored.generation, 0)
                self.assertEqual(restored.plan, legacy_plan)
                self.assertIsNone(restored.verified_at)
            engine.dispose()

    def test_true_0005_database_upgrades_and_preserves_deployment_and_task(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'operations-upgrade.db'}"
            migration_config = true_0005_database(url)
            engine = make_engine(url)
            inspector = inspect(engine)
            self.assertNotIn("deployment_operations", inspector.get_table_names())
            self.assertNotIn(
                "verified_at",
                {item["name"] for item in inspector.get_columns("deployments")},
            )
            self.assertFalse(
                {"operation_node_id", "operation_attempt"}
                & {item["name"] for item in inspector.get_columns("tasks")}
            )
            node_id = str(uuid.uuid4())
            deployment_id = "legacy-generation-zero"
            task_id = str(uuid.uuid4())
            frozen_plan = {
                "deployment_id": deployment_id,
                "generation": 0,
                "model": {"model_id": "legacy-model"},
            }
            nodes = Table("nodes", MetaData(), autoload_with=engine)
            deployments = Table("deployments", MetaData(), autoload_with=engine)
            tasks = Table("tasks", MetaData(), autoload_with=engine)
            with engine.begin() as connection:
                connection.execute(
                    nodes.insert().values(
                        id=node_id,
                        install_id="install-operations-upgrade",
                        display_name="operations-upgrade",
                        hostname="operations-upgrade",
                        agent_version="0.3.11",
                        approved=True,
                        created_at=utcnow(),
                    )
                )
                connection.execute(
                    deployments.insert().values(
                        id=deployment_id,
                        lineage_id=deployment_id,
                        previous_generation_id=None,
                        source_recommendation_id=None,
                        generation=0,
                        plan=frozen_plan,
                        accept_model_download=False,
                        pull_image=False,
                        status="CREATED",
                        created_at=utcnow(),
                    )
                )
                connection.execute(
                    tasks.insert().values(
                        id=task_id,
                        bulk_id=str(uuid.uuid4()),
                        node_id=node_id,
                        type="APPLY_DEPLOYMENT",
                        status="SUCCEEDED",
                        deployment_id=deployment_id,
                        payload={"legacy": True},
                        attempts=1,
                        result={"ok": True},
                        created_at=utcnow(),
                        updated_at=utcnow(),
                    )
                )
            engine.dispose()

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                restored_deployment = session.get(Deployment, deployment_id)
                restored_task = session.get(Task, task_id)
                self.assertEqual(restored_deployment.generation, 0)
                self.assertEqual(restored_deployment.plan, frozen_plan)
                self.assertIsNone(restored_deployment.verified_at)
                self.assertEqual(restored_task.payload, {"legacy": True})
                self.assertEqual(restored_task.status, "SUCCEEDED")
                self.assertIsNone(restored_task.operation_node_id)
                self.assertIsNone(restored_task.operation_attempt)
                self.assertEqual(
                    session.scalar(
                        select(func.count()).select_from(DeploymentOperation)
                    ),
                    0,
                )
            engine.dispose()

    def test_0006_downgrade_and_reupgrade_preserve_base_records(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'operations-round-trip.db'}"
            migration_config = config(url)
            command.upgrade(migration_config, "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            deployment_id = "operation-round-trip"
            frozen_plan = {
                "deployment_id": deployment_id,
                "generation": 1,
                "model": {"model_id": "round-trip-model"},
                "image": "registry.example/runtime@sha256:" + "a" * 64,
            }
            with factory() as session:
                node = _node(session, "operation-round-trip")
                deployment = Deployment(
                    id=deployment_id,
                    lineage_id=deployment_id,
                    generation=1,
                    plan=frozen_plan,
                    accept_model_download=False,
                    pull_image=False,
                    status="CREATED",
                )
                session.add(deployment)
                session.flush()
                operation = DeploymentOperation(
                    request_digest="sha256:" + "b" * 64,
                    lineage_id=deployment_id,
                    deployment_id=deployment_id,
                    kind="APPLY",
                    status="SUCCEEDED",
                    phase="COMPLETE",
                    node_ids=[node.id],
                    serve=False,
                    api=False,
                    completed_at=utcnow(),
                )
                session.add(operation)
                session.flush()
                operation_node = DeploymentOperationNode(
                    operation_id=operation.id,
                    node_id=node.id,
                    phase="APPLY",
                    status="SUCCEEDED",
                    attempt_count=1,
                    completed_at=utcnow(),
                )
                session.add(operation_node)
                session.flush()
                task = Task(
                    bulk_id=str(uuid.uuid4()),
                    node_id=node.id,
                    type="APPLY_DEPLOYMENT",
                    status="SUCCEEDED",
                    deployment_id=deployment_id,
                    operation_node_id=operation_node.id,
                    operation_attempt=1,
                    payload={"plan": frozen_plan},
                    attempts=1,
                    result={"ok": True},
                )
                session.add(task)
                session.commit()
                node_id = node.id
                operation_id = operation.id
                task_id = task.id
            engine.dispose()

            command.downgrade(migration_config, "0005")

            engine = make_engine(url)
            inspector = inspect(engine)
            self.assertNotIn("deployment_operations", inspector.get_table_names())
            self.assertNotIn(
                "deployment_operation_nodes", inspector.get_table_names()
            )
            self.assertNotIn(
                "verified_at",
                {item["name"] for item in inspector.get_columns("deployments")},
            )
            self.assertFalse(
                {"operation_node_id", "operation_attempt"}
                & {item["name"] for item in inspector.get_columns("tasks")}
            )
            deployments = Table("deployments", MetaData(), autoload_with=engine)
            tasks = Table("tasks", MetaData(), autoload_with=engine)
            with engine.connect() as connection:
                preserved_deployment = connection.execute(
                    select(
                        deployments.c.id,
                        deployments.c.generation,
                        deployments.c.plan,
                        deployments.c.status,
                    ).where(deployments.c.id == deployment_id)
                ).one()
                preserved_task = connection.execute(
                    select(
                        tasks.c.id,
                        tasks.c.node_id,
                        tasks.c.deployment_id,
                        tasks.c.status,
                        tasks.c.payload,
                    ).where(tasks.c.id == task_id)
                ).one()
            self.assertEqual(
                tuple(preserved_deployment),
                (deployment_id, 1, frozen_plan, "CREATED"),
            )
            self.assertEqual(
                tuple(preserved_task),
                (
                    task_id,
                    node_id,
                    deployment_id,
                    "SUCCEEDED",
                    {"plan": frozen_plan},
                ),
            )
            engine.dispose()

            command.upgrade(migration_config, "head")

            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                restored_deployment = session.get(Deployment, deployment_id)
                restored_task = session.get(Task, task_id)
                self.assertEqual(restored_deployment.plan, frozen_plan)
                self.assertIsNone(restored_deployment.verified_at)
                self.assertEqual(restored_task.payload, {"plan": frozen_plan})
                self.assertIsNone(restored_task.operation_node_id)
                self.assertIsNone(restored_task.operation_attempt)
                self.assertIsNone(session.get(DeploymentOperation, operation_id))
            engine.dispose()

    def test_0006_downgrade_rejects_prepared_inactive_operation(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'active-operation.db'}"
            migration_config = config(url)
            command.upgrade(migration_config, "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            deployment_id = "active-operation"
            with factory() as session:
                session.add(
                    Deployment(
                        id=deployment_id,
                        lineage_id=deployment_id,
                        generation=1,
                        plan={"deployment_id": deployment_id, "generation": 1},
                        accept_model_download=False,
                        pull_image=False,
                        status="CREATED",
                    )
                )
                session.flush()
                session.add(
                    DeploymentOperation(
                        request_digest="sha256:" + "c" * 64,
                        lineage_id=deployment_id,
                        deployment_id=deployment_id,
                        kind="APPLY",
                        status="PREPARED",
                        phase="APPLY",
                        node_ids=[],
                        serve=False,
                        api=False,
                        active_lineage_id=None,
                    )
                )
                session.commit()
            engine.dispose()

            with self.assertRaisesRegex(RuntimeError, "operations are active"):
                command.downgrade(migration_config, "0005")

            engine = make_engine(url)
            with engine.connect() as connection:
                self.assertEqual(
                    connection.scalar(text("SELECT version_num FROM alembic_version")),
                    "0006",
                )
            self.assertIn("deployment_operations", inspect(engine).get_table_names())
            engine.dispose()

    def test_0006_downgrade_rejects_linked_queued_task(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'active-operation-task.db'}"
            migration_config = config(url)
            command.upgrade(migration_config, "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            deployment_id = "active-operation-task"
            with factory() as session:
                node = _node(session, "active-operation-task")
                session.add(
                    Deployment(
                        id=deployment_id,
                        lineage_id=deployment_id,
                        generation=1,
                        plan={"deployment_id": deployment_id, "generation": 1},
                        accept_model_download=False,
                        pull_image=False,
                        status="CREATED",
                    )
                )
                session.flush()
                operation = DeploymentOperation(
                    request_digest="sha256:" + "d" * 64,
                    lineage_id=deployment_id,
                    deployment_id=deployment_id,
                    kind="APPLY",
                    status="FAILED",
                    phase="COMPLETE",
                    node_ids=[node.id],
                    serve=False,
                    api=False,
                    completed_at=utcnow(),
                )
                session.add(operation)
                session.flush()
                operation_node = DeploymentOperationNode(
                    operation_id=operation.id,
                    node_id=node.id,
                    phase="APPLY",
                    status="QUEUED",
                    attempt_count=1,
                )
                session.add(operation_node)
                session.flush()
                session.add(
                    Task(
                        bulk_id=str(uuid.uuid4()),
                        node_id=node.id,
                        type="APPLY_DEPLOYMENT",
                        status="QUEUED",
                        deployment_id=deployment_id,
                        operation_node_id=operation_node.id,
                        operation_attempt=1,
                        payload={"plan": {"deployment_id": deployment_id}},
                    )
                )
                session.commit()
            engine.dispose()

            with self.assertRaisesRegex(RuntimeError, "operation tasks are active"):
                command.downgrade(migration_config, "0005")

            engine = make_engine(url)
            with engine.connect() as connection:
                self.assertEqual(
                    connection.scalar(text("SELECT version_num FROM alembic_version")),
                    "0006",
                )
            engine.dispose()

    def test_0005_downgrade_and_reupgrade_preserve_deployment_plan(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'recommendation-round-trip.db'}"
            migration_config = true_0004_database(url)
            command.upgrade(migration_config, "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            recommendation_id = "sha256:" + "a" * 64
            deployment_id = "accepted-generation-1"
            frozen_plan = {
                "deployment_id": deployment_id,
                "generation": 1,
                "model": {"model_id": "accepted-model"},
                "image": "registry.example/runtime@sha256:" + "b" * 64,
            }
            with factory() as session:
                session.add(
                    DeploymentRecommendationRecord(
                        id=recommendation_id,
                        objective="quality-first",
                        selection_mode="explicit_nodes",
                        requested_node_ids=[],
                        catalog_version="sha256:" + "c" * 64,
                        policy_version="central-quality-within-slo-v1",
                        inventory_fingerprint="sha256:" + "d" * 64,
                        recommendation_snapshot={"id": recommendation_id},
                        inventory_snapshot=[],
                    )
                )
                session.flush()
                session.add(
                    Deployment(
                        id=deployment_id,
                        lineage_id=deployment_id,
                        previous_generation_id=None,
                        source_recommendation_id=recommendation_id,
                        generation=1,
                        plan=frozen_plan,
                        accept_model_download=False,
                        pull_image=False,
                        status="CREATED",
                    )
                )
                session.commit()
            engine.dispose()

            command.downgrade(migration_config, "0004")

            engine = make_engine(url)
            inspector = inspect(engine)
            self.assertNotIn(
                "deployment_recommendations",
                inspector.get_table_names(),
            )
            self.assertFalse(
                {
                    "lineage_id",
                    "previous_generation_id",
                    "source_recommendation_id",
                }
                & {
                    item["name"]
                    for item in inspector.get_columns("deployments")
                }
            )
            legacy = Table("deployments", MetaData(), autoload_with=engine)
            with engine.connect() as connection:
                preserved = connection.execute(
                    select(
                        legacy.c.id,
                        legacy.c.generation,
                        legacy.c.plan,
                        legacy.c.accept_model_download,
                        legacy.c.pull_image,
                        legacy.c.status,
                    ).where(legacy.c.id == deployment_id)
                ).one()
            self.assertEqual(
                tuple(preserved),
                (
                    deployment_id,
                    1,
                    frozen_plan,
                    False,
                    False,
                    "CREATED",
                ),
            )
            engine.dispose()

            command.upgrade(migration_config, "head")
            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                restored = session.get(Deployment, deployment_id)
                self.assertEqual(restored.lineage_id, deployment_id)
                self.assertIsNone(restored.previous_generation_id)
                self.assertIsNone(restored.source_recommendation_id)
                self.assertEqual(restored.plan, frozen_plan)
                self.assertEqual(
                    session.scalar(
                        select(func.count()).select_from(
                            DeploymentRecommendationRecord
                        )
                    ),
                    0,
                )
            engine.dispose()

    def test_0004_downgrade_and_reupgrade_preserve_benchmark_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'round-trip.db'}"
            migration_config = true_0003_database(url)
            command.upgrade(migration_config, "head")
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                node = _node(session, "migration-round-trip")
                artifact, runtime, release, placements = _release(
                    session, "migration-round-trip"
                )
                run, _ = prepare_benchmark_run(
                    session,
                    request_id=str(uuid.uuid4()),
                    release_id=release.id,
                    placement_id=placements[0].id,
                    node_ids=[node.id],
                    workload_id="short-chat-1k-128",
                    dure_commit="d" * 40,
                )
                body = _evidence_body(
                    session,
                    artifact,
                    runtime,
                    release,
                    placements[0],
                    [node],
                    input_tokens=run.input_tokens,
                    output_tokens=run.output_tokens,
                    concurrency=run.concurrency,
                )
                linked = register_benchmark_evidence(
                    session, benchmark_run_id=run.id, **body
                )
                unlinked = register_benchmark_evidence(session, **body)
                run.evidence_id = linked.id
                run.status = "SUCCEEDED"
                session.commit()
                self.assertEqual(linked.benchmark_run_id, run.id)
                self.assertIsNone(unlinked.benchmark_run_id)
                self.assertEqual(run.evidence_id, linked.id)
                expected = [
                    (
                        evidence.id,
                        evidence.evidence_digest,
                        evidence.status,
                        evidence.registration_sequence,
                    )
                    for evidence in (linked, unlinked)
                ]
            engine.dispose()

            command.downgrade(migration_config, "0003")

            engine = make_engine(url)
            inspector = inspect(engine)
            self.assertNotIn("benchmark_runs", inspector.get_table_names())
            self.assertNotIn(
                "benchmark_run_id",
                {
                    column["name"]
                    for column in inspector.get_columns("benchmark_evidence")
                },
            )
            legacy_evidence = Table(
                "benchmark_evidence", MetaData(), autoload_with=engine
            )
            with engine.connect() as connection:
                preserved_at_0003 = [
                    tuple(row)
                    for row in connection.execute(
                        select(
                            legacy_evidence.c.id,
                            legacy_evidence.c.evidence_digest,
                            legacy_evidence.c.status,
                            legacy_evidence.c.registration_sequence,
                        ).order_by(legacy_evidence.c.registration_sequence)
                    )
                ]
            self.assertEqual(preserved_at_0003, expected)
            engine.dispose()

            command.upgrade(migration_config, "head")
            self.assert_benchmark_head(url)
            engine = make_engine(url)
            factory = make_session_factory(engine)
            with factory() as session:
                restored = list(
                    session.scalars(
                        select(BenchmarkEvidence).order_by(
                            BenchmarkEvidence.registration_sequence
                        )
                    )
                )
                self.assertEqual(
                    [
                        (
                            evidence.id,
                            evidence.evidence_digest,
                            evidence.status,
                            evidence.registration_sequence,
                        )
                        for evidence in restored
                    ],
                    expected,
                )
                self.assertTrue(
                    all(evidence.benchmark_run_id is None for evidence in restored)
                )
                self.assertEqual(
                    session.scalar(select(func.count()).select_from(BenchmarkRun)),
                    0,
                )
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
