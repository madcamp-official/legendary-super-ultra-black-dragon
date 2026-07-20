from __future__ import annotations

import tempfile
import unittest
import uuid
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import MetaData, Table, func, inspect, select, text

from dure.control.db import Base, make_engine, make_session_factory
from dure.control.benchmark import register_benchmark_evidence
from dure.control.models import (
    BenchmarkEvidence,
    BenchmarkRun,
    ModelArtifact,
    ModelRelease,
    PlacementProfileRecord,
    RuntimeRelease,
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
HEAD_TABLES = REGISTRY_TABLES | {"benchmark_evidence", "benchmark_runs"}
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


class MigrationTests(unittest.TestCase):
    def assert_benchmark_head(self, url: str) -> None:
        engine = make_engine(url)
        inspector = inspect(engine)
        self.assertTrue(HEAD_TABLES <= set(inspector.get_table_names()))
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
        engine.dispose()

    def test_empty_database_upgrades_to_benchmark_head(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'empty.db'}"

            command.upgrade(config(url), "head")

            self.assert_benchmark_head(url)

    def test_legacy_0001_database_upgrades_to_benchmark_head(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'legacy.db'}"
            engine = make_engine(url)
            Base.metadata.create_all(engine)
            for table in (
                BenchmarkRun.__table__,
                BenchmarkEvidence.__table__,
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
