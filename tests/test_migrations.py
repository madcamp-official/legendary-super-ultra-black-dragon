from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text

from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import (
    BenchmarkEvidence,
    ModelArtifact,
    ModelRelease,
    PlacementProfileRecord,
    RuntimeRelease,
)
from dure.control.service import (
    create_model_artifact,
    create_model_release,
    create_runtime_release,
)


REGISTRY_TABLES = {
    "model_artifacts",
    "runtime_releases",
    "model_releases",
    "placement_profiles",
}
HEAD_TABLES = REGISTRY_TABLES | {"benchmark_evidence"}
BENCHMARK_INDEXES = {
    "ix_benchmark_evidence_release_id",
    "ix_benchmark_evidence_placement_id",
    "ix_benchmark_evidence_status",
}


def config(url: str) -> Config:
    value = Config()
    value.set_main_option(
        "script_location",
        str(Path(__file__).parents[1] / "src" / "dure" / "control" / "migrations"),
    )
    value.set_main_option("sqlalchemy.url", url)
    return value


class MigrationTests(unittest.TestCase):
    def assert_benchmark_head(self, url: str) -> None:
        engine = make_engine(url)
        inspector = inspect(engine)
        self.assertTrue(HEAD_TABLES <= set(inspector.get_table_names()))
        self.assertEqual(
            BENCHMARK_INDEXES,
            {item["name"] for item in inspector.get_indexes("benchmark_evidence")},
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


if __name__ == "__main__":
    unittest.main()
