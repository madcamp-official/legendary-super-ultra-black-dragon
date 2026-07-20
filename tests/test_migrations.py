from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect

from dure.control.db import Base, make_engine
from dure.control.models import (
    ModelArtifact,
    ModelRelease,
    PlacementProfileRecord,
    RuntimeRelease,
)


REGISTRY_TABLES = {
    "model_artifacts",
    "runtime_releases",
    "model_releases",
    "placement_profiles",
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
    def test_empty_database_upgrades_to_registry_head(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'empty.db'}"

            command.upgrade(config(url), "head")

            engine = make_engine(url)
            self.assertTrue(REGISTRY_TABLES <= set(inspect(engine).get_table_names()))
            engine.dispose()

    def test_legacy_0001_database_upgrades_to_registry_head(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'legacy.db'}"
            engine = make_engine(url)
            Base.metadata.create_all(engine)
            for table in (
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

            engine = make_engine(url)
            self.assertTrue(REGISTRY_TABLES <= set(inspect(engine).get_table_names()))
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
