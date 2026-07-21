from __future__ import annotations

import io
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import CheckConstraint, ForeignKeyConstraint, inspect, text
from sqlalchemy.exc import IntegrityError

from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import FleetDeploymentRuntime, FleetRecord


def _config(url: str) -> Config:
    value = Config()
    value.set_main_option(
        "script_location",
        str(
            Path(__file__).parents[1]
            / "src"
            / "dure"
            / "control"
            / "migrations"
        ),
    )
    value.set_main_option("sqlalchemy.url", url)
    return value


def _seed_fleet_deployment(
    connection,
    *,
    marker: str,
    fleet_id: str,
    deployment_id: str,
) -> None:
    recommendation_id = "sha256:" + marker * 64
    created_at = datetime(2026, 7, 21, tzinfo=timezone.utc)
    connection.execute(
        text(
            "INSERT INTO fleet_recommendations "
            "(id, schema_version, objective, selection_mode, "
            "requested_node_ids, minimum_replicas, minimum_reserve_nodes, "
            "reserve_node_ids, inventory_fingerprint, "
            "source_inventory_fingerprint, catalog_version, "
            "catalog_policy_version, candidate_policy_version, "
            "scheduler_version, recommendation_snapshot, created_at) "
            "VALUES (:id, 1, 'quality-first', 'all_online', :empty_list, "
            ":empty_object, 0, :empty_list, :inventory, :source_inventory, "
            ":catalog, 'fleet-policy-v1', 'fleet-candidate-v2', "
            "'fleet-scheduler-v1', :empty_object, :created_at)"
        ),
        {
            "id": recommendation_id,
            "empty_list": "[]",
            "empty_object": "{}",
            "inventory": "sha256:" + marker * 64,
            "source_inventory": "sha256:" + marker.upper() * 64,
            "catalog": "sha256:" + marker * 64,
            "created_at": created_at,
        },
    )
    fleet_columns = {
        column["name"] for column in inspect(connection).get_columns("fleets")
    }
    if "updated_at" in fleet_columns:
        connection.execute(
            text(
                "INSERT INTO fleets "
                "(id, source_recommendation_id, status, created_at, "
                "updated_at) VALUES (:id, :recommendation_id, 'ACCEPTED', "
                ":created_at, :created_at)"
            ),
            {
                "id": fleet_id,
                "recommendation_id": recommendation_id,
                "created_at": created_at,
            },
        )
    else:
        connection.execute(
            text(
                "INSERT INTO fleets "
                "(id, source_recommendation_id, status, created_at) "
                "VALUES (:id, :recommendation_id, 'ACCEPTED', :created_at)"
            ),
            {
                "id": fleet_id,
                "recommendation_id": recommendation_id,
                "created_at": created_at,
            },
        )
    connection.execute(
        text(
            "INSERT INTO deployments "
            "(id, lineage_id, previous_generation_id, "
            "source_recommendation_id, fleet_id, fleet_candidate_id, "
            "generation, plan, accept_model_download, pull_image, status, "
            "verified_at, created_at) VALUES "
            "(:id, :lineage_id, NULL, NULL, :fleet_id, :candidate_id, 1, "
            ":plan, 0, 0, 'CREATED', NULL, :created_at)"
        ),
        {
            "id": deployment_id,
            "lineage_id": f"lineage-{deployment_id}",
            "fleet_id": fleet_id,
            "candidate_id": "sha256:" + marker * 64,
            "plan": "{}",
            "created_at": created_at,
        },
    )


def _insert_runtime(
    connection,
    *,
    runtime_id: str,
    fleet_id: str,
    deployment_id: str,
    status: str = "ACCEPTED",
    failure_phase: str | None = None,
    failure_code: str | None = None,
) -> None:
    now = datetime(2026, 7, 21, tzinfo=timezone.utc)
    connection.execute(
        text(
            "INSERT INTO fleet_deployment_runtime "
            "(id, fleet_id, deployment_id, status, preparation_id, "
            "current_operation_id, failure_phase, failure_code, "
            "created_at, updated_at) VALUES "
            "(:id, :fleet_id, :deployment_id, :status, NULL, NULL, "
            ":failure_phase, :failure_code, :now, :now)"
        ),
        {
            "id": runtime_id,
            "fleet_id": fleet_id,
            "deployment_id": deployment_id,
            "status": status,
            "failure_phase": failure_phase,
            "failure_code": failure_code,
            "now": now,
        },
    )


class FleetRuntimeMigrationTests(unittest.TestCase):
    def test_0015_schema_matches_models_and_enforces_ownership(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'fleet-runtime.db'}"
            command.upgrade(_config(url), "0015")
            engine = make_engine(url)
            inspector = inspect(engine)

            fleet_columns = {
                item["name"]: item
                for item in inspector.get_columns("fleets")
            }
            self.assertIn("updated_at", fleet_columns)
            self.assertFalse(fleet_columns["updated_at"]["nullable"])
            fleet_status_check = next(
                item["sqltext"]
                for item in inspector.get_check_constraints("fleets")
                if item["name"] == "ck_fleets_status"
            )
            for status in (
                "ACCEPTED",
                "PREPARING",
                "PREPARED",
                "APPLYING",
                "VERIFYING",
                "ACTIVE",
                "PARTIAL_FAILED",
                "FAILED",
            ):
                self.assertIn(status, fleet_status_check)

            runtime_columns = {
                item["name"]: item
                for item in inspector.get_columns(
                    "fleet_deployment_runtime"
                )
            }
            self.assertEqual(
                {
                    "id",
                    "fleet_id",
                    "deployment_id",
                    "status",
                    "preparation_id",
                    "current_operation_id",
                    "failure_phase",
                    "failure_code",
                    "created_at",
                    "updated_at",
                },
                set(runtime_columns),
            )
            self.assertEqual(
                ["id"],
                inspector.get_pk_constraint("fleet_deployment_runtime")[
                    "constrained_columns"
                ],
            )
            self.assertEqual(
                set(runtime_columns),
                set(
                    Base.metadata.tables[
                        "fleet_deployment_runtime"
                    ].columns.keys()
                ),
            )

            check_names = {
                item["name"]
                for item in inspector.get_check_constraints(
                    "fleet_deployment_runtime"
                )
            }
            self.assertEqual(
                {
                    "ck_fleet_deployment_runtime_id_canonical_uuid",
                    "ck_fleet_deployment_runtime_status",
                    "ck_fleet_deployment_runtime_failure",
                },
                check_names,
            )
            unique_columns = {
                tuple(item["column_names"])
                for item in inspector.get_unique_constraints(
                    "fleet_deployment_runtime"
                )
            }
            self.assertEqual(
                {
                    ("fleet_id", "deployment_id"),
                    ("preparation_id",),
                    ("current_operation_id",),
                },
                unique_columns,
            )
            foreign_keys = {
                item["name"]: item
                for item in inspector.get_foreign_keys(
                    "fleet_deployment_runtime"
                )
            }
            self.assertEqual(
                ["fleet_id", "deployment_id"],
                foreign_keys[
                    "fk_fleet_deployment_runtime_fleet_deployment"
                ]["constrained_columns"],
            )
            self.assertEqual(
                ["fleet_id", "id"],
                foreign_keys[
                    "fk_fleet_deployment_runtime_fleet_deployment"
                ]["referred_columns"],
            )
            self.assertEqual(
                {"fleet_id", "preparation_id", "current_operation_id"},
                {
                    "fleet_id"
                    if name == "fk_fleet_deployment_runtime_fleet_id"
                    else (
                        "preparation_id"
                        if name
                        == "fk_fleet_deployment_runtime_preparation_id"
                        else "current_operation_id"
                    )
                    for name in foreign_keys
                    if name
                    in {
                        "fk_fleet_deployment_runtime_fleet_id",
                        "fk_fleet_deployment_runtime_preparation_id",
                        "fk_fleet_deployment_runtime_current_operation_id",
                    }
                },
            )
            self.assertIn(
                "ix_fleet_deployment_runtime_fleet_status",
                {item["name"] for item in inspector.get_indexes(
                    "fleet_deployment_runtime"
                )},
            )

            with engine.begin() as connection:
                _seed_fleet_deployment(
                    connection,
                    marker="1",
                    fleet_id="11111111-1111-1111-1111-111111111111",
                    deployment_id="fleet-runtime-deployment-1",
                )
                _seed_fleet_deployment(
                    connection,
                    marker="2",
                    fleet_id="22222222-2222-2222-2222-222222222222",
                    deployment_id="fleet-runtime-deployment-2",
                )
                _insert_runtime(
                    connection,
                    runtime_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    fleet_id="11111111-1111-1111-1111-111111111111",
                    deployment_id="fleet-runtime-deployment-1",
                )

            with self.assertRaises(IntegrityError), engine.begin() as connection:
                _insert_runtime(
                    connection,
                    runtime_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    fleet_id="11111111-1111-1111-1111-111111111111",
                    deployment_id="fleet-runtime-deployment-2",
                )
            with self.assertRaises(IntegrityError), engine.begin() as connection:
                _insert_runtime(
                    connection,
                    runtime_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
                    fleet_id="22222222-2222-2222-2222-222222222222",
                    deployment_id="fleet-runtime-deployment-2",
                    status="UNKNOWN",
                )
            with self.assertRaises(IntegrityError), engine.begin() as connection:
                _insert_runtime(
                    connection,
                    runtime_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
                    fleet_id="22222222-2222-2222-2222-222222222222",
                    deployment_id="fleet-runtime-deployment-2",
                    status="PREPARE_FAILED",
                )
            with engine.begin() as connection:
                _insert_runtime(
                    connection,
                    runtime_id="eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
                    fleet_id="22222222-2222-2222-2222-222222222222",
                    deployment_id="fleet-runtime-deployment-2",
                    status="PREPARE_FAILED",
                    failure_phase="PREPARE",
                    failure_code="MODEL_LOAD_FAILED",
                )
            with self.assertRaises(IntegrityError), engine.begin() as connection:
                connection.execute(
                    text("UPDATE fleets SET status = 'UNKNOWN'")
                )
            engine.dispose()

    def test_0015_backfills_0014_and_downgrade_is_data_safe(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{Path(temporary) / 'fleet-backfill.db'}"
            migration_config = _config(url)
            command.upgrade(migration_config, "0015")
            command.downgrade(migration_config, "0014")

            engine = make_engine(url)
            inspector = inspect(engine)
            self.assertNotIn(
                "fleet_deployment_runtime", inspector.get_table_names()
            )
            self.assertNotIn(
                "updated_at",
                {
                    item["name"]
                    for item in inspector.get_columns("fleets")
                },
            )
            with engine.begin() as connection:
                _seed_fleet_deployment(
                    connection,
                    marker="3",
                    fleet_id="33333333-3333-3333-3333-333333333333",
                    deployment_id="legacy-fleet-deployment",
                )
            engine.dispose()

            command.upgrade(migration_config, "0015")
            engine = make_engine(url)
            revision = ScriptDirectory.from_config(
                migration_config
            ).get_revision("0015").module
            expected_id = revision._runtime_id(
                "33333333-3333-3333-3333-333333333333",
                "legacy-fleet-deployment",
            )
            with engine.connect() as connection:
                runtime = connection.execute(
                    text(
                        "SELECT id, fleet_id, deployment_id, status, "
                        "preparation_id, current_operation_id, "
                        "failure_phase, failure_code "
                        "FROM fleet_deployment_runtime"
                    )
                ).mappings().one()
            self.assertEqual(expected_id, runtime["id"])
            self.assertEqual("ACCEPTED", runtime["status"])
            self.assertIsNone(runtime["preparation_id"])
            self.assertIsNone(runtime["current_operation_id"])
            self.assertIsNone(runtime["failure_phase"])
            self.assertIsNone(runtime["failure_code"])
            factory = make_session_factory(engine)
            with factory() as session:
                stored = session.get(
                    FleetDeploymentRuntime, expected_id
                )
                self.assertIsNotNone(stored)
                self.assertEqual(
                    "legacy-fleet-deployment", stored.deployment_id
                )
                self.assertIsNotNone(stored.to_dict()["updated_at"])
                fleet = session.get(
                    FleetRecord,
                    "33333333-3333-3333-3333-333333333333",
                )
                self.assertIsNotNone(fleet.updated_at)
                self.assertIsNotNone(fleet.to_dict()["updated_at"])
            engine.dispose()

            with self.assertRaisesRegex(RuntimeError, "downgrade 0015"):
                command.downgrade(migration_config, "0014")

            engine = make_engine(url)
            with engine.begin() as connection:
                connection.execute(
                    text("DELETE FROM fleet_deployment_runtime")
                )
                connection.execute(
                    text("UPDATE fleets SET status = 'PREPARING'")
                )
            engine.dispose()
            with self.assertRaisesRegex(RuntimeError, "downgrade 0015"):
                command.downgrade(migration_config, "0014")

            engine = make_engine(url)
            with engine.begin() as connection:
                connection.execute(
                    text("UPDATE fleets SET status = 'ACCEPTED'")
                )
            engine.dispose()
            command.downgrade(migration_config, "0014")

            engine = make_engine(url)
            inspector = inspect(engine)
            self.assertNotIn(
                "fleet_deployment_runtime", inspector.get_table_names()
            )
            self.assertNotIn(
                "updated_at",
                {
                    item["name"]
                    for item in inspector.get_columns("fleets")
                },
            )
            engine.dispose()

    def test_0015_postgresql_offline_upgrade_is_deterministic(self):
        output = io.StringIO()
        migration_config = _config("postgresql://dure@localhost/dure")

        with redirect_stdout(output):
            command.upgrade(migration_config, "0014:0015", sql=True)

        sql = output.getvalue()
        self.assertIn("ADD COLUMN updated_at", sql)
        self.assertIn("CREATE TABLE fleet_deployment_runtime", sql)
        self.assertIn("ck_fleet_deployment_runtime_status", sql)
        self.assertIn(
            "fk_fleet_deployment_runtime_fleet_deployment", sql
        )
        self.assertIn("md5(f.id || ':' || d.id)", sql)

        revision = ScriptDirectory.from_config(
            migration_config
        ).get_revision("0015").module
        self.assertEqual(
            (
                "LOCK TABLE fleets, fleet_deployment_runtime "
                "IN ACCESS EXCLUSIVE MODE",
            ),
            revision._destructive_downgrade_lock_sql("postgresql"),
        )
        self.assertEqual(
            (), revision._destructive_downgrade_lock_sql("sqlite")
        )


if __name__ == "__main__":
    unittest.main()
