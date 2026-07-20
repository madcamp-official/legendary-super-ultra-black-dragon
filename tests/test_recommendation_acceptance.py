from __future__ import annotations

import copy
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from dure.control.api import create_app
from dure.control.models import (
    AuditEvent,
    BenchmarkRun,
    Deployment,
    DeploymentOperation,
    DeploymentRecommendationRecord,
    Node,
    NodeProfileRecord,
    Task,
    utcnow,
)
from dure.control.recommendation import (
    RecommendationNotAcceptableError,
    _lock_recommendation_inputs,
    _ray_head_ip,
)

from .helpers import profile
from .test_recommendation import _add_node, _add_release


class RecommendationAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.temporary.name) / 'recommendation-accept.db'}"
        self.client = TestClient(
            create_app(
                database_url=url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.factory = self.client.app.state.session_factory
        self.admin = {"Authorization": "Bearer admin-secret"}

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def _seed_active_candidate(
        self,
        key: str = "accept",
        *,
        quality_rank: int = 10,
    ) -> tuple[str, str, str]:
        with self.factory() as session:
            node = _add_node(session, f"node-{key}", now=utcnow())
            release, placement = _add_release(
                session,
                key,
                quality_rank=quality_rank,
                evidence_nodes=[node],
            )
            return node.id, release.id, placement.id

    def _recommend(self, node_id: str) -> dict:
        response = self.client.post(
            "/v1/admin/deployment-recommendations",
            headers=self.admin,
            json={
                "node_ids": [node_id],
                "all_online": False,
                "objective": "quality-first",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["recommendation"]

    def _accept(
        self,
        recommendation_id: str,
        *,
        previous_generation_id: str | None = None,
    ):
        body = {}
        if previous_generation_id is not None:
            body["previous_generation_id"] = previous_generation_id
        return self.client.post(
            f"/v1/admin/deployment-recommendations/{recommendation_id}/accept",
            headers=self.admin,
            json=body,
        )

    def _change_profile(self, node_id: str, disk_free_mib: int) -> None:
        with self.factory() as session:
            record = session.get(NodeProfileRecord, node_id)
            changed = copy.deepcopy(record.profile)
            changed["disk_free_mib"] = disk_free_mib
            record.profile = changed
            record.updated_at = utcnow()
            session.commit()

    def assert_error_code(self, response, status_code: int, code: str) -> None:
        self.assertEqual(response.status_code, status_code, response.text)
        self.assertEqual(response.json()["detail"]["code"], code)

    def test_repeated_recommendation_persists_one_snapshot_without_execution_rows(self):
        node_id, _, _ = self._seed_active_candidate("repeat")

        first = self._recommend(node_id)
        second = self._recommend(node_id)

        self.assertEqual(first, second)
        with self.factory() as session:
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(DeploymentRecommendationRecord)
                ),
                1,
            )
            for model in (Deployment, Task, BenchmarkRun):
                self.assertEqual(
                    session.scalar(select(func.count()).select_from(model)),
                    0,
                    model.__tablename__,
                )

    def test_show_requires_authentication_and_returns_404_for_unknown_snapshot(self):
        node_id, _, _ = self._seed_active_candidate("show")
        recommendation = self._recommend(node_id)
        path = f"/v1/admin/deployment-recommendations/{recommendation['id']}"

        self.assertEqual(self.client.get(path).status_code, 401)
        self.assertEqual(
            self.client.post(path + "/accept", json={}).status_code,
            401,
        )
        shown = self.client.get(path, headers=self.admin)
        self.assertEqual(shown.status_code, 200, shown.text)
        self.assertEqual(shown.json()["recommendation"], recommendation)
        self.assertIsNone(shown.json()["deployment"])

        missing_id = "sha256:" + "f" * 64
        missing_path = f"/v1/admin/deployment-recommendations/{missing_id}"
        self.assert_error_code(
            self.client.get(missing_path, headers=self.admin),
            404,
            "RECOMMENDATION_NOT_FOUND",
        )
        self.assert_error_code(
            self.client.post(missing_path + "/accept", headers=self.admin, json={}),
            404,
            "RECOMMENDATION_NOT_FOUND",
        )

    def test_accept_creates_immutable_plan_with_safe_flags_and_no_execution_rows(self):
        node_id, release_id, placement_id = self._seed_active_candidate("success")
        recommendation = self._recommend(node_id)

        response = self._accept(recommendation["id"])

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertTrue(body["created"])
        deployment = body["deployment"]
        self.assertEqual(deployment["source_recommendation_id"], recommendation["id"])
        self.assertEqual(deployment["lineage_id"], deployment["id"])
        self.assertIsNone(deployment["previous_generation_id"])
        self.assertEqual(deployment["generation"], 1)
        self.assertEqual(deployment["status"], "CREATED")
        self.assertFalse(deployment["accept_model_download"])
        self.assertFalse(deployment["pull_image"])
        plan = deployment["plan"]
        self.assertEqual(plan["deployment_id"], deployment["id"])
        self.assertEqual(plan["generation"], 1)
        self.assertEqual(plan["image"], recommendation["selected"]["runtime_image"])
        self.assertEqual(
            plan["model_revision"],
            recommendation["selected"]["artifact_revision"],
        )
        self.assertEqual(
            plan["model_path"],
            "/var/lib/dure/models/"
            f"{recommendation['selected']['model_id']}--"
            f"{recommendation['selected']['artifact_revision']}",
        )
        self.assertEqual(
            [item["node_id"] for item in plan["assignments"]],
            recommendation["selected"]["node_ids"],
        )
        self.assertEqual(recommendation["selected"]["model_release_id"], release_id)
        self.assertEqual(recommendation["selected"]["placement_id"], placement_id)
        shown = self.client.get(
            f"/v1/admin/deployment-recommendations/{recommendation['id']}",
            headers=self.admin,
        )
        self.assertEqual(shown.status_code, 200, shown.text)
        self.assertEqual(shown.json()["deployment"], deployment)

        frozen_plan = copy.deepcopy(plan)
        self._change_profile(node_id, 79000)
        with self.factory() as session:
            stored = session.get(Deployment, deployment["id"])
            self.assertEqual(stored.plan, frozen_plan)
            self.assertFalse(stored.accept_model_download)
            self.assertFalse(stored.pull_image)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)),
                0,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(BenchmarkRun)),
                0,
            )

    def test_postgresql_accept_locks_registry_and_inventory_tables(self):
        session = Mock()
        session.get_bind.return_value.dialect.name = "postgresql"
        session.scalars.return_value = []
        record = Mock(selection_mode="all_online", requested_node_ids=[])

        _lock_recommendation_inputs(session, record)

        statement = str(session.execute.call_args.args[0])
        self.assertEqual(
            statement,
            "LOCK TABLE model_artifacts, model_releases, nodes, node_profiles, "
            "placement_profiles, runtime_releases IN SHARE MODE",
        )
        session.scalars.assert_not_called()

    def test_multinode_ray_head_rejects_public_only_address(self):
        public_only = profile("public-only", address="203.0.113.10")

        with self.assertRaises(RecommendationNotAcceptableError) as raised:
            _ray_head_ip(public_only, multi_node=True)

        self.assertEqual(raised.exception.code, "GENERATION_NETWORK_UNSUPPORTED")
        self.assertEqual(_ray_head_ip(public_only, multi_node=False), "127.0.0.1")

    def test_accept_rejects_changed_profile_content(self):
        node_id, _, _ = self._seed_active_candidate("changed-profile")
        recommendation = self._recommend(node_id)
        self._change_profile(node_id, 79000)

        response = self._accept(recommendation["id"])

        self.assert_error_code(response, 409, "RECOMMENDATION_STALE")

    def test_accept_rejects_stale_profile(self):
        node_id, _, _ = self._seed_active_candidate("stale-profile")
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            record = session.get(NodeProfileRecord, node_id)
            record.updated_at = utcnow() - timedelta(seconds=91)
            session.commit()

        response = self._accept(recommendation["id"])

        self.assert_error_code(response, 409, "RECOMMENDATION_STALE")

    def test_accept_rejects_node_approval_change(self):
        node_id, _, _ = self._seed_active_candidate("approval")
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            node = session.get(Node, node_id)
            node.approved = False
            session.commit()

        response = self._accept(recommendation["id"])

        self.assert_error_code(response, 409, "RECOMMENDATION_STALE")

    def test_all_online_accept_rejects_newly_available_node(self):
        self._seed_active_candidate("all-online")
        response = self.client.post(
            "/v1/admin/deployment-recommendations",
            headers=self.admin,
            json={"all_online": True, "objective": "quality-first"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        recommendation = response.json()["recommendation"]

        with self.factory() as session:
            _add_node(session, "node-added-later", now=utcnow())

        stale = self._accept(recommendation["id"])
        self.assert_error_code(stale, 409, "RECOMMENDATION_STALE")

    def test_accept_rejects_catalog_change_even_when_selected_candidate_is_same(self):
        node_id, selected_release_id, _ = self._seed_active_candidate(
            "catalog-selected", quality_rank=20
        )
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            node = session.get(Node, node_id)
            _add_release(
                session,
                "catalog-lower",
                quality_rank=1,
                evidence_nodes=[node],
            )

        current = self._recommend(node_id)
        self.assertEqual(
            current["selected"]["model_release_id"],
            selected_release_id,
        )
        self.assertNotEqual(current["catalog_version"], recommendation["catalog_version"])
        response = self._accept(recommendation["id"])
        self.assert_error_code(response, 409, "RECOMMENDATION_STALE")

    def test_accept_rejects_changed_or_missing_selected_candidate(self):
        node_id, selected_release_id, _ = self._seed_active_candidate(
            "selection", quality_rank=10
        )
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            node = session.get(Node, node_id)
            higher, _ = _add_release(
                session,
                "selection-higher",
                quality_rank=100,
                evidence_nodes=[node],
            )

        current = self._recommend(node_id)
        self.assertEqual(current["selected"]["model_release_id"], higher.id)
        self.assertNotEqual(higher.id, selected_release_id)
        self.assert_error_code(
            self._accept(recommendation["id"]),
            409,
            "RECOMMENDATION_STALE",
        )

        with self.factory() as session:
            empty_node = _add_node(
                session,
                "node-empty",
                now=utcnow(),
                stored_profile=None,
            )
        empty = self._recommend(empty_node.id)
        self.assertIsNone(empty["selected"])
        self.assert_error_code(
            self._accept(empty["id"]),
            409,
            "RECOMMENDATION_NOT_FEASIBLE",
        )

    def test_same_accept_is_idempotent_and_writes_one_audit_event(self):
        node_id, _, _ = self._seed_active_candidate("idempotent")
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            audit_before = session.scalar(select(func.count()).select_from(AuditEvent))
            accept_audit_before = session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "recommendation.accept")
            )

        first = self._accept(recommendation["id"])
        second = self._accept(recommendation["id"])

        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 200, second.text)
        self.assertTrue(first.json()["created"])
        self.assertFalse(second.json()["created"])
        self.assertEqual(
            first.json()["deployment"]["id"],
            second.json()["deployment"]["id"],
        )
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                1,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(AuditEvent)),
                audit_before + 1,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "recommendation.accept")
                ),
                accept_audit_before + 1,
            )

    def test_previous_generation_builds_linear_chain_and_rejects_conflicts(self):
        node_id, _, _ = self._seed_active_candidate("chain")
        first_recommendation = self._recommend(node_id)
        first = self._accept(first_recommendation["id"])
        self.assertEqual(first.status_code, 200, first.text)
        first_deployment = first.json()["deployment"]

        self._change_profile(node_id, 79000)
        second_recommendation = self._recommend(node_id)
        second = self._accept(
            second_recommendation["id"],
            previous_generation_id=first_deployment["id"],
        )
        self.assertEqual(second.status_code, 200, second.text)
        second_deployment = second.json()["deployment"]
        self.assertEqual(second_deployment["lineage_id"], first_deployment["lineage_id"])
        self.assertEqual(
            second_deployment["previous_generation_id"],
            first_deployment["id"],
        )
        self.assertEqual(second_deployment["generation"], 2)

        different_previous = self._accept(second_recommendation["id"])
        self.assert_error_code(
            different_previous,
            409,
            "RECOMMENDATION_ALREADY_ACCEPTED",
        )

        self._change_profile(node_id, 78000)
        third_recommendation = self._recommend(node_id)
        stale_previous = self._accept(
            third_recommendation["id"],
            previous_generation_id=first_deployment["id"],
        )
        self.assert_error_code(
            stale_previous,
            409,
            "PREVIOUS_GENERATION_NOT_LATEST",
        )
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                2,
            )

    def test_rolled_back_latest_generation_cannot_continue_the_old_lineage(self):
        node_id, _, _ = self._seed_active_candidate("rolled-back-lineage")
        first_recommendation = self._recommend(node_id)
        first = self._accept(first_recommendation["id"])
        self.assertEqual(first.status_code, 200, first.text)
        first_deployment = first.json()["deployment"]
        with self.factory() as session:
            deployment = session.get(Deployment, first_deployment["id"])
            deployment.status = "ROLLED_BACK"
            deployment.verified_at = None
            session.commit()

        self._change_profile(node_id, 79000)
        second_recommendation = self._recommend(node_id)
        response = self._accept(
            second_recommendation["id"],
            previous_generation_id=first_deployment["id"],
        )

        self.assert_error_code(
            response,
            409,
            "PREVIOUS_GENERATION_ROLLED_BACK",
        )

    def test_legacy_lineage_mutation_blocks_accepting_the_next_generation(self):
        node_id, _, _ = self._seed_active_candidate("legacy-mutation-lineage")
        first_recommendation = self._recommend(node_id)
        first = self._accept(first_recommendation["id"])
        self.assertEqual(first.status_code, 200, first.text)
        first_deployment = first.json()["deployment"]
        with self.factory() as session:
            session.add(
                Task(
                    bulk_id="legacy-lineage-mutation",
                    node_id=first_deployment["plan"]["assignments"][0]["node_id"],
                    type="START_DEPLOYMENT",
                    deployment_id=first_deployment["id"],
                    payload={},
                )
            )
            session.commit()

        self._change_profile(node_id, 79000)
        second_recommendation = self._recommend(node_id)
        response = self._accept(
            second_recommendation["id"],
            previous_generation_id=first_deployment["id"],
        )

        self.assert_error_code(response, 409, "DEPLOYMENT_MUTATION_ACTIVE")

    def test_accept_rejects_a_new_generation_while_lineage_operation_is_active(self):
        node_id, _, _ = self._seed_active_candidate("active-operation")
        first_recommendation = self._recommend(node_id)
        first = self._accept(first_recommendation["id"])
        self.assertEqual(first.status_code, 200, first.text)
        first_deployment = first.json()["deployment"]

        self._change_profile(node_id, 79000)
        second_recommendation = self._recommend(node_id)
        with self.factory() as session:
            session.add(
                DeploymentOperation(
                    request_digest="sha256:" + "a" * 64,
                    lineage_id=first_deployment["lineage_id"],
                    deployment_id=first_deployment["id"],
                    kind="VERIFY",
                    status="RUNNING",
                    phase="VERIFY",
                    node_ids=[node_id],
                    serve=False,
                    api=False,
                    active_lineage_id=first_deployment["lineage_id"],
                )
            )
            session.commit()

        response = self._accept(
            second_recommendation["id"],
            previous_generation_id=first_deployment["id"],
        )

        self.assert_error_code(response, 409, "DEPLOYMENT_OPERATION_ACTIVE")
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                1,
            )

    def test_accept_body_forbids_extra_fields_and_wrong_types(self):
        node_id, _, _ = self._seed_active_candidate("strict")
        recommendation = self._recommend(node_id)
        path = (
            f"/v1/admin/deployment-recommendations/{recommendation['id']}/accept"
        )

        for body in (
            {"apply": True},
            {"previous_generation_id": None, "pull_image": True},
            {"previous_generation_id": 123},
            {"previous_generation_id": ""},
            {"previous_generation_id": "x" * 256},
        ):
            with self.subTest(body=body):
                response = self.client.post(path, headers=self.admin, json=body)
                self.assertEqual(response.status_code, 422, response.text)


if __name__ == "__main__":
    unittest.main()
