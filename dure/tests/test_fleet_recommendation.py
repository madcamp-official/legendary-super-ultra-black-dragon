from __future__ import annotations

import tempfile
import unittest
import uuid
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from dure.control.api import create_app
from dure.control.db import Base, make_engine, make_session_factory
from dure.control.fleet_recommendation import (
    FleetRecommendationConflictError,
    FleetRecommendationError,
    FleetRecommendationIntegrityError,
    FleetRecommendationNotFoundError,
    evaluate_fleet_recommendation,
    persist_fleet_recommendation,
    recommend_fleet,
    show_fleet_recommendation,
)
from dure.control.models import (
    Deployment,
    DeploymentRecommendationRecord,
    FleetRecommendationRecord,
    Node,
    NodeProfileRecord,
    Task,
    utcnow,
)

from .helpers import profile


class FleetRecommendationServiceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.engine = make_engine(
            f"sqlite:///{Path(self.temporary.name) / 'fleet-recommendation.db'}"
        )
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)

    def tearDown(self):
        self.engine.dispose()
        self.temporary.cleanup()

    def _node(self, session, suffix: str) -> str:
        now = utcnow()
        node = Node(
            install_id=f"fleet-recommendation-{suffix}-{uuid.uuid4()}",
            display_name=f"fleet-{suffix}",
            hostname=f"fleet-{suffix}",
            agent_version="0.3.30",
            approved=True,
            last_seen=now,
        )
        session.add(node)
        session.flush()
        session.add(
            NodeProfileRecord(
                node_id=node.id,
                profile=profile(
                    f"reported-{suffix}",
                    address=f"10.40.0.{int(suffix) + 10}",
                ).to_dict(),
                updated_at=now,
            )
        )
        session.commit()
        return node.id

    def test_recommendation_is_content_addressed_idempotent_and_read_only(self):
        with self.factory() as session:
            node_ids = [self._node(session, "1"), self._node(session, "2")]

            first = recommend_fleet(
                session,
                node_ids=list(reversed(node_ids)),
                all_online=False,
                minimum_replicas={"qwen2.5-72b-awq": 1},
            )
            second = recommend_fleet(
                session,
                node_ids=node_ids,
                all_online=False,
                minimum_replicas={"qwen2.5-72b-awq": 1},
            )

            self.assertTrue(first["created"])
            self.assertFalse(second["created"])
            self.assertEqual(
                first["recommendation"], second["recommendation"]
            )
            recommendation = first["recommendation"]
            self.assertEqual(recommendation["requested_node_ids"], sorted(node_ids))
            self.assertEqual(
                recommendation["evaluation"]["schedule"][
                    "unmet_minimum_replicas"
                ],
                {"qwen2.5-72b-awq": 1},
            )
            self.assertEqual(
                {item["reason"] for item in recommendation["evaluation"]["unassigned_nodes"]},
                {"NO_VALIDATED_CANDIDATE"},
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(FleetRecommendationRecord)
                ),
                1,
            )
            for model in (Task, Deployment, DeploymentRecommendationRecord):
                self.assertEqual(
                    session.scalar(select(func.count()).select_from(model)),
                    0,
                )

            shown = show_fleet_recommendation(
                session, recommendation["id"]
            )
            self.assertEqual(shown["recommendation"], recommendation)
            self.assertEqual(shown["recorded_at"], first["recorded_at"])

    def test_all_online_freezes_the_observed_node_set(self):
        with self.factory() as session:
            node_ids = [self._node(session, "1"), self._node(session, "2")]
            snapshot = evaluate_fleet_recommendation(
                session,
                node_ids=[],
                all_online=True,
            )

            self.assertEqual(snapshot["selection_mode"], "all_online")
            self.assertEqual(snapshot["requested_node_ids"], sorted(node_ids))
            self.assertNotIn("recorded_at", snapshot)

    def test_policy_validation_and_snapshot_conflict_fail_closed(self):
        with self.factory() as session:
            node_id = self._node(session, "1")
            invalid_calls = (
                {
                    "node_ids": [node_id, node_id],
                    "all_online": False,
                },
                {
                    "node_ids": [node_id],
                    "all_online": False,
                    "minimum_replicas": {"outside-model": 1},
                },
                {
                    "node_ids": [node_id],
                    "all_online": False,
                    "reserve_node_ids": [str(uuid.uuid4())],
                },
            )
            for kwargs in invalid_calls:
                with self.subTest(kwargs=kwargs):
                    with self.assertRaises(FleetRecommendationError):
                        evaluate_fleet_recommendation(session, **kwargs)

            snapshot = evaluate_fleet_recommendation(
                session,
                node_ids=[node_id],
                all_online=False,
            )
            record, created = persist_fleet_recommendation(session, snapshot)
            self.assertTrue(created)
            record.scheduler_version = "tampered"
            session.commit()
            with self.assertRaises(FleetRecommendationConflictError):
                persist_fleet_recommendation(session, snapshot)

    def test_show_rejects_missing_and_tampered_snapshots(self):
        with self.factory() as session:
            with self.assertRaises(FleetRecommendationNotFoundError):
                show_fleet_recommendation(session, "sha256:" + "0" * 64)

            node_id = self._node(session, "1")
            response = recommend_fleet(
                session,
                node_ids=[node_id],
                all_online=False,
            )
            record = session.get(
                FleetRecommendationRecord,
                response["recommendation"]["id"],
            )
            changed = dict(record.recommendation_snapshot)
            changed["objective"] = "tampered"
            record.recommendation_snapshot = changed
            session.commit()

            with self.assertRaises(FleetRecommendationIntegrityError):
                show_fleet_recommendation(session, record.id)


class FleetRecommendationAPITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        database_url = (
            f"sqlite:///{Path(self.temporary.name) / 'fleet-api.db'}"
        )
        self.client = TestClient(
            create_app(
                database_url=database_url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.factory = self.client.app.state.session_factory
        self.admin = {"Authorization": "Bearer admin-secret"}
        with self.factory() as session:
            now = utcnow()
            node = Node(
                install_id=f"fleet-api-{uuid.uuid4()}",
                display_name="fleet-api",
                hostname="fleet-api",
                agent_version="0.3.30",
                approved=True,
                last_seen=now,
            )
            session.add(node)
            session.flush()
            session.add(
                NodeProfileRecord(
                    node_id=node.id,
                    profile=profile(
                        "fleet-api-reported",
                        address="10.41.0.10",
                    ).to_dict(),
                    updated_at=now,
                )
            )
            session.commit()
            self.node_id = node.id

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def test_admin_api_persists_and_reads_an_immutable_recommendation(self):
        payload = {
            "node_ids": [self.node_id],
            "all_online": False,
            "objective": "quality-first",
            "minimum_replicas": {"qwen2.5-7b-awq": 1},
            "minimum_reserve_nodes": 0,
            "reserve_node_ids": [],
        }
        unauthorized = self.client.post(
            "/v1/admin/fleet-recommendations", json=payload
        )
        self.assertEqual(unauthorized.status_code, 401)

        created = self.client.post(
            "/v1/admin/fleet-recommendations",
            headers=self.admin,
            json=payload,
        )
        self.assertEqual(created.status_code, 200, created.text)
        self.assertTrue(created.json()["created"])
        recommendation_id = created.json()["recommendation"]["id"]

        repeated = self.client.post(
            "/v1/admin/fleet-recommendations",
            headers=self.admin,
            json=payload,
        )
        self.assertEqual(repeated.status_code, 200, repeated.text)
        self.assertFalse(repeated.json()["created"])
        self.assertEqual(
            repeated.json()["recommendation"]["id"], recommendation_id
        )

        shown = self.client.get(
            f"/v1/admin/fleet-recommendations/{recommendation_id}",
            headers=self.admin,
        )
        self.assertEqual(shown.status_code, 200, shown.text)
        self.assertEqual(shown.json()["recommendation"]["id"], recommendation_id)

    def test_api_schema_and_not_found_paths_are_strict(self):
        invalid_payloads = (
            {
                "node_ids": [self.node_id],
                "all_online": False,
                "unknown": True,
            },
            {
                "node_ids": [self.node_id],
                "all_online": False,
                "minimum_replicas": {"unknown-model": 1},
            },
            {
                "node_ids": [],
                "all_online": False,
            },
        )
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                response = self.client.post(
                    "/v1/admin/fleet-recommendations",
                    headers=self.admin,
                    json=payload,
                )
                self.assertEqual(response.status_code, 422, response.text)

        missing = self.client.get(
            "/v1/admin/fleet-recommendations/sha256:" + "0" * 64,
            headers=self.admin,
        )
        self.assertEqual(missing.status_code, 404, missing.text)
        self.assertEqual(
            missing.json()["detail"]["code"],
            "FLEET_RECOMMENDATION_NOT_FOUND",
        )

        unknown_node = self.client.post(
            "/v1/admin/fleet-recommendations",
            headers=self.admin,
            json={
                "node_ids": [str(uuid.uuid4())],
                "all_online": False,
            },
        )
        self.assertEqual(unknown_node.status_code, 404, unknown_node.text)
        self.assertEqual(
            unknown_node.json()["detail"]["code"],
            "RECOMMENDATION_NODE_NOT_FOUND",
        )


if __name__ == "__main__":
    unittest.main()
