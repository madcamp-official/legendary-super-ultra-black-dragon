from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import func, select

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover
    TestClient = None

from dure.control.api import create_app
from dure.control.models import (
    ArtifactManifest,
    ArtifactPreparation,
    AuditEvent,
    BenchmarkRun,
    Deployment,
    DeploymentRecommendationRecord,
    StageArtifactRank,
    StageArtifactValidationEvidence,
    StageArtifactValidationRank,
    StageArtifactVariant,
    Task,
)
from dure.control.service import (
    canonical_artifact_manifest_digest,
    create_model_artifact,
    create_runtime_release,
    register_artifact_manifest,
)
from dure.control.stage_artifacts import (
    StageArtifactConflictError,
    StageArtifactNotFoundError,
    validated_stage_artifact_projection,
)
from dure.stage_artifact import StageExportContract, TrustedStageBuilder

from .test_stage_builder import SyntheticNativeExporter, _write_source


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _source_manifest() -> dict:
    return {
        "schema_version": 1,
        "files": [
            {
                "path": "model.safetensors",
                "kind": "REGULAR",
                "size_bytes": 8,
                "sha256": _digest("a"),
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": 8,
                        "sha256": _digest("a"),
                    }
                ],
            },
            {
                "path": "config.json",
                "kind": "REGULAR",
                "size_bytes": 2,
                "sha256": _digest("b"),
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": 2,
                        "sha256": _digest("b"),
                    }
                ],
            },
        ],
    }


def _stage_manifest(rank: int, *, changed: bool = False) -> dict:
    characters = (
        ("c", "d", "e", "f", "0")
        if changed
        else (
            ("0", "1", "2", "3", "4")
            if rank == 0
            else ("5", "6", "7", "8", "9")
        )
    )
    files = [
        ("model-rank-0-part-0.safetensors", 4 + rank, characters[0]),
        ("config.json", 2, characters[1]),
        ("tokenizer.json", 3, characters[2]),
        ("tokenizer_config.json", 4, characters[3]),
        ("dure-stage.json", 5, characters[4]),
    ]
    return {
        "schema_version": 1,
        "files": [
            {
                "path": path,
                "kind": "REGULAR",
                "size_bytes": size_bytes,
                "sha256": _digest(character),
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": size_bytes,
                        "sha256": _digest(character),
                    }
                ],
            }
            for path, size_bytes, character in files
        ],
    }


@unittest.skipIf(TestClient is None, "FastAPI test client is unavailable")
class StageArtifactRegistryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        database_url = (
            f"sqlite:///{Path(self.temporary.name) / 'stage-artifact-registry.db'}"
        )
        self.client = TestClient(
            create_app(
                database_url=database_url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.admin = {"Authorization": "Bearer admin-secret"}
        self.factory = self.client.app.state.session_factory
        source_manifest = _source_manifest()
        with self.factory() as session:
            artifact = create_model_artifact(
                session,
                model_id="qwen2.5-stage-source",
                repository=f"Example/Stage-Source-{uuid.uuid4()}",
                revision="1" * 40,
                manifest_digest=canonical_artifact_manifest_digest(source_manifest),
                quantization="awq",
                size_mib=1,
                default_max_model_len=1024,
                layer_count=2,
                license_id="apache-2.0",
            )
            register_artifact_manifest(
                session,
                artifact_id=artifact.id,
                manifest=source_manifest,
            )
            runtime = create_runtime_release(
                session,
                version=f"stage-runtime-{uuid.uuid4()}",
                image="registry.example/vllm@sha256:" + "2" * 64,
                vllm_version="0.9.0",
                cuda_version="12.4",
                gpu_architectures=["ampere"],
            )
            self.source_manifest_digest = artifact.manifest_digest
            self.runtime_image = runtime.image

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def _variant_body(self, *, changed_rank: int | None = None) -> dict:
        stages = []
        for rank in range(2):
            manifest = _stage_manifest(rank, changed=rank == changed_rank)
            stages.append(
                {
                    "pipeline_rank": rank,
                    "tensor_rank": 0,
                    "manifest_digest": canonical_artifact_manifest_digest(manifest),
                    "tensor_key_count": 2 + rank,
                    "tensor_keys_digest": _digest(str(4 + rank)),
                    "weight_size_bytes": 4 + rank,
                    "manifest": manifest,
                }
            )
        return {
            "source_manifest_digest": self.source_manifest_digest,
            "runtime_image": self.runtime_image,
            "vllm_version": "0.9.0",
            "exporter_build_digest": _digest("6"),
            "architecture": "Qwen2ForCausalLM",
            "quantization": "awq",
            "tensor_parallel_size": 1,
            "pipeline_parallel_size": 2,
            "loader_format": "VLLM_SHARDED_STATE_V1",
            "stages": stages,
        }

    def _create_variant(self) -> dict:
        response = self.client.post(
            "/v1/admin/stage-artifact-variants",
            headers=self.admin,
            json=self._variant_body(),
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["variant"]

    def _create_validated_variant(self) -> dict:
        variant = self._create_variant()
        endpoint = (
            "/v1/admin/stage-artifact-variants/"
            f"{variant['artifact_set_digest']}"
        )
        evidence = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=self._evidence_body(variant),
        )
        self.assertEqual(evidence.status_code, 200, evidence.text)
        transition = self.client.post(
            endpoint + "/transition",
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(transition.status_code, 200, transition.text)
        return transition.json()["variant"]

    def _evidence_body(
        self,
        variant: dict,
        *,
        kind: str = "GPU_EXPORT_LOAD",
        status: str = "PASSED",
        validator_version: str = "validator-1",
        validation_run_id: str | None = None,
        failure_code: str | None = None,
        ranks: list[dict] | None = None,
    ) -> dict:
        if ranks is None:
            ranks = [
                {
                    "pipeline_rank": item["pipeline_rank"],
                    "tensor_rank": item["tensor_rank"],
                    "manifest_digest": item["manifest_digest"],
                    "tensor_keys_digest": item["tensor_keys_digest"],
                    "loaded_tensor_count": item["tensor_key_count"],
                    "loaded_weight_size_bytes": item["weight_size_bytes"],
                }
                for item in variant["stages"]
            ]
        return {
            "schema_version": 1,
            "variant_identity_digest": variant["artifact_set_digest"],
            "validation_run_id": validation_run_id or str(uuid.uuid4()),
            "kind": kind,
            "status": status,
            "validator_version": validator_version,
            "validator_build_digest": _digest("7"),
            "failure_code": failure_code,
            "ranks": ranks,
        }

    def _registry_counts(self) -> dict[str, int]:
        with self.factory() as session:
            return {
                model.__tablename__: session.scalar(
                    select(func.count()).select_from(model)
                )
                for model in (
                    StageArtifactVariant,
                    StageArtifactRank,
                    StageArtifactValidationEvidence,
                    StageArtifactValidationRank,
                    ArtifactManifest,
                    AuditEvent,
                    DeploymentRecommendationRecord,
                    Deployment,
                    ArtifactPreparation,
                    BenchmarkRun,
                    Task,
                )
            }

    def test_register_is_order_independent_idempotent_and_has_no_runtime_side_effects(self):
        before = self._registry_counts()
        body = self._variant_body()

        first = self.client.post(
            "/v1/admin/stage-artifact-variants",
            headers=self.admin,
            json=body,
        )

        self.assertEqual(first.status_code, 200, first.text)
        first_value = first.json()
        self.assertTrue(first_value["created"])
        variant = first_value["variant"]
        self.assertEqual(variant["status"], "DRAFT")
        self.assertEqual([item["rank"] for item in variant["stages"]], [0, 1])
        self.assertRegex(variant["contract_identity_digest"], r"^sha256:[0-9a-f]{64}$")
        self.assertRegex(variant["artifact_set_digest"], r"^sha256:[0-9a-f]{64}$")

        reordered = copy.deepcopy(body)
        reordered["stages"].reverse()
        second = self.client.post(
            "/v1/admin/stage-artifact-variants",
            headers=self.admin,
            json=reordered,
        )
        self.assertEqual(second.status_code, 200, second.text)
        self.assertFalse(second.json()["created"])
        self.assertEqual(
            second.json()["variant"]["artifact_set_digest"],
            variant["artifact_set_digest"],
        )

        after = self._registry_counts()
        self.assertEqual(after["stage_artifact_variants"], 1)
        self.assertEqual(after["stage_artifact_ranks"], 2)
        self.assertEqual(after["artifact_manifests"], before["artifact_manifests"] + 2)
        for unchanged in (
            "deployment_recommendations",
            "deployments",
            "artifact_preparations",
            "benchmark_runs",
            "tasks",
        ):
            self.assertEqual(after[unchanged], before[unchanged], unchanged)
        with self.factory() as session:
            detached = list(
                session.scalars(
                    select(ArtifactManifest).where(
                        ArtifactManifest.model_artifact_id.is_(None)
                    )
                )
            )
            self.assertEqual(len(detached), 2)

    def test_validated_projection_is_closed_immutable_and_includes_rank_sizes(self):
        variant = self._create_validated_variant()

        with self.factory() as session:
            projection = validated_stage_artifact_projection(
                session, variant["artifact_set_digest"]
            )
            replay = validated_stage_artifact_projection(
                session, variant["artifact_set_digest"]
            )

        self.assertEqual(projection, replay)
        self.assertEqual(
            set(projection),
            {
                "artifact_set_digest",
                "contract_identity_digest",
                "source_manifest_digest",
                "runtime_image",
                "vllm_version",
                "exporter_build_digest",
                "architecture",
                "quantization",
                "tensor_parallel_size",
                "pipeline_parallel_size",
                "loader_format",
                "ranks",
            },
        )
        self.assertEqual(
            {
                key: projection[key]
                for key in (
                    "artifact_set_digest",
                    "contract_identity_digest",
                    "source_manifest_digest",
                    "runtime_image",
                    "vllm_version",
                    "exporter_build_digest",
                    "architecture",
                    "quantization",
                    "tensor_parallel_size",
                    "pipeline_parallel_size",
                    "loader_format",
                )
            },
            {
                key: variant[key]
                for key in (
                    "artifact_set_digest",
                    "contract_identity_digest",
                    "source_manifest_digest",
                    "runtime_image",
                    "vllm_version",
                    "exporter_build_digest",
                    "architecture",
                    "quantization",
                    "tensor_parallel_size",
                    "pipeline_parallel_size",
                    "loader_format",
                )
            },
        )
        self.assertEqual([item["rank"] for item in projection["ranks"]], [0, 1])
        self.assertEqual(
            [item["total_size_bytes"] for item in projection["ranks"]],
            [18, 19],
        )
        self.assertEqual(
            [item["file_count"] for item in projection["ranks"]],
            [5, 5],
        )
        for projected, registered in zip(
            projection["ranks"], variant["stages"], strict=True
        ):
            self.assertEqual(
                set(projected),
                {
                    "rank",
                    "pipeline_rank",
                    "tensor_rank",
                    "manifest_digest",
                    "tensor_key_count",
                    "tensor_keys_digest",
                    "weight_size_bytes",
                    "total_size_bytes",
                    "file_count",
                },
            )
            for field in (
                "rank",
                "pipeline_rank",
                "tensor_rank",
                "manifest_digest",
                "tensor_key_count",
                "tensor_keys_digest",
                "weight_size_bytes",
            ):
                self.assertEqual(projected[field], registered[field])

    def test_projection_requires_exact_current_validated_variant(self):
        with self.factory() as session:
            with self.assertRaises(StageArtifactNotFoundError):
                validated_stage_artifact_projection(session, _digest("f"))
            with self.assertRaises(ValueError):
                validated_stage_artifact_projection(session, "latest")

        draft = self._create_variant()
        with self.factory() as session:
            with self.assertRaisesRegex(
                StageArtifactConflictError, "currently VALIDATED"
            ):
                validated_stage_artifact_projection(
                    session, draft["artifact_set_digest"]
                )

        endpoint = (
            "/v1/admin/stage-artifact-variants/"
            f"{draft['artifact_set_digest']}"
        )
        evidence = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=self._evidence_body(draft),
        )
        self.assertEqual(evidence.status_code, 200, evidence.text)
        validated = self.client.post(
            endpoint + "/transition",
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(validated.status_code, 200, validated.text)

        with self.factory() as session:
            record = session.get(StageArtifactVariant, draft["artifact_set_digest"])
            self.assertIsNotNone(record)
            record.validated_at = None
            with session.no_autoflush:
                # The mutation gate refreshes the locked row instead of
                # trusting potentially stale identity-map state.
                validated_stage_artifact_projection(
                    session, draft["artifact_set_digest"]
                )
            self.assertIsNotNone(record.validated_at)
            session.rollback()

        with self.factory() as session:
            latest = session.scalar(
                select(StageArtifactValidationEvidence)
                .where(
                    StageArtifactValidationEvidence.variant_id
                    == draft["artifact_set_digest"],
                    StageArtifactValidationEvidence.kind == "GPU_EXPORT_LOAD",
                )
                .order_by(
                    StageArtifactValidationEvidence.registration_sequence.desc()
                )
            )
            self.assertIsNotNone(latest)
            latest.status = "FAILED"
            with session.no_autoflush:
                with self.assertRaisesRegex(
                    StageArtifactConflictError, "must be PASSED"
                ):
                    validated_stage_artifact_projection(
                        session, draft["artifact_set_digest"]
                    )
            session.rollback()

        revoked = self.client.post(
            endpoint + "/transition",
            headers=self.admin,
            json={"status": "REVOKED"},
        )
        self.assertEqual(revoked.status_code, 200, revoked.text)
        with self.factory() as session:
            with self.assertRaisesRegex(
                StageArtifactConflictError, "currently VALIDATED"
            ):
                validated_stage_artifact_projection(
                    session, draft["artifact_set_digest"]
                )

    def test_projection_rejects_incomplete_evidence_and_corrupt_manifest(self):
        variant = self._create_validated_variant()

        with self.factory() as session:
            latest = session.scalar(
                select(StageArtifactValidationEvidence)
                .where(
                    StageArtifactValidationEvidence.variant_id
                    == variant["artifact_set_digest"],
                    StageArtifactValidationEvidence.kind == "GPU_EXPORT_LOAD",
                )
                .order_by(
                    StageArtifactValidationEvidence.registration_sequence.desc()
                )
            )
            self.assertIsNotNone(latest)
            evidence_rank = session.scalar(
                select(StageArtifactValidationRank).where(
                    StageArtifactValidationRank.evidence_id
                    == latest.identity_digest,
                    StageArtifactValidationRank.rank == 1,
                )
            )
            self.assertIsNotNone(evidence_rank)
            session.delete(evidence_rank)
            session.flush()
            with self.assertRaisesRegex(
                StageArtifactConflictError,
                "validation ranks are inconsistent",
            ):
                validated_stage_artifact_projection(
                    session, variant["artifact_set_digest"]
                )
            session.rollback()

        with self.factory() as session:
            stage_rank = session.scalar(
                select(StageArtifactRank).where(
                    StageArtifactRank.variant_id == variant["artifact_set_digest"],
                    StageArtifactRank.rank == 0,
                )
            )
            self.assertIsNotNone(stage_rank)
            manifest = session.get(ArtifactManifest, stage_rank.manifest_digest)
            self.assertIsNotNone(manifest)
            manifest.total_size_bytes += 1
            with session.no_autoflush:
                with self.assertRaisesRegex(
                    StageArtifactConflictError,
                    "manifest is internally inconsistent",
                ):
                    validated_stage_artifact_projection(
                        session, variant["artifact_set_digest"]
                    )
            session.rollback()

    def test_trusted_builder_registration_payload_is_accepted_without_adapter_logic(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source_manifest, source_digest = _write_source(source)
            with self.factory() as session:
                artifact = create_model_artifact(
                    session,
                    model_id=f"qwen2.5-builder-source-{uuid.uuid4().hex[:8]}",
                    repository=f"Example/Builder-Source-{uuid.uuid4()}",
                    revision="3" * 40,
                    manifest_digest=source_digest,
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1024,
                    layer_count=2,
                    license_id="apache-2.0",
                )
                register_artifact_manifest(
                    session,
                    artifact_id=artifact.id,
                    manifest=source_manifest,
                )
            contract = StageExportContract(
                source_manifest_digest=source_digest,
                runtime_image=self.runtime_image,
                exporter_build_digest=_digest("6"),
                pipeline_parallel_size=2,
            )
            built = TrustedStageBuilder(SyntheticNativeExporter()).build(
                source,
                root / "built",
                contract,
                source_manifest,
            )

            response = self.client.post(
                "/v1/admin/stage-artifact-variants",
                headers=self.admin,
                json=built.registration_payload(),
            )

        self.assertEqual(response.status_code, 200, response.text)
        value = response.json()["variant"]
        self.assertTrue(response.json()["created"])
        self.assertEqual(value["loader_format"], "VLLM_SHARDED_STATE_V1")
        self.assertEqual(
            [item["manifest_digest"] for item in value["stages"]],
            [item.artifact_manifest_digest for item in built.stages],
        )

    def test_same_contract_with_different_stage_bytes_is_a_conflict(self):
        variant = self._create_variant()
        before = self._registry_counts()

        response = self.client.post(
            "/v1/admin/stage-artifact-variants",
            headers=self.admin,
            json=self._variant_body(changed_rank=1),
        )

        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self._registry_counts(), before)
        detail = self.client.get(
            f"/v1/admin/stage-artifact-variants/{variant['artifact_set_digest']}",
            headers=self.admin,
        )
        self.assertEqual(detail.status_code, 200, detail.text)

    def test_missing_duplicate_wrong_topology_and_digest_are_rejected_atomically(self):
        cases = []
        missing = self._variant_body()
        missing["stages"] = missing["stages"][:1]
        cases.append(missing)
        duplicate = self._variant_body()
        duplicate["stages"][1]["pipeline_rank"] = 0
        cases.append(duplicate)
        wrong_tensor_rank = self._variant_body()
        wrong_tensor_rank["stages"][1]["tensor_rank"] = 1
        cases.append(wrong_tensor_rank)
        wrong_digest = self._variant_body()
        wrong_digest["stages"][1]["manifest_digest"] = _digest("f")
        cases.append(wrong_digest)
        nested_file = self._variant_body()
        nested_file["stages"][0]["manifest"]["files"][1]["path"] = (
            "metadata/config.json"
        )
        nested_file["stages"][0]["manifest_digest"] = (
            canonical_artifact_manifest_digest(
                nested_file["stages"][0]["manifest"]
            )
        )
        cases.append(nested_file)
        missing_metadata = self._variant_body()
        missing_metadata["stages"][0]["manifest"]["files"] = [
            item
            for item in missing_metadata["stages"][0]["manifest"]["files"]
            if item["path"] != "tokenizer.json"
        ]
        missing_metadata["stages"][0]["manifest_digest"] = (
            canonical_artifact_manifest_digest(
                missing_metadata["stages"][0]["manifest"]
            )
        )
        cases.append(missing_metadata)
        unknown_field = self._variant_body()
        unknown_field["stages"][0]["command"] = "python arbitrary.py"
        cases.append(unknown_field)

        before = self._registry_counts()
        for body in cases:
            with self.subTest(body=body):
                response = self.client.post(
                    "/v1/admin/stage-artifact-variants",
                    headers=self.admin,
                    json=body,
                )
                self.assertEqual(response.status_code, 422, response.text)
                self.assertEqual(self._registry_counts(), before)

    def test_source_runtime_and_vllm_identity_are_fail_closed(self):
        missing_source = self._variant_body()
        missing_source["source_manifest_digest"] = _digest("f")
        response = self.client.post(
            "/v1/admin/stage-artifact-variants",
            headers=self.admin,
            json=missing_source,
        )
        self.assertEqual(response.status_code, 404, response.text)

        missing_runtime = self._variant_body()
        missing_runtime["runtime_image"] = "registry.example/other@sha256:" + "8" * 64
        response = self.client.post(
            "/v1/admin/stage-artifact-variants",
            headers=self.admin,
            json=missing_runtime,
        )
        self.assertEqual(response.status_code, 404, response.text)

        wrong_vllm = self._variant_body()
        wrong_vllm["vllm_version"] = "0.9.1"
        response = self.client.post(
            "/v1/admin/stage-artifact-variants",
            headers=self.admin,
            json=wrong_vllm,
        )
        self.assertEqual(response.status_code, 422, response.text)

        tagged_image = self._variant_body()
        tagged_image["runtime_image"] = (
            "registry.example/vllm:latest@sha256:" + "2" * 64
        )
        response = self.client.post(
            "/v1/admin/stage-artifact-variants",
            headers=self.admin,
            json=tagged_image,
        )
        self.assertEqual(response.status_code, 422, response.text)

    def test_latest_gpu_evidence_gates_validation_and_revoke_is_terminal(self):
        variant = self._create_variant()
        endpoint = (
            "/v1/admin/stage-artifact-variants/"
            f"{variant['artifact_set_digest']}"
        )
        transition = endpoint + "/transition"

        response = self.client.post(
            transition,
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(response.status_code, 409, response.text)

        synthetic = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=self._evidence_body(variant, kind="SYNTHETIC"),
        )
        self.assertEqual(synthetic.status_code, 200, synthetic.text)
        self.assertEqual(synthetic.json()["evidence"]["registration_sequence"], 1)
        response = self.client.post(
            transition,
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(response.status_code, 409, response.text)

        passed_body = self._evidence_body(variant)
        passed = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=passed_body,
        )
        self.assertEqual(passed.status_code, 200, passed.text)
        self.assertEqual(passed.json()["evidence"]["registration_sequence"], 2)
        replay = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=passed_body,
        )
        self.assertEqual(replay.status_code, 200, replay.text)
        self.assertFalse(replay.json()["created"])
        self.assertEqual(replay.json()["evidence"]["registration_sequence"], 2)
        conflicting_replay = copy.deepcopy(passed_body)
        conflicting_replay.update(
            {
                "status": "FAILED",
                "failure_code": "STAGE_LOAD_FAILED",
                "ranks": [],
            }
        )
        conflict = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=conflicting_replay,
        )
        self.assertEqual(conflict.status_code, 409, conflict.text)

        validated = self.client.post(
            transition,
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(validated.status_code, 200, validated.text)
        self.assertEqual(validated.json()["variant"]["status"], "VALIDATED")
        self.assertIsNotNone(validated.json()["variant"]["validated_at"])

        replay_after_validation = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=passed_body,
        )
        self.assertEqual(
            replay_after_validation.status_code,
            200,
            replay_after_validation.text,
        )
        self.assertFalse(replay_after_validation.json()["created"])
        self.assertEqual(
            replay_after_validation.json()["evidence"]["registration_sequence"],
            2,
        )

        before_late_evidence = self._registry_counts()
        late_evidence = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=self._evidence_body(
                variant,
                status="FAILED",
                failure_code="STAGE_LOAD_FAILED",
                ranks=[],
            ),
        )
        self.assertEqual(late_evidence.status_code, 409, late_evidence.text)
        self.assertEqual(self._registry_counts(), before_late_evidence)
        detail = self.client.get(endpoint, headers=self.admin)
        self.assertEqual(detail.status_code, 200, detail.text)
        self.assertEqual(detail.json()["variant"]["status"], "VALIDATED")

        repeated_validation = self.client.post(
            transition,
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(
            repeated_validation.status_code,
            200,
            repeated_validation.text,
        )

        validation_run_id = str(uuid.uuid4())
        failed_value = {
            "schema_version": 1,
            "variant_identity_digest": variant["artifact_set_digest"],
            "validation_run_id": validation_run_id,
            "kind": "GPU_EXPORT_LOAD",
            "status": "FAILED",
            "validator_version": "out-of-band-failure",
            "validator_build_digest": _digest("e"),
            "failure_code": "STAGE_LOAD_FAILED",
            "ranks": [],
        }
        canonical_json = json.dumps(
            failed_value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        identity_digest = "sha256:" + hashlib.sha256(
            canonical_json.encode("utf-8")
        ).hexdigest()
        with self.factory() as session:
            session.add(
                StageArtifactValidationEvidence(
                    identity_digest=identity_digest,
                    variant_id=variant["artifact_set_digest"],
                    validation_run_id=validation_run_id,
                    registration_sequence=3,
                    schema_version=1,
                    kind="GPU_EXPORT_LOAD",
                    status="FAILED",
                    validator_version="out-of-band-failure",
                    validator_build_digest=_digest("e"),
                    rank_count=0,
                    failure_code="STAGE_LOAD_FAILED",
                    canonical_evidence_json=canonical_json,
                )
            )
            session.commit()

        stale_revalidation = self.client.post(
            transition,
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(stale_revalidation.status_code, 409, stale_revalidation.text)
        detail = self.client.get(endpoint, headers=self.admin)
        self.assertEqual(detail.status_code, 200, detail.text)
        self.assertEqual(detail.json()["variant"]["status"], "VALIDATED")

        revoked = self.client.post(
            transition,
            headers=self.admin,
            json={"status": "REVOKED"},
        )
        self.assertEqual(revoked.status_code, 200, revoked.text)
        self.assertEqual(revoked.json()["variant"]["status"], "REVOKED")

        replay_after_revoke = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=passed_body,
        )
        self.assertEqual(replay_after_revoke.status_code, 200, replay_after_revoke.text)
        self.assertFalse(replay_after_revoke.json()["created"])

        after_revoke = self._registry_counts()
        new_after_revoke = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=self._evidence_body(
                variant,
                status="FAILED",
                failure_code="STAGE_LOAD_FAILED",
                ranks=[],
            ),
        )
        self.assertEqual(new_after_revoke.status_code, 409, new_after_revoke.text)
        self.assertEqual(self._registry_counts(), after_revoke)

        response = self.client.post(
            transition,
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(response.status_code, 409, response.text)

    def test_variant_readback_failure_rolls_back_registration(self):
        before = self._registry_counts()

        with patch(
            "dure.control.api.stage_artifact_variant_dict",
            side_effect=StageArtifactConflictError(
                "stored stage artifact variant is internally inconsistent"
            ),
        ):
            response = self.client.post(
                "/v1/admin/stage-artifact-variants",
                headers=self.admin,
                json=self._variant_body(),
            )

        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self._registry_counts(), before)

    def test_evidence_readback_failure_rolls_back_registration(self):
        variant = self._create_variant()
        endpoint = (
            "/v1/admin/stage-artifact-variants/"
            f"{variant['artifact_set_digest']}/evidence"
        )
        before = self._registry_counts()

        with patch(
            "dure.control.api.stage_artifact_evidence_dict",
            side_effect=StageArtifactConflictError(
                "stored stage validation evidence is internally inconsistent"
            ),
        ):
            response = self.client.post(
                endpoint,
                headers=self.admin,
                json=self._evidence_body(variant),
            )

        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self._registry_counts(), before)

    def test_transition_readback_failure_rolls_back_status_and_audit(self):
        variant = self._create_variant()
        endpoint = (
            "/v1/admin/stage-artifact-variants/"
            f"{variant['artifact_set_digest']}"
        )
        evidence = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=self._evidence_body(variant),
        )
        self.assertEqual(evidence.status_code, 200, evidence.text)
        before = self._registry_counts()

        with patch(
            "dure.control.api.stage_artifact_variant_dict",
            side_effect=StageArtifactConflictError(
                "stored stage artifact variant is internally inconsistent"
            ),
        ):
            response = self.client.post(
                endpoint + "/transition",
                headers=self.admin,
                json={"status": "VALIDATED"},
            )

        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self._registry_counts(), before)
        detail = self.client.get(endpoint, headers=self.admin)
        self.assertEqual(detail.status_code, 200, detail.text)
        self.assertEqual(detail.json()["variant"]["status"], "DRAFT")

    def test_newer_failed_gpu_evidence_blocks_older_pass_but_synthetic_does_not(self):
        variant = self._create_variant()
        endpoint = (
            "/v1/admin/stage-artifact-variants/"
            f"{variant['artifact_set_digest']}"
        )
        for body in (
            self._evidence_body(variant, validator_version="gpu-pass-1"),
            self._evidence_body(
                variant,
                status="FAILED",
                validator_version="gpu-fail-2",
                failure_code="STAGE_LOAD_FAILED",
                ranks=[],
            ),
        ):
            response = self.client.post(
                endpoint + "/evidence",
                headers=self.admin,
                json=body,
            )
            self.assertEqual(response.status_code, 200, response.text)
        blocked = self.client.post(
            endpoint + "/transition",
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(blocked.status_code, 409, blocked.text)

        not_run = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=self._evidence_body(
                variant,
                status="NOT_RUN",
                validator_version="gpu-not-run-3",
                failure_code="STAGE_GPU_NOT_AVAILABLE",
                ranks=[],
            ),
        )
        self.assertEqual(not_run.status_code, 200, not_run.text)
        blocked = self.client.post(
            endpoint + "/transition",
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(blocked.status_code, 409, blocked.text)

        newer_pass = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=self._evidence_body(variant, validator_version="gpu-pass-4"),
        )
        self.assertEqual(newer_pass.status_code, 200, newer_pass.text)
        synthetic_failure = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json=self._evidence_body(
                variant,
                kind="SYNTHETIC",
                status="FAILED",
                validator_version="synthetic-fail-5",
                failure_code="STAGE_TENSOR_COVERAGE_INVALID",
                ranks=[],
            ),
        )
        self.assertEqual(synthetic_failure.status_code, 200, synthetic_failure.text)
        validated = self.client.post(
            endpoint + "/transition",
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(validated.status_code, 200, validated.text)

    def test_passing_evidence_rejects_missing_swapped_or_partial_rank_coverage(self):
        variant = self._create_variant()
        endpoint = (
            "/v1/admin/stage-artifact-variants/"
            f"{variant['artifact_set_digest']}/evidence"
        )
        full = self._evidence_body(variant)
        missing = copy.deepcopy(full)
        missing["ranks"] = missing["ranks"][:1]
        swapped = copy.deepcopy(full)
        swapped["ranks"][0]["manifest_digest"] = swapped["ranks"][1][
            "manifest_digest"
        ]
        wrong_size = copy.deepcopy(full)
        wrong_size["ranks"][0]["loaded_weight_size_bytes"] += 1
        wrong_identity = copy.deepcopy(full)
        wrong_identity["variant_identity_digest"] = _digest("f")

        before = self._registry_counts()
        for body in (missing, swapped, wrong_size, wrong_identity):
            response = self.client.post(endpoint, headers=self.admin, json=body)
            self.assertEqual(response.status_code, 422, response.text)
            self.assertEqual(self._registry_counts(), before)

    def test_list_and_openapi_expose_closed_management_surface(self):
        variant = self._create_variant()
        response = self.client.get(
            "/v1/admin/stage-artifact-variants",
            headers=self.admin,
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            [item["artifact_set_digest"] for item in response.json()["variants"]],
            [variant["artifact_set_digest"]],
        )
        schema = self.client.get("/openapi.json").json()
        create_schema = schema["components"]["schemas"]["StageArtifactVariantCreate"]
        evidence_schema = schema["components"]["schemas"]["StageArtifactEvidenceCreate"]
        self.assertFalse(create_schema["additionalProperties"])
        self.assertFalse(evidence_schema["additionalProperties"])
        for forbidden in ("command", "args", "env", "path", "url", "token"):
            self.assertNotIn(forbidden, create_schema["properties"])
            self.assertNotIn(forbidden, evidence_schema["properties"])


if __name__ == "__main__":
    unittest.main()
