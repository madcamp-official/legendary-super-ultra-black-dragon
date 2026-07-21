from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import threading
import unittest
from pathlib import Path

from dure.agent import Agent, TaskExecutor
from dure.artifact_download import ArtifactChunkDownloader, TrustedHTTPSOrigin
from dure.artifact_manifest import parse_artifact_manifest
from dure.artifact_prepare import (
    ArtifactPreparationError,
    ArtifactPreparationExecutor,
    PREPARATION_FAILURE_CODES,
    preparation_failure_code,
    trusted_origin_from_config,
    validate_preparation_result,
)
from dure.command import CommandResult
from dure.http import APIError
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_VERIFICATION_VERSION,
)
from dure.model_store import (
    ContentAddressedModelStore,
    ModelCachePreparer,
    ModelStoreError,
    PreparedModelCache,
)
from dure.stage_cache import StageCacheIdentity, stage_contract_identity_digest


NODE_ID = "11111111-1111-4111-8111-111111111111"
PREPARATION_ID = "22222222-2222-4222-8222-222222222222"
PREPARATION_NODE_ID = "33333333-3333-4333-8333-333333333333"
ATTEMPT_ID = "44444444-4444-4444-8444-444444444444"
DEPLOYMENT_ID = "55555555-5555-4555-8555-555555555555"
TASK_ID = "66666666-6666-4666-8666-666666666666"
MANIFEST_DIGEST = "sha256:" + "b" * 64
RUNTIME_IMAGE = "registry.example.test/vllm@sha256:" + "c" * 64
ORIGIN = TrustedHTTPSOrigin("https://artifacts.example.test/models")
MANIFEST = {
    "schema_version": 1,
    "files": [
        {
            "path": "config.json",
            "kind": "REGULAR",
            "size_bytes": 2,
            "sha256": "sha256:" + "d" * 64,
            "chunks": [
                {
                    "ordinal": 0,
                    "offset_bytes": 0,
                    "length_bytes": 2,
                    "sha256": "sha256:" + "d" * 64,
                }
            ],
        }
    ],
}


def common_payload() -> dict:
    return {
        "preparation_id": PREPARATION_ID,
        "preparation_node_id": PREPARATION_NODE_ID,
        "attempt_id": ATTEMPT_ID,
        "attempt_no": 1,
        "deployment_id": DEPLOYMENT_ID,
        "generation": 7,
        "node_id": NODE_ID,
        "apply": True,
    }


def model_task() -> dict:
    return {
        "id": TASK_ID,
        "node_id": NODE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "type": "PREPARE_MODEL",
        "payload": {
            **common_payload(),
            "model_id": "dure-model",
            "repository": "Example/Dure-Model",
            "revision": "a" * 40,
            "manifest_digest": MANIFEST_DIGEST,
            "quantization": "fp16",
            "cache_kind": MODEL_CACHE_KIND_FULL_SNAPSHOT,
        },
    }


def stage_model_task() -> dict:
    task = model_task()
    contract_identity_digest = stage_contract_identity_digest(
        source_manifest_digest="sha256:" + "3" * 64,
        runtime_image=RUNTIME_IMAGE,
        vllm_version="0.9.0",
        exporter_build_digest="sha256:" + "4" * 64,
        architecture="Qwen2ForCausalLM",
        quantization="awq",
        tensor_parallel_size=1,
        pipeline_parallel_size=3,
        loader_format="VLLM_SHARDED_STATE_V1",
    )
    task["payload"].update(
        cache_kind=MODEL_CACHE_KIND_STAGE,
        artifact_set_digest="sha256:" + "1" * 64,
        contract_identity_digest=contract_identity_digest,
        source_manifest_digest="sha256:" + "3" * 64,
        runtime_image=RUNTIME_IMAGE,
        vllm_version="0.9.0",
        exporter_build_digest="sha256:" + "4" * 64,
        architecture="Qwen2ForCausalLM",
        loader_format="VLLM_SHARDED_STATE_V1",
        tensor_parallel_size=1,
        pipeline_parallel_size=3,
        pipeline_rank=1,
        tensor_rank=0,
        tensor_keys_digest="sha256:" + "5" * 64,
    )
    task["payload"]["quantization"] = "awq"
    return task


def image_task() -> dict:
    return {
        "id": TASK_ID,
        "node_id": NODE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "type": "PREPARE_IMAGE",
        "payload": {
            **common_payload(),
            "runtime_image": RUNTIME_IMAGE,
        },
    }


class FakeModelPreparer:
    def __init__(self, *, exception: Exception | None = None) -> None:
        self.exception = exception
        self.calls = []

    def prepare_full_snapshot(self, *, identity, manifest, origin):
        self.calls.append((identity, manifest, origin))
        if self.exception is not None:
            raise self.exception
        return PreparedModelCache(
            path=Path("/var/lib/dure/models/not-returned-to-controller"),
            identity=identity,
            reused=len(self.calls) > 1,
            file_count=3,
            total_size_bytes=4096,
        )

    def prepare_stage(self, *, identity, manifest, origin):
        self.calls.append((identity, manifest, origin))
        if self.exception is not None:
            raise self.exception
        return PreparedModelCache(
            path=Path("/var/lib/dure/models/stages/not-returned-to-controller"),
            identity=identity,
            reused=len(self.calls) > 1,
            file_count=5,
            total_size_bytes=8192,
        )


class MemoryResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.status = 200
        self.offset = 0

    def header_values(self, name: str) -> tuple[str, ...]:
        if name.lower() == "content-length":
            return (str(len(self.payload)),)
        return ()

    def read(self, size: int) -> bytes:
        value = self.payload[self.offset : self.offset + size]
        self.offset += len(value)
        return value

    def close(self) -> None:
        return None


class MemoryTransport:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.calls = []

    def open(self, origin, object_url, *, headers, timeout_seconds):
        del origin, timeout_seconds
        self.calls.append((object_url, dict(headers)))
        digest = "sha256:" + object_url.rsplit("/", 1)[-1]
        return MemoryResponse(self.objects[digest])


class ImageRunner:
    def __init__(self, inspect_results, *, pull_result=(0, "", "")) -> None:
        self.inspect_results = list(inspect_results)
        self.pull_result = pull_result
        self.calls: list[tuple[str, ...]] = []

    def exists(self, executable: str) -> bool:
        return executable == "docker"

    def run(self, argv, *, timeout=15, env=None):
        del timeout, env
        command = tuple(argv)
        self.calls.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            if not self.inspect_results:
                raise AssertionError("unexpected image inspection")
            return CommandResult(command, *self.inspect_results.pop(0))
        if command[:3] == ("docker", "pull", "--quiet"):
            return CommandResult(command, *self.pull_result)
        raise AssertionError(f"unexpected Docker command: {command}")

    def run_limited_output(
        self, argv, *, timeout=15, max_output_bytes, env=None
    ):
        del max_output_bytes
        return self.run(argv, timeout=timeout, env=env)


class MissingDockerRunner:
    def exists(self, executable: str) -> bool:
        return False

    def run(self, argv, *, timeout=15, env=None):  # pragma: no cover
        raise AssertionError("Docker must not be invoked")


class FakeAgentClient:
    def __init__(self, task: dict, manifest: dict | None = None) -> None:
        self.task = task
        self.manifest = manifest or MANIFEST
        self.requests = []

    def request(self, method, path, payload=None):
        self.requests.append((method, path, payload))
        if path == "/v1/agent/tasks/claim":
            return {"task": self.task}
        if path.endswith("/artifact-manifest"):
            return {"manifest": copy.deepcopy(self.manifest)}
        return {}


class FailingReportAgentClient(FakeAgentClient):
    def __init__(
        self,
        task: dict,
        report_errors: list[APIError],
        manifest: dict | None = None,
    ) -> None:
        super().__init__(task, manifest)
        self.report_errors = list(report_errors)
        self.claim_count = 0

    def request(self, method, path, payload=None):
        if path == "/v1/agent/tasks/claim":
            self.claim_count += 1
        if (
            path.endswith(("/complete", "/fail"))
            and self.report_errors
        ):
            self.requests.append((method, path, payload))
            raise self.report_errors.pop(0)
        return super().request(method, path, payload)


class AgentPreparationTests(unittest.TestCase):
    def model_executor(self, preparer=None, *, loader=None):
        return ArtifactPreparationExecutor(
            NODE_ID,
            origin=ORIGIN,
            model_preparer=preparer or FakeModelPreparer(),
            manifest_loader=loader or (lambda task_id: copy.deepcopy(MANIFEST)),
        )

    def test_agent_heartbeat_uses_only_closed_preparation_progress(self):
        agent = object.__new__(Agent)

        class ProgressExecutor:
            @staticmethod
            def preparation_progress(task_id):
                self.assertEqual(task_id, TASK_ID)
                return {"downloaded_bytes": 123}

        agent.executor = ProgressExecutor()
        self.assertEqual(
            agent._task_heartbeat_payload(
                TASK_ID,
                is_preparation=True,
            ),
            {"progress": {"downloaded_bytes": 123}},
        )
        self.assertIsNone(
            agent._task_heartbeat_payload(
                TASK_ID,
                is_preparation=False,
            )
        )

    def test_task_executor_keeps_legacy_preparation_executor_compatible(self):
        class LegacyPreparationExecutor:
            @staticmethod
            def execute(_task):
                return {"ok": True}

        executor = TaskExecutor(
            NODE_ID,
            preparation_executor=LegacyPreparationExecutor(),
        )
        self.assertIsNone(executor.preparation_progress(TASK_ID))
        executor.clear_preparation_progress(TASK_ID)

    def test_model_task_uses_task_scoped_manifest_and_local_origin(self):
        preparer = FakeModelPreparer()
        loaded = []

        def load_manifest(task_id):
            loaded.append(task_id)
            return copy.deepcopy(MANIFEST)

        executor = self.model_executor(preparer, loader=load_manifest)

        result = executor.execute(model_task())

        self.assertEqual(loaded, [TASK_ID])
        self.assertEqual(len(preparer.calls), 1)
        identity, manifest, origin = preparer.calls[0]
        self.assertEqual(identity.manifest_digest, MANIFEST_DIGEST)
        self.assertEqual(identity.cache_kind, MODEL_CACHE_KIND_FULL_SNAPSHOT)
        self.assertEqual(manifest, MANIFEST)
        self.assertIs(origin, ORIGIN)
        self.assertEqual(
            set(result),
            {
                "preparation_id",
                "preparation_node_id",
                "attempt_id",
                "attempt_no",
                "deployment_id",
                "generation",
                "node_id",
                "stage",
                "reused",
                "model_id",
                "manifest_digest",
                "cache_kind",
                "verification_version",
                "bytes_verified",
                "file_count",
            },
        )
        self.assertEqual(result["bytes_verified"], 4096)
        self.assertEqual(
            result["verification_version"], MODEL_CACHE_VERIFICATION_VERSION
        )
        self.assertNotIn("path", result)
        self.assertNotIn("manifest", result)

    def test_stage_task_uses_closed_composite_identity_and_reports_no_path(self):
        preparer = FakeModelPreparer()
        executor = self.model_executor(preparer)

        task = stage_model_task()
        result = executor.execute(task)

        self.assertEqual(len(preparer.calls), 1)
        identity, manifest, origin = preparer.calls[0]
        self.assertIsInstance(identity, StageCacheIdentity)
        self.assertEqual(identity.pipeline_rank, 1)
        self.assertEqual(identity.tensor_rank, 0)
        self.assertEqual(identity.artifact_set_digest, "sha256:" + "1" * 64)
        self.assertEqual(manifest, MANIFEST)
        self.assertIs(origin, ORIGIN)
        self.assertEqual(result["cache_kind"], MODEL_CACHE_KIND_STAGE)
        self.assertEqual(result["manifest_digest"], MANIFEST_DIGEST)
        self.assertEqual(result["artifact_set_digest"], identity.artifact_set_digest)
        self.assertEqual(result["pipeline_rank"], 1)
        self.assertEqual(result["tensor_rank"], 0)
        self.assertEqual(result["tensor_keys_digest"], identity.tensor_keys_digest)
        self.assertEqual(
            result["cache_identity_digest"], identity.cache_identity_digest
        )
        self.assertEqual(result["bytes_verified"], 8192)
        self.assertEqual(result["file_count"], 5)
        self.assertNotIn("path", result)
        self.assertNotIn("manifest", result)
        self.assertNotIn("runtime_image", result)

    def test_stage_result_history_rejects_boolean_integer_aliases(self):
        task = stage_model_task()
        result = self.model_executor(FakeModelPreparer()).execute(task)

        for field in ("verification_version", "pipeline_rank", "tensor_rank"):
            with self.subTest(field=field):
                tampered = dict(result)
                tampered[field] = bool(result[field])
                with self.assertRaises(ArtifactPreparationError) as caught:
                    validate_preparation_result(task, tampered, NODE_ID)
                self.assertEqual(
                    caught.exception.code,
                    "PREPARATION_HISTORY_INVALID",
                )

    def test_stage_task_rejects_partial_mixed_and_arbitrary_payloads_before_action(self):
        variants = []
        missing = stage_model_task()
        missing["payload"].pop("tensor_keys_digest")
        variants.append(missing)
        mixed = model_task()
        mixed["payload"]["pipeline_rank"] = 1
        variants.append(mixed)
        unpinned = stage_model_task()
        unpinned["payload"]["runtime_image"] = "registry.example/vllm:latest"
        variants.append(unpinned)
        boolean_rank = stage_model_task()
        boolean_rank["payload"]["pipeline_rank"] = True
        variants.append(boolean_rank)
        with_path = stage_model_task()
        with_path["payload"]["path"] = "/tmp/foreign-stage"
        variants.append(with_path)

        for task in variants:
            preparer = FakeModelPreparer()
            with self.subTest(task=task), self.assertRaises(
                ArtifactPreparationError
            ) as caught:
                self.model_executor(preparer).execute(task)
            self.assertEqual(caught.exception.code, "PREPARATION_PAYLOAD_REJECTED")
            self.assertEqual(preparer.calls, [])

    def test_model_handler_integrates_pr3_store_with_fake_transport(self):
        config = b'{"model_type":"dure-test"}'
        digest = "sha256:" + hashlib.sha256(config).hexdigest()
        manifest = {
            "schema_version": 1,
            "files": [
                {
                    "path": "config.json",
                    "kind": "REGULAR",
                    "size_bytes": len(config),
                    "sha256": digest,
                    "chunks": [
                        {
                            "ordinal": 0,
                            "offset_bytes": 0,
                            "length_bytes": len(config),
                            "sha256": digest,
                        }
                    ],
                }
            ],
        }
        task = model_task()
        task["payload"]["manifest_digest"] = parse_artifact_manifest(
            manifest
        ).digest
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = ContentAddressedModelStore(
                store_root=root / "store",
                model_root=root / "models",
            )
            transport = MemoryTransport({digest: config})
            downloader = ArtifactChunkDownloader(
                store,
                transport=transport,
                attempts=1,
            )
            preparer = ModelCachePreparer(
                store,
                downloader,
                disk_reserve_bytes=0,
            )
            executor = ArtifactPreparationExecutor(
                NODE_ID,
                origin=ORIGIN,
                model_preparer=preparer,
                manifest_loader=lambda task_id: copy.deepcopy(manifest),
            )

            try:
                result = executor.execute(task)
            except ModelStoreError as exc:
                if exc.code == "MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE":
                    self.skipTest(
                        "Linux renameat2(RENAME_NOREPLACE) is unavailable"
                    )
                raise

        self.assertEqual(result["bytes_verified"], len(config))
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(
            transport.calls[0][0],
            ORIGIN.object_url(digest),
        )
        self.assertNotIn("url", task["payload"])
        self.assertEqual(
            executor.progress_snapshot(TASK_ID),
            {"downloaded_bytes": len(config)},
        )

    def test_model_handler_exposes_live_chunk_high_water_without_identity_data(self):
        first_block = 1024 * 1024
        config = b'{"model_type":"dure-test"}'
        weights = b"x" * (first_block + 128)
        config_digest = "sha256:" + hashlib.sha256(config).hexdigest()
        weights_digest = "sha256:" + hashlib.sha256(weights).hexdigest()
        manifest = {
            "schema_version": 1,
            "files": [
                {
                    "path": "config.json",
                    "kind": "REGULAR",
                    "size_bytes": len(config),
                    "sha256": config_digest,
                    "chunks": [
                        {
                            "ordinal": 0,
                            "offset_bytes": 0,
                            "length_bytes": len(config),
                            "sha256": config_digest,
                        }
                    ],
                },
                {
                    "path": "model.safetensors",
                    "kind": "REGULAR",
                    "size_bytes": len(weights),
                    "sha256": weights_digest,
                    "chunks": [
                        {
                            "ordinal": 0,
                            "offset_bytes": 0,
                            "length_bytes": len(weights),
                            "sha256": weights_digest,
                        }
                    ],
                }
            ],
        }
        task = model_task()
        task["payload"]["manifest_digest"] = parse_artifact_manifest(
            manifest
        ).digest
        blocked = threading.Event()
        release = threading.Event()

        class BlockingResponse(MemoryResponse):
            def __init__(self, payload):
                super().__init__(payload)
                self.read_count = 0

            def read(self, size):
                self.read_count += 1
                if self.read_count == 2:
                    blocked.set()
                    if not release.wait(5):
                        raise TimeoutError("test did not release transport")
                return super().read(size)

        class BlockingTransport(MemoryTransport):
            def open(self, origin, object_url, *, headers, timeout_seconds):
                del origin, timeout_seconds
                self.calls.append((object_url, dict(headers)))
                digest = "sha256:" + object_url.rsplit("/", 1)[-1]
                payload = self.objects[digest]
                return (
                    BlockingResponse(payload)
                    if digest == weights_digest
                    else MemoryResponse(payload)
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = ContentAddressedModelStore(
                store_root=root / "store",
                model_root=root / "models",
            )
            preparer = ModelCachePreparer(
                store,
                ArtifactChunkDownloader(
                    store,
                    transport=BlockingTransport(
                        {
                            config_digest: config,
                            weights_digest: weights,
                        }
                    ),
                    attempts=1,
                ),
                disk_reserve_bytes=0,
            )
            executor = ArtifactPreparationExecutor(
                NODE_ID,
                origin=ORIGIN,
                model_preparer=preparer,
                manifest_loader=lambda task_id: copy.deepcopy(manifest),
            )
            errors = []

            def execute():
                try:
                    executor.execute(task)
                except Exception as exc:  # pragma: no cover - assertion below
                    errors.append(exc)

            worker = threading.Thread(target=execute)
            worker.start()
            self.assertTrue(blocked.wait(5))
            live_bytes = executor.progress_snapshot(TASK_ID)[
                "downloaded_bytes"
            ]
            self.assertIn(
                live_bytes,
                {first_block, first_block + len(config)},
            )
            self.assertNotIn("manifest_digest", executor.progress_snapshot(TASK_ID))
            self.assertNotIn("url", executor.progress_snapshot(TASK_ID))
            release.set()
            worker.join(10)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(
                executor.progress_snapshot(TASK_ID),
                {"downloaded_bytes": len(config) + len(weights)},
            )

    def test_model_task_rejects_remote_origin_path_token_and_arbitrary_fields(self):
        forbidden = {
            "url": "https://evil.example/chunk",
            "path": "/tmp/model",
            "token": "secret-token",
            "headers": {"Authorization": "secret-token"},
            "command": ["sh", "-c", "id"],
        }
        for field, value in forbidden.items():
            with self.subTest(field=field):
                task = model_task()
                task["payload"][field] = value
                with self.assertRaises(ArtifactPreparationError) as caught:
                    self.model_executor().execute(task)
                self.assertEqual(
                    caught.exception.code, "PREPARATION_PAYLOAD_REJECTED"
                )

    def test_foreign_node_and_generation_binding_fail_before_host_action(self):
        task = model_task()
        task["payload"]["node_id"] = "77777777-7777-4777-8777-777777777777"
        preparer = FakeModelPreparer()
        with self.assertRaises(ArtifactPreparationError) as foreign:
            self.model_executor(preparer).execute(task)
        self.assertEqual(foreign.exception.code, "PREPARATION_NODE_MISMATCH")
        self.assertEqual(preparer.calls, [])

        task = model_task()
        task["deployment_id"] = "77777777-7777-4777-8777-777777777777"
        with self.assertRaises(ArtifactPreparationError) as binding:
            self.model_executor(preparer).execute(task)
        self.assertEqual(
            binding.exception.code, "PREPARATION_BINDING_MISMATCH"
        )
        self.assertEqual(preparer.calls, [])

        task = model_task()
        task["payload"]["generation"] = 0
        with self.assertRaises(ArtifactPreparationError) as generation:
            self.model_executor(preparer).execute(task)
        self.assertEqual(
            generation.exception.code, "PREPARATION_PAYLOAD_REJECTED"
        )
        self.assertEqual(preparer.calls, [])

    def test_missing_manifest_and_digest_mismatch_are_closed_codes(self):
        unavailable = self.model_executor(loader=lambda task_id: None)
        with self.assertRaises(ArtifactPreparationError) as missing:
            unavailable.execute(model_task())
        self.assertEqual(
            missing.exception.code, "PREPARATION_MANIFEST_UNAVAILABLE"
        )

        preparer = FakeModelPreparer(
            exception=ModelStoreError("MODEL_STORE_DIGEST_MISMATCH")
        )
        with self.assertRaises(ModelStoreError) as mismatch:
            self.model_executor(preparer).execute(model_task())
        self.assertEqual(mismatch.exception.code, "MODEL_STORE_DIGEST_MISMATCH")
        self.assertEqual(
            preparation_failure_code(mismatch.exception),
            "MODEL_STORE_DIGEST_MISMATCH",
        )

    def test_origin_is_local_closed_https_configuration(self):
        origin = trusted_origin_from_config(
            {
                "base_url": "https://artifacts.example.test/models",
                "allowed_redirect_hosts": ["cdn.example.test"],
            }
        )
        self.assertEqual(origin.base_url, "https://artifacts.example.test/models")
        with self.assertRaises(ValueError):
            trusted_origin_from_config(
                {
                    "base_url": "https://artifacts.example.test/models",
                    "allowed_redirect_hosts": [],
                    "token": "must-not-be-supported",
                }
            )

    def test_image_reuse_and_pull_verify_exact_digest_without_container_action(self):
        exact = json.dumps([RUNTIME_IMAGE])
        reused_runner = ImageRunner([(0, exact, "")])
        reused = ArtifactPreparationExecutor(
            NODE_ID,
            runner=reused_runner,
            model_preparer=FakeModelPreparer(),
        ).execute(image_task())
        self.assertTrue(reused["reused"])
        self.assertEqual(reused["image_id"], "sha256:" + "c" * 64)

        pull_runner = ImageRunner(
            [(1, "", "not found"), (0, exact, "")],
            pull_result=(0, "pulled", ""),
        )
        pulled = ArtifactPreparationExecutor(
            NODE_ID,
            runner=pull_runner,
            model_preparer=FakeModelPreparer(),
        ).execute(image_task())
        self.assertFalse(pulled["reused"])
        self.assertEqual(
            [command[:3] for command in pull_runner.calls],
            [
                ("docker", "image", "inspect"),
                ("docker", "pull", "--quiet"),
                ("docker", "image", "inspect"),
            ],
        )
        self.assertFalse(
            any(
                command[:2] in {("docker", "run"), ("docker", "stop")}
                for command in pull_runner.calls
            )
        )

    def test_image_digest_mismatch_pull_failure_and_unpinned_input_fail_closed(self):
        other = "registry.example.test/vllm@sha256:" + "d" * 64
        mismatch_runner = ImageRunner([(0, json.dumps([other]), "")])
        with self.assertRaises(ArtifactPreparationError) as mismatch:
            ArtifactPreparationExecutor(
                NODE_ID,
                runner=mismatch_runner,
                model_preparer=FakeModelPreparer(),
            ).execute(image_task())
        self.assertEqual(
            mismatch.exception.code, "PREPARATION_IMAGE_DIGEST_MISMATCH"
        )

        pull_runner = ImageRunner(
            [(1, "", "missing")],
            pull_result=(1, "", "registry credential must not leak"),
        )
        with self.assertRaises(ArtifactPreparationError) as pull:
            ArtifactPreparationExecutor(
                NODE_ID,
                runner=pull_runner,
                model_preparer=FakeModelPreparer(),
            ).execute(image_task())
        self.assertEqual(
            pull.exception.code, "PREPARATION_IMAGE_PULL_FAILED"
        )
        self.assertNotIn("credential", str(pull.exception))

        task = image_task()
        task["payload"]["runtime_image"] = "registry.example.test/vllm:latest"
        with self.assertRaises(ArtifactPreparationError) as unpinned:
            ArtifactPreparationExecutor(
                NODE_ID,
                runner=MissingDockerRunner(),
                model_preparer=FakeModelPreparer(),
            ).execute(task)
        self.assertEqual(
            unpinned.exception.code, "PREPARATION_PAYLOAD_REJECTED"
        )

    def test_missing_docker_never_runs_a_host_command(self):
        with self.assertRaises(ArtifactPreparationError) as caught:
            ArtifactPreparationExecutor(
                NODE_ID,
                runner=MissingDockerRunner(),
                model_preparer=FakeModelPreparer(),
            ).execute(image_task())
        self.assertEqual(
            caught.exception.code, "PREPARATION_RUNTIME_UNAVAILABLE"
        )

    def test_agent_replays_closed_result_without_reexecuting_download(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            preparer = FakeModelPreparer()
            client = FakeAgentClient(model_task())
            agent = Agent(
                {
                    "server": "https://controller.example.test",
                    "node_id": NODE_ID,
                    "credential": "agent-secret",
                    "state_file": str(root / "state.json"),
                    "artifact_origin": {
                        "base_url": ORIGIN.base_url,
                        "allowed_redirect_hosts": [],
                    },
                },
                history_path=root / "history.json",
            )
            agent.client = client
            agent.executor = TaskExecutor(
                NODE_ID,
                preparation_executor=ArtifactPreparationExecutor(
                    NODE_ID,
                    origin=ORIGIN,
                    model_preparer=preparer,
                    manifest_loader=lambda task_id: copy.deepcopy(MANIFEST),
                ),
            )

            self.assertTrue(agent.once())
            self.assertTrue(agent.once())

            self.assertEqual(len(preparer.calls), 1)
            completions = [
                request
                for request in client.requests
                if request[1].endswith("/complete")
            ]
            self.assertEqual(len(completions), 2)
            self.assertEqual(completions[0][2], completions[1][2])

    def test_agent_retries_pending_report_before_claim_without_reexecution(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            preparer = FakeModelPreparer()
            client = FailingReportAgentClient(
                model_task(),
                [APIError("temporary report failure", status_code=503)],
            )
            agent = Agent(
                {
                    "server": "https://controller.example.test",
                    "node_id": NODE_ID,
                    "credential": "agent-secret",
                    "state_file": str(root / "state.json"),
                },
                history_path=root / "history.json",
            )
            agent.client = client
            agent.executor = TaskExecutor(
                NODE_ID,
                preparation_executor=ArtifactPreparationExecutor(
                    NODE_ID,
                    origin=ORIGIN,
                    model_preparer=preparer,
                    manifest_loader=lambda task_id: copy.deepcopy(MANIFEST),
                ),
            )

            with self.assertRaises(APIError):
                agent.once()
            failed_report_history = json.loads(
                (root / "history.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                failed_report_history["completed"][TASK_ID]["status"],
                "complete",
            )
            self.assertEqual(
                failed_report_history["pending_reports"][TASK_ID]["status"],
                "complete",
            )

            self.assertTrue(agent.once())

            completions = [
                request
                for request in client.requests
                if request[1].endswith("/complete")
            ]
            persisted = json.loads(
                (root / "history.json").read_text(encoding="utf-8")
            )
            self.assertEqual(len(preparer.calls), 1)
            self.assertEqual(client.claim_count, 1)
            self.assertEqual(len(completions), 2)
            self.assertEqual(completions[0][2], completions[1][2])
            self.assertEqual(agent.history["pending_reports"], {})
            self.assertEqual(persisted["pending_reports"], {})

    def test_agent_drops_terminally_rejected_pending_failure_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            preparer = FakeModelPreparer(exception=RuntimeError("failure"))
            client = FailingReportAgentClient(
                model_task(),
                [
                    APIError("temporary report transport failure"),
                    APIError("terminal report rejection", status_code=422),
                ],
            )
            agent = Agent(
                {
                    "server": "https://controller.example.test",
                    "node_id": NODE_ID,
                    "credential": "agent-secret",
                    "state_file": str(root / "state.json"),
                },
                history_path=root / "history.json",
            )
            agent.client = client
            agent.executor = TaskExecutor(
                NODE_ID,
                preparation_executor=ArtifactPreparationExecutor(
                    NODE_ID,
                    origin=ORIGIN,
                    model_preparer=preparer,
                    manifest_loader=lambda task_id: copy.deepcopy(MANIFEST),
                ),
            )

            with self.assertRaises(APIError):
                agent.once()
            self.assertIn(TASK_ID, agent.history["pending_reports"])
            self.assertTrue(agent.once())

            failures = [
                request
                for request in client.requests
                if request[1].endswith("/fail")
            ]
            self.assertEqual(len(preparer.calls), 1)
            self.assertEqual(client.claim_count, 1)
            self.assertEqual(len(failures), 2)
            self.assertEqual(agent.history["pending_reports"], {})

    def test_agent_redacts_unexpected_exception_and_replays_only_closed_code(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            secret = "https://private.example/token=top-secret"
            preparer = FakeModelPreparer(exception=RuntimeError(secret))
            client = FakeAgentClient(model_task())
            agent = Agent(
                {
                    "server": "https://controller.example.test",
                    "node_id": NODE_ID,
                    "credential": "agent-secret",
                    "state_file": str(root / "state.json"),
                },
                history_path=root / "history.json",
            )
            agent.client = client
            agent.executor = TaskExecutor(
                NODE_ID,
                preparation_executor=ArtifactPreparationExecutor(
                    NODE_ID,
                    origin=ORIGIN,
                    model_preparer=preparer,
                    manifest_loader=lambda task_id: copy.deepcopy(MANIFEST),
                ),
            )

            self.assertTrue(agent.once())
            self.assertTrue(agent.once())

            failures = [
                request for request in client.requests if request[1].endswith("/fail")
            ]
            self.assertEqual(len(failures), 2)
            self.assertEqual(
                failures[0][2], {"error": "PREPARATION_EXECUTION_FAILED"}
            )
            self.assertEqual(failures[0][2], failures[1][2])
            history = (root / "history.json").read_text(encoding="utf-8")
            self.assertNotIn(secret, history)
            self.assertTrue(
                set(item["error"] for item in (failure[2] for failure in failures))
                <= PREPARATION_FAILURE_CODES
            )


if __name__ == "__main__":
    unittest.main()
