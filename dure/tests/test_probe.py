import json
import tempfile
import unittest
from pathlib import Path

from dure.command import CommandResult
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_VERIFICATION_VERSION,
    build_model_cache_marker,
    build_stage_model_cache_marker,
)
from dure.models import ArtifactCacheObservation, NodeProfile
from dure.probe import NodeProbe
from dure.stage_cache import (
    StageCacheIdentity,
    stage_cache_path,
    stage_contract_identity_digest,
)

from .helpers import FakeRunner


class ProbeTests(unittest.TestCase):
    def test_huggingface_config_links_are_limited_to_repository_blobs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            hub = root / "hub"
            hub.mkdir()
            revision = "a" * 40
            config = json.dumps(
                {
                    "model_type": "dure-test",
                    "quantization_config": {"quant_method": "awq"},
                }
            )

            allowed = hub / "models--Example--Allowed"
            allowed_blob = allowed / "blobs" / ("b" * 64)
            allowed_blob.parent.mkdir(parents=True)
            allowed_blob.write_text(config, encoding="utf-8")
            allowed_snapshot = allowed / "snapshots" / revision
            allowed_snapshot.mkdir(parents=True)
            (allowed_snapshot / "config.json").symlink_to(
                Path("../../blobs") / allowed_blob.name
            )

            outside_blob = root / "outside-config.json"
            outside_blob.write_text(config, encoding="utf-8")
            outside = hub / "models--Example--Outside"
            outside_snapshot = outside / "snapshots" / revision
            outside_snapshot.mkdir(parents=True)
            (outside_snapshot / "config.json").symlink_to(outside_blob)

            non_blob = hub / "models--Example--NonBlob"
            non_blob_config = non_blob / "metadata" / "config.json"
            non_blob_config.parent.mkdir(parents=True)
            non_blob_config.write_text(config, encoding="utf-8")
            non_blob_snapshot = non_blob / "snapshots" / revision
            non_blob_snapshot.mkdir(parents=True)
            (non_blob_snapshot / "config.json").symlink_to(
                Path("../../metadata/config.json")
            )

            result = NodeProbe(
                FakeRunner(),
                model_roots=[hub],
            ).collect()

        by_id = {item.model_id: item for item in result.installed_models}
        self.assertTrue(by_id["Example/Allowed"].complete)
        self.assertEqual(by_id["Example/Allowed"].quantization, "awq")
        self.assertFalse(by_id["Example/Outside"].complete)
        self.assertIsNone(by_id["Example/Outside"].quantization)
        self.assertFalse(by_id["Example/NonBlob"].complete)
        self.assertIsNone(by_id["Example/NonBlob"].quantization)

    def test_parses_nvidia_smi_and_runtime(self):
        gpu_query = (
            "nvidia-smi",
            "--query-gpu=index,name,uuid,driver_version,memory.total",
            "--format=csv,noheader,nounits",
        )
        cap_query = (
            "nvidia-smi",
            "--query-gpu=index,compute_cap",
            "--format=csv,noheader,nounits",
        )
        runner = FakeRunner(
            executables={"nvidia-smi", "docker", "ray"},
            responses={
                gpu_query: CommandResult(
                    gpu_query,
                    0,
                    "0, NVIDIA GeForce RTX 3090, GPU-123, 610.43.02, 24576",
                ),
                cap_query: CommandResult(cap_query, 0, "0, 8.6"),
                ("docker", "version"): CommandResult(("docker", "version"), 0, "ok"),
                ("docker", "info", "--format", "{{json .Runtimes}}"): CommandResult(
                    ("docker", "info"), 0, '{"runc":{},"nvidia":{}}'
                ),
                ("ray", "--version"): CommandResult(
                    ("ray", "--version"), 0, "ray, version 2.56.1"
                ),
            },
        )

        result = NodeProbe(runner).collect()

        self.assertEqual(len(result.gpus), 1)
        self.assertEqual(result.gpus[0].memory_mib, 24576)
        self.assertEqual(result.gpus[0].compute_capability, "8.6")
        self.assertTrue(result.runtime.engine_ready)
        self.assertTrue(result.runtime.nvidia_runtime)
        self.assertTrue(result.runtime.ray_available)

    def test_reports_missing_gpu(self):
        result = NodeProbe(FakeRunner()).collect()
        self.assertEqual(result.gpus, [])
        self.assertIn("No CUDA-capable NVIDIA GPU detected", result.issues)

    def test_network_probe_binds_addresses_to_the_default_interface(self):
        address_command = ("ip", "-j", "address", "show")
        route_command = ("ip", "-j", "route", "show", "default")
        runner = FakeRunner(
            executables={"ip"},
            responses={
                address_command: CommandResult(
                    address_command,
                    0,
                    json.dumps(
                        [
                            {
                                "ifname": "docker0",
                                "addr_info": [
                                    {"family": "inet", "local": "172.17.0.1"}
                                ],
                            },
                            {
                                "ifname": "ens3",
                                "addr_info": [
                                    {"family": "inet", "local": "10.0.0.12"}
                                ],
                            },
                        ]
                    ),
                ),
                route_command: CommandResult(
                    route_command, 0, json.dumps([{"dev": "ens3"}])
                ),
            },
        )

        network = NodeProbe(runner).collect().network

        self.assertEqual(network.default_interface, "ens3")
        self.assertEqual(network.addresses, ["172.17.0.1", "10.0.0.12"])
        self.assertEqual(network.default_interface_addresses, ["10.0.0.12"])

    def test_detects_installed_models_and_llm_workloads(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            model_path = model_root / "qwen-local"
            model_path.mkdir(parents=True)
            (model_path / "config.json").write_text(
                json.dumps(
                    {
                        "_name_or_path": "Qwen/Qwen2.5-14B-Instruct-AWQ",
                        "quantization_config": {"quant_method": "awq"},
                    }
                ),
                encoding="utf-8",
            )
            (model_path / ".dure-model.json").write_text(
                json.dumps(
                    {
                        "schema": "dure-model-cache-v1",
                        "repository": "Qwen/Qwen2.5-14B-Instruct-AWQ",
                        "revision": "a" * 40,
                        "manifest_digest": "sha256:" + "b" * 64,
                        "quantization": "awq",
                    }
                ),
                encoding="utf-8",
            )
            incomplete = model_root / "partial-model"
            incomplete.mkdir()
            containers = "\n".join(
                [
                    json.dumps(
                        {
                            "Names": "dure-api-deploy-1",
                            "Image": "registry/vllm@sha256:abc",
                            "Status": "Up 2 hours",
                            "Labels": "dure.deployment=deploy-1,dure.generation=2,dure.model=qwen2.5-14b-awq",
                        }
                    ),
                    json.dumps(
                        {
                            "Names": "unrelated-db",
                            "Image": "postgres:16",
                            "Status": "Up 2 hours",
                            "Labels": "",
                        }
                    ),
                ]
            )
            runner = FakeRunner(
                executables={"docker", "du"},
                responses={
                    ("docker", "version"): CommandResult(("docker", "version"), 0, "ok"),
                    ("docker", "info", "--format", "{{json .Runtimes}}"): CommandResult(
                        ("docker", "info"), 0, '{"nvidia":{}}'
                    ),
                    ("docker", "ps", "--all", "--format", "{{json .}}"): CommandResult(
                        ("docker", "ps"), 0, containers
                    ),
                    ("du", "-sm", "--", str(model_path)): CommandResult(
                        ("du", "-sm"), 0, f"10240\t{model_path}"
                    ),
                    ("du", "-sm", "--", str(incomplete)): CommandResult(
                        ("du", "-sm"), 0, f"100\t{incomplete}"
                    ),
                },
            )

            result = NodeProbe(runner, model_roots=[model_root]).collect()

        by_id = {item.model_id: item for item in result.installed_models}
        self.assertTrue(by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].complete)
        self.assertEqual(by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].quantization, "awq")
        self.assertEqual(
            by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].revision, "a" * 40
        )
        self.assertEqual(by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].size_mib, 10240)
        self.assertEqual(
            by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].manifest_digest,
            "sha256:" + "b" * 64,
        )
        self.assertEqual(
            by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].cache_kind,
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        self.assertEqual(
            by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].verification_version,
            MODEL_CACHE_VERIFICATION_VERSION,
        )
        self.assertFalse(by_id["partial-model"].complete)
        self.assertEqual(len(result.workloads), 1)
        self.assertEqual(result.workloads[0].deployment_id, "deploy-1")
        self.assertEqual(result.workloads[0].model_id, "qwen2.5-14b-awq")

    def test_old_profile_json_defaults_new_inventory_fields(self):
        value = NodeProbe(FakeRunner()).collect().to_dict()
        value.pop("installed_models")
        value.pop("workloads")
        value.pop("artifact_cache_observations")
        value.pop("artifact_cache_scan_complete")

        restored = NodeProfile.from_dict(value)

        self.assertEqual(restored.installed_models, [])
        self.assertEqual(restored.workloads, [])
        self.assertIsNone(restored.artifact_cache_observations)
        self.assertIsNone(restored.artifact_cache_scan_complete)

    def test_cache_observations_are_closed_metadata_only_and_scan_is_complete(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            cache_digest = "sha256:" + "b" * 64
            cache_path = model_root / ("sha256-" + "b" * 64)
            cache_path.mkdir(parents=True)
            (cache_path / ".dure-model.json").write_text(
                json.dumps(
                    build_model_cache_marker(
                        repository="Example/Full-AWQ",
                        revision="a" * 40,
                        manifest_digest=cache_digest,
                        quantization="awq",
                    )
                ),
                encoding="utf-8",
            )
            # Large payloads are deliberately not opened or hashed by the probe.
            (cache_path / "model.safetensors").write_bytes(b"not-hashed")

            result = NodeProbe(FakeRunner(), model_roots=[model_root]).collect()

        self.assertTrue(result.artifact_cache_scan_complete)
        self.assertEqual(len(result.artifact_cache_observations or []), 1)
        observation = result.artifact_cache_observations[0]
        self.assertEqual(observation.condition, "PRESENT")
        self.assertEqual(observation.cache_identity_digest, cache_digest)
        self.assertEqual(
            set(observation.to_dict()),
            {
                "cache_kind",
                "cache_identity_digest",
                "condition",
                "manifest_digest",
                "verification_version",
            },
        )
        self.assertNotIn("path", observation.to_dict())

    def test_cache_observation_distinguishes_unsafe_corrupt_and_mismatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            model_root.mkdir()
            unsafe = model_root / ("sha256-" + "1" * 64)
            unsafe.mkdir(mode=0o777)
            unsafe.chmod(0o777)
            corrupt = model_root / ("sha256-" + "2" * 64)
            corrupt.mkdir()
            (corrupt / ".dure-model.json").write_text("not-json", encoding="utf-8")
            mismatch = model_root / ("sha256-" + "3" * 64)
            mismatch.mkdir()
            (mismatch / ".dure-model.json").write_text(
                json.dumps(
                    build_model_cache_marker(
                        repository="Example/Full-AWQ",
                        revision="a" * 40,
                        manifest_digest="sha256:" + "4" * 64,
                        quantization="awq",
                    )
                ),
                encoding="utf-8",
            )

            result = NodeProbe(FakeRunner(), model_roots=[model_root]).collect()

        conditions = {
            item.cache_identity_digest: item.condition
            for item in result.artifact_cache_observations or []
        }
        self.assertEqual(conditions["sha256:" + "1" * 64], "UNSAFE")
        self.assertEqual(conditions["sha256:" + "2" * 64], "CORRUPT")
        self.assertEqual(
            conditions["sha256:" + "3" * 64], "IDENTITY_MISMATCH"
        )
        self.assertTrue(result.artifact_cache_scan_complete)

    def test_cache_scan_marks_truncated_inventory_incomplete(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            model_root.mkdir()
            stage_root = model_root / "stages"
            stage_root.mkdir()
            for number in range(129):
                (model_root / f"sha256-{number:064x}").mkdir()
            for number in range(128):
                (stage_root / f"sha256-{number + 129:064x}").mkdir()

            result = NodeProbe(
                FakeRunner(), model_roots=[model_root, stage_root]
            ).collect()

        self.assertFalse(result.artifact_cache_scan_complete)
        self.assertEqual(len(result.artifact_cache_observations or []), 256)

    def test_profile_rejects_more_than_256_valid_cache_observations(self):
        value = NodeProbe(FakeRunner()).collect().to_dict()
        value["artifact_cache_observations"] = [
            {
                "cache_kind": MODEL_CACHE_KIND_FULL_SNAPSHOT,
                "cache_identity_digest": f"sha256:{number:064x}",
                "condition": "UNSAFE",
            }
            for number in range(257)
        ]
        value["artifact_cache_scan_complete"] = False

        with self.assertRaisesRegex(
            ValueError,
            "artifact cache observations exceed the maximum allowed count",
        ):
            NodeProfile.from_dict(value)

    def test_profile_rejects_non_string_cache_observation_enums(self):
        value = NodeProbe(FakeRunner()).collect().to_dict()
        observation = {
            "cache_kind": MODEL_CACHE_KIND_FULL_SNAPSHOT,
            "cache_identity_digest": "sha256:" + "b" * 64,
            "condition": "UNSAFE",
        }
        value["artifact_cache_observations"] = [observation]
        value["artifact_cache_scan_complete"] = True

        for field in ("cache_kind", "condition"):
            with self.subTest(field=field):
                invalid = dict(value)
                invalid["artifact_cache_observations"] = [
                    {**observation, field: [observation[field]]}
                ]
                with self.assertRaisesRegex(
                    ValueError,
                    "artifact cache observation identity is invalid",
                ):
                    NodeProfile.from_dict(invalid)

    def test_profile_serialization_rejects_more_than_256_cache_observations(self):
        profile = NodeProbe(FakeRunner()).collect()
        profile.artifact_cache_observations = [
            ArtifactCacheObservation(
                cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
                cache_identity_digest=f"sha256:{number:064x}",
                condition="UNSAFE",
            )
            for number in range(257)
        ]
        profile.artifact_cache_scan_complete = False

        with self.assertRaisesRegex(
            ValueError,
            "artifact cache observations exceed the maximum allowed count",
        ):
            profile.to_dict()

    def test_v2_full_snapshot_and_stage_markers_are_distinct(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            for name, repository, cache_kind in (
                ("full", "Example/Full-AWQ", MODEL_CACHE_KIND_FULL_SNAPSHOT),
                ("stage", "Example/Stage-AWQ", MODEL_CACHE_KIND_STAGE),
            ):
                model_path = model_root / name
                model_path.mkdir(parents=True)
                (model_path / "config.json").write_text(
                    json.dumps(
                        {
                            "_name_or_path": repository,
                            "quantization_config": {"quant_method": "awq"},
                        }
                    ),
                    encoding="utf-8",
                )
                (model_path / ".dure-model.json").write_text(
                    json.dumps(
                        build_model_cache_marker(
                            repository=repository,
                            revision="a" * 40,
                            manifest_digest="sha256:" + "b" * 64,
                            quantization="awq",
                            cache_kind=cache_kind,
                        )
                    ),
                    encoding="utf-8",
                )

            result = NodeProbe(FakeRunner(), model_roots=[model_root]).collect()

        by_id = {item.model_id: item for item in result.installed_models}
        self.assertEqual(
            by_id["Example/Full-AWQ"].cache_kind,
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        self.assertEqual(
            by_id["Example/Stage-AWQ"].cache_kind,
            MODEL_CACHE_KIND_STAGE,
        )
        self.assertTrue(by_id["Example/Full-AWQ"].complete)
        self.assertFalse(by_id["Example/Stage-AWQ"].complete)
        for item in by_id.values():
            self.assertEqual(item.manifest_digest, "sha256:" + "b" * 64)
            self.assertEqual(
                item.verification_version, MODEL_CACHE_VERIFICATION_VERSION
            )

    def test_stage_cache_root_projects_the_closed_rank_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            runtime_image = "registry.example/vllm@sha256:" + "c" * 64
            source_manifest_digest = "sha256:" + "f" * 64
            exporter_build_digest = "sha256:" + "1" * 64
            identity = StageCacheIdentity(
                repository="Qwen/Test-AWQ",
                revision="a" * 40,
                manifest_digest="sha256:" + "2" * 64,
                quantization="awq",
                artifact_set_digest="sha256:" + "d" * 64,
                contract_identity_digest=stage_contract_identity_digest(
                    source_manifest_digest=source_manifest_digest,
                    runtime_image=runtime_image,
                    vllm_version="0.9.0",
                    exporter_build_digest=exporter_build_digest,
                    architecture="Qwen2ForCausalLM",
                    quantization="awq",
                    tensor_parallel_size=1,
                    pipeline_parallel_size=2,
                    loader_format="VLLM_SHARDED_STATE_V1",
                ),
                source_manifest_digest=source_manifest_digest,
                runtime_image=runtime_image,
                vllm_version="0.9.0",
                exporter_build_digest=exporter_build_digest,
                architecture="Qwen2ForCausalLM",
                loader_format="VLLM_SHARDED_STATE_V1",
                tensor_parallel_size=1,
                pipeline_parallel_size=2,
                pipeline_rank=0,
                tensor_rank=0,
                tensor_keys_digest="sha256:" + "4" * 64,
            )
            cache_path = stage_cache_path(identity, model_root=model_root)
            cache_path.mkdir(parents=True)
            (cache_path / "config.json").write_text(
                json.dumps(
                    {
                        "_name_or_path": identity.repository,
                        "quantization_config": {"quant_method": "awq"},
                    }
                ),
                encoding="utf-8",
            )
            (cache_path / ".dure-model.json").write_text(
                json.dumps(build_stage_model_cache_marker(identity)),
                encoding="utf-8",
            )

            result = NodeProbe(
                FakeRunner(), model_roots=[model_root / "stages"]
            ).collect()

        self.assertEqual(len(result.installed_models), 1)
        model = result.installed_models[0]
        self.assertEqual(model.model_id, identity.repository)
        self.assertEqual(model.path, str(cache_path))
        self.assertEqual(model.cache_kind, MODEL_CACHE_KIND_STAGE)
        self.assertFalse(model.complete)
        self.assertEqual(model.manifest_digest, identity.manifest_digest)
        self.assertEqual(
            model.artifact_set_digest, identity.artifact_set_digest
        )
        self.assertEqual(
            model.contract_identity_digest, identity.contract_identity_digest
        )
        self.assertEqual(
            model.source_manifest_digest, identity.source_manifest_digest
        )
        self.assertEqual(model.runtime_image, identity.runtime_image)
        self.assertEqual(model.vllm_version, identity.vllm_version)
        self.assertEqual(
            model.exporter_build_digest, identity.exporter_build_digest
        )
        self.assertEqual(model.architecture, identity.architecture)
        self.assertEqual(model.loader_format, identity.loader_format)
        self.assertEqual(
            model.tensor_parallel_size, identity.tensor_parallel_size
        )
        self.assertEqual(
            model.pipeline_parallel_size, identity.pipeline_parallel_size
        )
        self.assertEqual(model.pipeline_rank, identity.pipeline_rank)
        self.assertEqual(model.tensor_rank, identity.tensor_rank)
        self.assertEqual(model.tensor_keys_digest, identity.tensor_keys_digest)
        self.assertEqual(
            model.cache_identity_digest, identity.cache_identity_digest
        )

        restored = NodeProfile.from_dict(result.to_dict()).installed_models[0]
        self.assertEqual(restored, model)

    def test_old_installed_model_json_defaults_cache_identity_fields(self):
        value = NodeProbe(FakeRunner()).collect().to_dict()
        value["installed_models"] = [
            {
                "source": "dure",
                "model_id": "Example/Legacy-AWQ",
                "path": "/var/lib/dure/models/legacy",
                "revision": "a" * 40,
                "quantization": "awq",
                "size_mib": 1024,
                "complete": True,
            }
        ]

        restored = NodeProfile.from_dict(value)

        model = restored.installed_models[0]
        self.assertIsNone(model.manifest_digest)
        self.assertIsNone(model.cache_kind)
        self.assertIsNone(model.verification_version)


if __name__ == "__main__":
    unittest.main()
