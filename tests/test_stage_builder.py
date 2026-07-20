from __future__ import annotations

import hashlib
import json
import os
import subprocess
import struct
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import dure.stage_artifact as stage_artifact_module
from dure.artifact_manifest import parse_artifact_manifest
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_SCHEMA_V2,
    MODEL_CACHE_VERIFICATION_VERSION,
)
from dure.stage_artifact import (
    DEFAULT_MAX_PART_BYTES,
    STAGE_MARKER_FILE,
    STAGE_SET_INDEX_FILE,
    StageArtifactError,
    StageExportContract,
    TensorSpec,
    TrustedStageBuilder,
    WorkerStageExport,
    _vllm_worker_export,
    verify_stage_artifact_set,
)


EXPORTER_DIGEST = "sha256:" + "2" * 64
RUNTIME_IMAGE = "registry.example.com/dure/vllm@sha256:" + "3" * 64
STATIC_SOURCE_DIGEST = "sha256:" + "1" * 64


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _write_safetensors(path: Path, tensors: tuple[TensorSpec, ...], seed: int) -> None:
    dtype_bytes = {"F16": 2, "I32": 4, "F32": 4}
    header: dict[str, object] = {}
    body = bytearray()
    for index, tensor in enumerate(sorted(tensors, key=lambda item: item.name)):
        elements = 1
        for dimension in tensor.shape:
            elements *= dimension
        size = elements * dtype_bytes[tensor.dtype]
        start = len(body)
        body.extend(bytes([(seed + index) % 251 + 1]) * size)
        header[tensor.name] = {
            "dtype": tensor.dtype,
            "shape": list(tensor.shape),
            "data_offsets": [start, len(body)],
        }
    encoded = _canonical(header)
    padded_length = (len(encoded) + 7) // 8 * 8
    encoded += b" " * (padded_length - len(encoded))
    path.write_bytes(struct.pack("<Q", len(encoded)) + encoded + body)


def _source_manifest(root: Path) -> tuple[dict, str]:
    files: list[dict[str, object]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == ".dure-model.json":
            continue
        payload = path.read_bytes()
        digest = "sha256:" + hashlib.sha256(payload).hexdigest()
        chunks = []
        if payload:
            chunks.append(
                {
                    "ordinal": 0,
                    "offset_bytes": 0,
                    "length_bytes": len(payload),
                    "sha256": digest,
                }
            )
        files.append(
            {
                "path": path.relative_to(root).as_posix(),
                "kind": "REGULAR",
                "size_bytes": len(payload),
                "sha256": digest,
                "chunks": chunks,
            }
        )
    parsed = parse_artifact_manifest({"schema_version": 1, "files": files})
    return parsed.document, parsed.digest


def _refresh_source_manifest(root: Path) -> tuple[dict, str]:
    manifest, digest = _source_manifest(root)
    marker = {
        "schema": MODEL_CACHE_SCHEMA_V2,
        "repository": "Qwen/Qwen2.5-7B-Instruct-AWQ",
        "revision": "4" * 40,
        "manifest_digest": digest,
        "quantization": "awq",
        "cache_kind": MODEL_CACHE_KIND_FULL_SNAPSHOT,
        "verification_version": MODEL_CACHE_VERIFICATION_VERSION,
    }
    (root / ".dure-model.json").write_bytes(_canonical(marker))
    return manifest, digest


def _write_source(
    root: Path,
    *,
    config_updates: dict | None = None,
) -> tuple[dict, str]:
    root.mkdir()
    config = {
        "architectures": ["Qwen2ForCausalLM"],
        "model_type": "qwen2",
        "hidden_size": 8,
        "num_hidden_layers": 2,
        "quantization_config": {
            "bits": 4,
            "group_size": 128,
            "quant_method": "awq",
        },
    }
    config.update(config_updates or {})
    (root / "config.json").write_bytes(_canonical(config))
    (root / "tokenizer.json").write_bytes(_canonical({"version": "1.0"}))
    (root / "tokenizer_config.json").write_bytes(
        _canonical({"tokenizer_class": "Qwen2TokenizerFast"})
    )
    (root / "generation_config.json").write_bytes(_canonical({"do_sample": False}))
    # Source weights are read only by the real vLLM adapter. Their exact content
    # is deliberately irrelevant to the synthetic artifact writer, but their
    # identity remains covered by the canonical source manifest.
    (root / "model.safetensors").write_bytes(b"trusted-source-fixture")
    return _refresh_source_manifest(root)


def _contract(source_digest: str, pp: int = 2, **updates) -> StageExportContract:
    values = {
        "source_manifest_digest": source_digest,
        "runtime_image": RUNTIME_IMAGE,
        "exporter_build_digest": EXPORTER_DIGEST,
        "pipeline_parallel_size": pp,
    }
    values.update(updates)
    return StageExportContract(**values)


STAGE_TENSORS = {
    0: (
        TensorSpec("model.embed_tokens.weight", "F16", (2, 2)),
        TensorSpec("model.layers.0.self_attn.q_proj.qweight", "I32", (2, 2)),
    ),
    1: (
        TensorSpec("lm_head.weight", "F16", (2, 2)),
        TensorSpec("model.layers.1.self_attn.q_proj.qweight", "I32", (2, 2)),
    ),
}


class SyntheticNativeExporter:
    def __init__(
        self,
        *,
        missing_rank: int | None = None,
        unexpected_file: bool = False,
        wrong_coverage: bool = False,
        missing_part: bool = False,
    ) -> None:
        self.missing_rank = missing_rank
        self.unexpected_file = unexpected_file
        self.wrong_coverage = wrong_coverage
        self.missing_part = missing_part
        self.calls = 0

    def export(self, source: Path, workspace: Path, contract: StageExportContract):
        self.calls += 1
        results = []
        for rank in range(contract.pipeline_parallel_size):
            if rank == self.missing_rank:
                continue
            tensors = STAGE_TENSORS[rank]
            first = (tensors[0],)
            second = (tensors[1],)
            stage = workspace / "stages" / str(rank)
            _write_safetensors(stage / "model-rank-0-part-0.safetensors", first, rank * 10)
            part = 2 if self.missing_part and rank == 0 else 1
            _write_safetensors(
                stage / f"model-rank-0-part-{part}.safetensors", second, rank * 10 + 1
            )
            if self.unexpected_file and rank == 0:
                (stage / "arbitrary.bin").write_bytes(b"not allowed")
            expected = tensors
            if self.wrong_coverage and rank == 0:
                expected = tensors + (TensorSpec("model.layers.999.weight", "F16", (1,)),)
            results.append(WorkerStageExport(rank, tuple(sorted(expected, key=lambda item: item.name))))
        return results


class StageBuilderTests(unittest.TestCase):
    def test_gpu_acceptance_requires_explicit_export_and_load_opt_in(self):
        script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "acceptance-vllm-stage-builder.py"
        )
        cases = ({}, {"DURE_RUN_STAGE_GPU_ACCEPTANCE": "1"})
        for updates in cases:
            with self.subTest(updates=updates):
                environment = os.environ.copy()
                environment.pop("DURE_RUN_STAGE_GPU_ACCEPTANCE", None)
                environment.pop("DURE_STAGE_ACCEPTANCE_LOAD", None)
                environment.update(updates)
                completed = subprocess.run(
                    [sys.executable, str(script)],
                    check=False,
                    capture_output=True,
                    text=True,
                    env=environment,
                )
                self.assertEqual(completed.returncode, 77)
                self.assertEqual(completed.stderr, "")
                report = json.loads(completed.stdout)
                self.assertEqual(report["status"], "NOT_RUN")

    def test_synthetic_stage_set_is_deterministic_and_has_exact_coverage(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source_manifest, source_digest = _write_source(source)
            contract = _contract(source_digest)
            first = TrustedStageBuilder(SyntheticNativeExporter()).build(
                source, root / "first", contract, source_manifest
            )
            second = TrustedStageBuilder(SyntheticNativeExporter()).build(
                source, root / "second", contract, source_manifest
            )

            self.assertEqual(first.index_digest, second.index_digest)
            self.assertEqual(first.index, second.index)
            self.assertEqual(
                [item.artifact_manifest_digest for item in first.stages],
                [item.artifact_manifest_digest for item in second.stages],
            )
            self.assertEqual([item.rank for item in first.stages], [0, 1])
            self.assertEqual(first.stages[0].tensor_keys, tuple(item.name for item in STAGE_TENSORS[0]))
            for rank in range(2):
                stage = first.root / "stages" / str(rank)
                self.assertTrue((stage / STAGE_MARKER_FILE).is_file())
                self.assertTrue((stage / "config.json").is_file())
                self.assertTrue((stage / "tokenizer.json").is_file())
                paths = {item["path"] for item in first.stages[rank].artifact_manifest["files"]}
                self.assertIn(STAGE_MARKER_FILE, paths)
                self.assertIn("model-rank-0-part-0.safetensors", paths)
            verified = verify_stage_artifact_set(
                first.root,
                expected_contract=contract,
                expected_index_digest=first.index_digest,
            )
            self.assertEqual(verified.to_dict(), first.to_dict())

    def test_registration_payload_has_closed_control_shape(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source_manifest, source_digest = _write_source(source)
            built = TrustedStageBuilder(SyntheticNativeExporter()).build(
                source,
                root / "artifact",
                _contract(source_digest),
                source_manifest,
            )

            payload = built.registration_payload()
            self.assertEqual(
                set(payload),
                {
                    "source_manifest_digest",
                    "runtime_image",
                    "vllm_version",
                    "exporter_build_digest",
                    "architecture",
                    "quantization",
                    "tensor_parallel_size",
                    "pipeline_parallel_size",
                    "loader_format",
                    "stages",
                },
            )
            self.assertEqual(built.contract.loader_format, "sharded_state")
            self.assertEqual(payload["loader_format"], "VLLM_SHARDED_STATE_V1")
            self.assertEqual(len(payload["stages"]), 2)
            for rank, stage in enumerate(payload["stages"]):
                self.assertEqual(
                    set(stage),
                    {
                        "pipeline_rank",
                        "tensor_rank",
                        "manifest_digest",
                        "tensor_key_count",
                        "tensor_keys_digest",
                        "weight_size_bytes",
                        "manifest",
                    },
                )
                self.assertEqual(stage["pipeline_rank"], rank)
                self.assertEqual(stage["tensor_rank"], 0)
                expected_weight_bytes = sum(
                    item["size_bytes"]
                    for item in stage["manifest"]["files"]
                    if item["path"].startswith("model-rank-0-part-")
                )
                self.assertEqual(stage["weight_size_bytes"], expected_weight_bytes)

    def test_source_manifest_rejects_tamper_and_extra_files_before_export(self):
        cases = (("tamper", "STAGE_DIGEST_MISMATCH"), ("extra", "STAGE_SOURCE_UNSAFE"))
        for change, expected_code in cases:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / "source"
                source_manifest, source_digest = _write_source(source)
                if change == "tamper":
                    (source / "model.safetensors").write_bytes(b"tampered-source")
                else:
                    (source / "unexpected.bin").write_bytes(b"not-in-manifest")
                exporter = SyntheticNativeExporter()

                with self.assertRaises(StageArtifactError) as caught:
                    TrustedStageBuilder(exporter).build(
                        source,
                        root / "artifact",
                        _contract(source_digest),
                        source_manifest,
                    )

                self.assertEqual(caught.exception.code, expected_code)
                self.assertEqual(exporter.calls, 0)
                self.assertFalse((root / "artifact").exists())

    def test_rank_missing_wrong_file_part_gap_and_tensor_coverage_fail_closed(self):
        cases = (
            (SyntheticNativeExporter(missing_rank=1), "STAGE_RANK_SET_INVALID"),
            (SyntheticNativeExporter(unexpected_file=True), "STAGE_FILE_SET_INVALID"),
            (SyntheticNativeExporter(missing_part=True), "STAGE_FILE_SET_INVALID"),
            (SyntheticNativeExporter(wrong_coverage=True), "STAGE_TENSOR_COVERAGE_INVALID"),
        )
        for index, (exporter, expected_code) in enumerate(cases):
            with self.subTest(expected_code=expected_code), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / "source"
                source_manifest, source_digest = _write_source(source)
                with self.assertRaises(StageArtifactError) as caught:
                    TrustedStageBuilder(exporter).build(
                        source,
                        root / f"out-{index}",
                        _contract(source_digest),
                        source_manifest,
                    )
                self.assertEqual(caught.exception.code, expected_code)

    def test_weight_digest_tampering_is_detected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source_manifest, source_digest = _write_source(source)
            built = TrustedStageBuilder(SyntheticNativeExporter()).build(
                source,
                root / "artifact",
                _contract(source_digest),
                source_manifest,
            )
            weight = built.root / "stages" / "0" / "model-rank-0-part-0.safetensors"
            payload = bytearray(weight.read_bytes())
            payload[-1] ^= 0x01
            weight.write_bytes(payload)

            with self.assertRaises(StageArtifactError) as caught:
                verify_stage_artifact_set(
                    built.root,
                    expected_index_digest=built.index_digest,
                )
            self.assertEqual(caught.exception.code, "STAGE_DIGEST_MISMATCH")

    def test_symlink_and_hardlink_stage_files_are_rejected(self):
        for link_kind in ("symlink", "hardlink"):
            with self.subTest(link_kind=link_kind), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / "source"
                source_manifest, source_digest = _write_source(source)
                built = TrustedStageBuilder(SyntheticNativeExporter()).build(
                    source,
                    root / "artifact",
                    _contract(source_digest),
                    source_manifest,
                )
                stage = built.root / "stages" / "0"
                config = stage / "config.json"
                config.unlink()
                if link_kind == "symlink":
                    os.symlink("tokenizer.json", config)
                else:
                    os.link(stage / "tokenizer.json", config)

                with self.assertRaises(StageArtifactError) as caught:
                    verify_stage_artifact_set(
                        built.root,
                        expected_index_digest=built.index_digest,
                    )
                self.assertEqual(caught.exception.code, "STAGE_FILE_SET_INVALID")

    def test_atomic_publish_failure_never_creates_or_replaces_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source_manifest, source_digest = _write_source(source)
            output = root / "artifact"
            failure = StageArtifactError(
                "STAGE_ATOMIC_PUBLISH_UNAVAILABLE", "injected no-replace failure"
            )
            with mock.patch.object(
                stage_artifact_module,
                "_publish_noreplace",
                side_effect=failure,
            ):
                with self.assertRaises(StageArtifactError) as caught:
                    TrustedStageBuilder(SyntheticNativeExporter()).build(
                        source,
                        output,
                        _contract(source_digest),
                        source_manifest,
                    )
            self.assertEqual(
                caught.exception.code, "STAGE_ATOMIC_PUBLISH_UNAVAILABLE"
            )
            self.assertFalse(output.exists())
            self.assertTrue(list(root.glob(".artifact.building-*")))

            output.mkdir()
            sentinel = output / "sentinel"
            sentinel.write_bytes(b"preserve")
            with self.assertRaises(StageArtifactError) as exists:
                TrustedStageBuilder(SyntheticNativeExporter()).build(
                    source,
                    output,
                    _contract(source_digest),
                    source_manifest,
                )
            self.assertEqual(exists.exception.code, "STAGE_TARGET_EXISTS")
            self.assertEqual(sentinel.read_bytes(), b"preserve")

    def test_atomic_publish_fsyncs_exported_weights_and_parent_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            source = root / "source"
            source_manifest, source_digest = _write_source(source)
            synchronized: list[str] = []
            real_fsync = os.fsync

            def record_fsync(descriptor: int) -> None:
                try:
                    synchronized.append(os.readlink(f"/proc/self/fd/{descriptor}"))
                except OSError:
                    synchronized.append("")
                real_fsync(descriptor)

            with mock.patch.object(
                stage_artifact_module.os,
                "fsync",
                side_effect=record_fsync,
            ):
                built = TrustedStageBuilder(SyntheticNativeExporter()).build(
                    source,
                    root / "artifact",
                    _contract(source_digest),
                    source_manifest,
                )

            self.assertTrue(built.root.is_dir())
            self.assertTrue(
                any(
                    value.endswith("model-rank-0-part-0.safetensors")
                    for value in synchronized
                )
            )
            self.assertGreaterEqual(
                sum(value == str(root) for value in synchronized),
                2,
            )

    def test_tree_scan_or_weight_fsync_failure_never_publishes(self):
        for failure_kind in ("walk", "weight-fsync"):
            with self.subTest(failure_kind=failure_kind), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                source = root / "source"
                source_manifest, source_digest = _write_source(source)
                output = root / "artifact"
                real_fsync = os.fsync

                def fail_weight_fsync(descriptor: int) -> None:
                    try:
                        path = os.readlink(f"/proc/self/fd/{descriptor}")
                    except OSError:
                        path = ""
                    if path.endswith("model-rank-0-part-0.safetensors"):
                        raise OSError("injected weight fsync failure")
                    real_fsync(descriptor)

                if failure_kind == "walk":
                    failure = mock.patch.object(
                        stage_artifact_module.os,
                        "walk",
                        side_effect=OSError("injected tree scan failure"),
                    )
                else:
                    failure = mock.patch.object(
                        stage_artifact_module.os,
                        "fsync",
                        side_effect=fail_weight_fsync,
                    )
                with failure, mock.patch.object(
                    stage_artifact_module,
                    "_publish_noreplace",
                    wraps=stage_artifact_module._publish_noreplace,
                ) as publish:
                    with self.assertRaises(StageArtifactError) as caught:
                        TrustedStageBuilder(SyntheticNativeExporter()).build(
                            source,
                            output,
                            _contract(source_digest),
                            source_manifest,
                        )

                self.assertEqual(caught.exception.code, "STAGE_IO_FAILED")
                publish.assert_not_called()
                self.assertFalse(output.exists())
                self.assertTrue(list(root.glob(".artifact.building-*")))

    def test_contract_and_source_reject_remote_code_lora_moe_multimodal_and_architecture(self):
        for field in ("trust_remote_code", "enable_lora", "is_moe", "is_multimodal"):
            with self.subTest(contract_flag=field):
                with self.assertRaises(StageArtifactError) as caught:
                    _contract(STATIC_SOURCE_DIGEST, **{field: True})
                self.assertEqual(caught.exception.code, "STAGE_CONTRACT_REJECTED")
        with self.assertRaises(StageArtifactError):
            _contract(STATIC_SOURCE_DIGEST, tensor_parallel_size=2)
        with self.assertRaises(StageArtifactError):
            _contract(STATIC_SOURCE_DIGEST, pp=65)

        source_cases = (
            ({"architectures": ["LlamaForCausalLM"]}, None),
            ({"auto_map": {"AutoModel": "modeling_custom.Custom"}}, "modeling_custom.py"),
            ({"num_experts": 8}, None),
            ({"vision_config": {}}, None),
        )
        for index, (updates, extra_name) in enumerate(source_cases):
            with self.subTest(source_case=index), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / "source"
                source_manifest, source_digest = _write_source(
                    source,
                    config_updates=updates,
                )
                if extra_name:
                    (source / extra_name).write_text("raise RuntimeError()", encoding="utf-8")
                    source_manifest, source_digest = _refresh_source_manifest(source)
                with self.assertRaises(StageArtifactError) as caught:
                    TrustedStageBuilder(SyntheticNativeExporter()).build(
                        source,
                        root / "artifact",
                        _contract(source_digest),
                        source_manifest,
                    )
                self.assertEqual(caught.exception.code, "STAGE_SOURCE_UNSUPPORTED")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            _write_source(source)
            (source / "adapter_config.json").write_bytes(_canonical({"peft_type": "LORA"}))
            source_manifest, source_digest = _refresh_source_manifest(source)
            with self.assertRaises(StageArtifactError) as caught:
                TrustedStageBuilder(SyntheticNativeExporter()).build(
                    source,
                    root / "artifact",
                    _contract(source_digest),
                    source_manifest,
                )
            self.assertEqual(caught.exception.code, "STAGE_SOURCE_UNSUPPORTED")

    def test_worker_adapter_uses_pipeline_rank_directory_and_native_save_model(self):
        with tempfile.TemporaryDirectory() as temporary:
            stages = Path(temporary) / "stages"
            (stages / "0").mkdir(parents=True)
            (stages / "1").mkdir()
            calls: list[tuple[object, str, object, int]] = []

            class Group:
                rank_in_group = 1
                world_size = 2

            class Tensor:
                dtype = "torch.float16"
                shape = (2, 2)

            class Model:
                def state_dict(self):
                    return {"model.layers.1.weight": Tensor()}

            class Worker:
                class ModelRunner:
                    model = Model()

                model_runner = ModelRunner()

            class FakeLoader:
                @staticmethod
                def _filter_subtensors(value):
                    return value

                @staticmethod
                def save_model(model, path, pattern=None, max_size=None):
                    calls.append((model, path, pattern, max_size))

            vllm = types.ModuleType("vllm")
            distributed = types.ModuleType("vllm.distributed")
            distributed.get_pp_group = lambda: Group()
            distributed.get_tensor_model_parallel_rank = lambda: 0
            distributed.get_tensor_model_parallel_world_size = lambda: 1
            model_executor = types.ModuleType("vllm.model_executor")
            model_loader = types.ModuleType("vllm.model_executor.model_loader")
            loader_module = types.ModuleType(
                "vllm.model_executor.model_loader.sharded_state_loader"
            )
            loader_module.ShardedStateLoader = FakeLoader
            modules = {
                "vllm": vllm,
                "vllm.distributed": distributed,
                "vllm.model_executor": model_executor,
                "vllm.model_executor.model_loader": model_loader,
                "vllm.model_executor.model_loader.sharded_state_loader": loader_module,
            }
            with mock.patch.dict(sys.modules, modules):
                result = _vllm_worker_export(
                    Worker(),
                    stages_root=str(stages),
                    expected_pipeline_size=2,
                    max_part_bytes=DEFAULT_MAX_PART_BYTES,
                )

            self.assertEqual(result["rank"], 1)
            self.assertEqual(result["tensors"][0]["name"], "model.layers.1.weight")
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][1], str(stages / "1"))
            self.assertIsNone(calls[0][2])
            self.assertEqual(calls[0][3], DEFAULT_MAX_PART_BYTES)


if __name__ == "__main__":
    unittest.main()
