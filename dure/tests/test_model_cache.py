import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_MARKER_MAX_BYTES,
    MODEL_CACHE_SCHEMA_V1,
    MODEL_CACHE_SCHEMA_V2,
    MODEL_CACHE_SCHEMA_V3,
    MODEL_CACHE_VERIFICATION_VERSION,
    ModelCacheMarkerError,
    build_model_cache_marker,
    build_stage_model_cache_marker,
    decode_model_cache_marker,
    parse_model_cache_marker,
    read_model_cache_marker,
)
from dure.stage_cache import StageCacheIdentity, stage_contract_identity_digest


def marker_v1() -> dict[str, str]:
    return {
        "schema": MODEL_CACHE_SCHEMA_V1,
        "repository": "Example/Model-AWQ",
        "revision": "a" * 40,
        "manifest_digest": "sha256:" + "b" * 64,
        "quantization": "awq",
    }


def stage_identity(**changes) -> StageCacheIdentity:
    values = {
        "repository": "Example/Model-AWQ",
        "revision": "a" * 40,
        "manifest_digest": "sha256:" + "b" * 64,
        "quantization": "awq",
        "artifact_set_digest": "sha256:" + "c" * 64,
        "contract_identity_digest": stage_contract_identity_digest(
            source_manifest_digest="sha256:" + "e" * 64,
            runtime_image="registry.example/vllm@sha256:" + "f" * 64,
            vllm_version="0.9.0",
            exporter_build_digest="sha256:" + "1" * 64,
            architecture="Qwen2ForCausalLM",
            quantization="awq",
            tensor_parallel_size=1,
            pipeline_parallel_size=3,
            loader_format="VLLM_SHARDED_STATE_V1",
        ),
        "source_manifest_digest": "sha256:" + "e" * 64,
        "runtime_image": "registry.example/vllm@sha256:" + "f" * 64,
        "vllm_version": "0.9.0",
        "exporter_build_digest": "sha256:" + "1" * 64,
        "architecture": "Qwen2ForCausalLM",
        "loader_format": "VLLM_SHARDED_STATE_V1",
        "tensor_parallel_size": 1,
        "pipeline_parallel_size": 3,
        "pipeline_rank": 1,
        "tensor_rank": 0,
        "tensor_keys_digest": "sha256:" + "2" * 64,
    }
    values.update(changes)
    return StageCacheIdentity(**values)


class ModelCacheMarkerTests(unittest.TestCase):
    def test_v3_stage_marker_binds_the_complete_cache_identity(self):
        identity = stage_identity()

        value = build_stage_model_cache_marker(identity)
        parsed = decode_model_cache_marker(json.dumps(value))

        self.assertEqual(value["schema"], MODEL_CACHE_SCHEMA_V3)
        self.assertEqual(value["cache_kind"], MODEL_CACHE_KIND_STAGE)
        self.assertEqual(
            value["cache_identity_digest"], identity.cache_identity_digest
        )
        self.assertEqual(parsed.stage_identity(), identity)
        self.assertEqual(parsed.to_dict(), value)

    def test_v3_stage_marker_rejects_partial_tampered_and_boolean_identity(self):
        base = build_stage_model_cache_marker(stage_identity())
        invalid = []
        missing = dict(base)
        missing.pop("source_manifest_digest")
        invalid.append(missing)
        tampered = dict(base)
        tampered["pipeline_rank"] = 2
        invalid.append(tampered)
        boolean_rank = dict(base)
        boolean_rank["pipeline_rank"] = True
        invalid.append(boolean_rank)
        extra = dict(base)
        extra["path"] = "/tmp/model"
        invalid.append(extra)
        v2_shape = dict(base)
        v2_shape["schema"] = MODEL_CACHE_SCHEMA_V2
        invalid.append(v2_shape)

        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ModelCacheMarkerError):
                parse_model_cache_marker(value)

    def test_v1_marker_maps_to_full_snapshot_without_changing_wire_shape(self):
        value = marker_v1()

        parsed = parse_model_cache_marker(value)

        self.assertEqual(parsed.cache_kind, MODEL_CACHE_KIND_FULL_SNAPSHOT)
        self.assertEqual(
            parsed.verification_version, MODEL_CACHE_VERIFICATION_VERSION
        )
        self.assertEqual(parsed.to_dict(), value)

    def test_v2_generator_emits_closed_full_and_stage_markers(self):
        for cache_kind in (
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
            MODEL_CACHE_KIND_STAGE,
        ):
            with self.subTest(cache_kind=cache_kind):
                value = build_model_cache_marker(
                    repository="Example/Model-AWQ",
                    revision="a" * 40,
                    manifest_digest="sha256:" + "b" * 64,
                    quantization="awq",
                    cache_kind=cache_kind,
                )

                self.assertEqual(value["schema"], MODEL_CACHE_SCHEMA_V2)
                self.assertEqual(value["cache_kind"], cache_kind)
                self.assertEqual(
                    value["verification_version"],
                    MODEL_CACHE_VERIFICATION_VERSION,
                )
                self.assertEqual(
                    decode_model_cache_marker(json.dumps(value)).to_dict(), value
                )

    def test_unknown_partial_and_unsupported_v2_fields_are_rejected(self):
        base = build_model_cache_marker(
            repository="Example/Model-AWQ",
            revision="a" * 40,
            manifest_digest="sha256:" + "b" * 64,
            quantization="awq",
        )
        invalid_values = []
        with_unknown = dict(base)
        with_unknown["url"] = "https://secret.example/model"
        invalid_values.append(with_unknown)
        missing_kind = dict(base)
        missing_kind.pop("cache_kind")
        invalid_values.append(missing_kind)
        wrong_kind = dict(base)
        wrong_kind["cache_kind"] = "FULL"
        invalid_values.append(wrong_kind)
        wrong_version = dict(base)
        wrong_version["verification_version"] = 2
        invalid_values.append(wrong_version)
        boolean_version = dict(base)
        boolean_version["verification_version"] = True
        invalid_values.append(boolean_version)

        for value in invalid_values:
            with self.subTest(value=value), self.assertRaises(ModelCacheMarkerError):
                parse_model_cache_marker(value)

    def test_duplicate_json_keys_are_rejected(self):
        encoded = json.dumps(marker_v1())
        encoded = encoded[:-1] + ', "revision": "' + "c" * 40 + '"}'

        with self.assertRaises(ModelCacheMarkerError):
            decode_model_cache_marker(encoded)

    def test_deeply_nested_json_is_reduced_to_a_marker_error(self):
        encoded = "[" * 10_000 + "0" + "]" * 10_000

        with self.assertRaises(ModelCacheMarkerError):
            decode_model_cache_marker(encoded)

    def test_safe_reader_rejects_links_special_files_and_oversized_content(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            valid = root / "valid.json"
            valid.write_text(json.dumps(marker_v1()), encoding="utf-8")
            self.assertEqual(
                read_model_cache_marker(valid).to_dict(),
                marker_v1(),
            )

            symlink = root / "symlink.json"
            symlink.symlink_to(valid)
            with self.assertRaises(ModelCacheMarkerError):
                read_model_cache_marker(symlink)

            hardlink = root / "hardlink.json"
            os.link(valid, hardlink)
            with self.assertRaises(ModelCacheMarkerError):
                read_model_cache_marker(valid)

            oversized = root / "oversized.json"
            oversized.write_bytes(b"x" * (MODEL_CACHE_MARKER_MAX_BYTES + 1))
            with self.assertRaises(ModelCacheMarkerError):
                read_model_cache_marker(oversized)

            writable = root / "world-writable.json"
            writable.write_text(json.dumps(marker_v1()), encoding="utf-8")
            writable.chmod(0o666)
            with self.assertRaises(ModelCacheMarkerError):
                read_model_cache_marker(writable)

            foreign_owner = root / "foreign-owner.json"
            foreign_owner.write_text(json.dumps(marker_v1()), encoding="utf-8")
            with patch(
                "dure.model_cache.os.geteuid",
                return_value=foreign_owner.stat().st_uid + 1,
            ), self.assertRaises(ModelCacheMarkerError):
                read_model_cache_marker(foreign_owner)

            if hasattr(os, "mkfifo"):
                fifo = root / "marker.fifo"
                os.mkfifo(fifo)
                with self.assertRaises(ModelCacheMarkerError):
                    read_model_cache_marker(fifo)


if __name__ == "__main__":
    unittest.main()
