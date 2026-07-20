import json
import unittest

from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_SCHEMA_V1,
    MODEL_CACHE_SCHEMA_V2,
    MODEL_CACHE_VERIFICATION_VERSION,
    ModelCacheMarkerError,
    build_model_cache_marker,
    decode_model_cache_marker,
    parse_model_cache_marker,
)


def marker_v1() -> dict[str, str]:
    return {
        "schema": MODEL_CACHE_SCHEMA_V1,
        "repository": "Example/Model-AWQ",
        "revision": "a" * 40,
        "manifest_digest": "sha256:" + "b" * 64,
        "quantization": "awq",
    }


class ModelCacheMarkerTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
