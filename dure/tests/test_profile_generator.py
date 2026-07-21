from __future__ import annotations

import unittest

from dure.profile_generator import (
    AUTO_PROFILE_GENERATOR_VERSION,
    generate_auto_placement_profile_specs,
)
from dure.resource_pool import FLEET_MODEL_IDS


class AutoPlacementProfileGeneratorTests(unittest.TestCase):
    def test_exact_allowlist_generates_only_tp1_profiles(self):
        generated = {
            model_id: generate_auto_placement_profile_specs(model_id)
            for model_id in sorted(FLEET_MODEL_IDS)
        }

        self.assertEqual(set(generated), set(FLEET_MODEL_IDS))
        self.assertEqual(
            [spec.pipeline_parallel_size for spec in generated["qwen2.5-72b-awq"]],
            [1, 2, 3],
        )
        for model_id, specs in generated.items():
            with self.subTest(model_id=model_id):
                self.assertTrue(specs)
                self.assertTrue(
                    all(spec.tensor_parallel_size == 1 for spec in specs)
                )
                self.assertTrue(
                    all(
                        spec.node_count == spec.pipeline_parallel_size
                        for spec in specs
                    )
                )
                self.assertTrue(
                    all(spec.to_dict()["status"] == "DRAFT" for spec in specs)
                )
                self.assertTrue(
                    all(
                        spec.to_dict()["generator_version"]
                        == AUTO_PROFILE_GENERATOR_VERSION
                        for spec in specs
                    )
                )

    def test_generation_is_deterministic_and_rejects_other_models(self):
        first = generate_auto_placement_profile_specs("qwen2.5-72b-awq")
        second = generate_auto_placement_profile_specs("qwen2.5-72b-awq")

        self.assertEqual(first, second)
        self.assertEqual(
            [spec.spec_digest for spec in first],
            [spec.spec_digest for spec in second],
        )
        with self.assertRaisesRegex(ValueError, "allowlist"):
            generate_auto_placement_profile_specs("unknown-model")


if __name__ == "__main__":
    unittest.main()
