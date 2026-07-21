from __future__ import annotations

import asyncio
import importlib.machinery
import importlib.util
import os
import unittest
from pathlib import Path


ENTRYPOINT = Path(__file__).resolve().parents[1] / "packaging" / "dure-benchmark"


def _load_entrypoint():
    loader = importlib.machinery.SourceFileLoader(
        "dure_packaged_benchmark", str(ENTRYPOINT)
    )
    spec = importlib.util.spec_from_loader("dure_packaged_benchmark", loader)
    if spec is None or spec.loader is None:
        raise RuntimeError("benchmark entrypoint cannot be imported")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PackagedBenchmarkEntrypointTests(unittest.TestCase):
    def test_entrypoint_is_small_executable_and_not_writable_by_group_or_other(self):
        metadata = ENTRYPOINT.stat()

        self.assertTrue(os.access(ENTRYPOINT, os.X_OK))
        self.assertLess(metadata.st_size, 64 * 1024)
        self.assertEqual(metadata.st_mode & 0o022, 0)

    def test_invalid_fixed_dimensions_fail_before_heavy_runtime_imports(self):
        module = _load_entrypoint()
        args = module._parser().parse_args(
            [
                "run",
                "--suite",
                "dure-serving-slo-v1",
                "--workload",
                "quality-eval",
                "--model",
                "/models/model",
                "--artifact-revision",
                "a" * 40,
                "--artifact-manifest-digest",
                "sha256:" + "b" * 64,
                "--quantization",
                "awq",
                "--input-tokens",
                "1023",
                "--output-tokens",
                "256",
                "--concurrency",
                "1",
                "--warmup-requests",
                "20",
                "--request-count",
                "200",
                "--duration-seconds",
                "900",
                "--output-format",
                "json-summary-v1",
            ]
        )

        with self.assertRaisesRegex(ValueError, "dimensions"):
            asyncio.run(module._run(args))


if __name__ == "__main__":
    unittest.main()
