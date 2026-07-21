from __future__ import annotations

import asyncio
import importlib.machinery
import importlib.util
import os
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


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
                "2",
                "--request-count",
                "20",
                "--duration-seconds",
                "240",
                "--output-format",
                "json-summary-v1",
            ]
        )

        with self.assertRaisesRegex(ValueError, "dimensions"):
            asyncio.run(module._run(args))

    def test_vllm_receives_tokens_prompt_mapping(self):
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
                "1024",
                "--output-tokens",
                "256",
                "--concurrency",
                "1",
                "--warmup-requests",
                "2",
                "--request-count",
                "20",
                "--duration-seconds",
                "240",
                "--output-format",
                "json-summary-v1",
            ]
        )
        observed_prompts = []

        class FakeTokenizer:
            def encode(self, _text, *, add_special_tokens):
                self.assert_false(add_special_tokens)
                return [1]

            def apply_chat_template(self, _messages, *, tokenize, add_generation_prompt):
                if not tokenize or not add_generation_prompt:
                    raise AssertionError("chat template flags changed")
                return [2, 3]

            @staticmethod
            def assert_false(value):
                if value:
                    raise AssertionError("special tokens must remain disabled")

        class FakeAutoTokenizer:
            @staticmethod
            def from_pretrained(_model, *, local_files_only, trust_remote_code):
                if not local_files_only or trust_remote_code:
                    raise AssertionError("tokenizer trust boundary changed")
                return FakeTokenizer()

        class FakeAsyncEngineArgs:
            def __init__(self, **_kwargs):
                pass

        class FakeAsyncLLMEngine:
            @classmethod
            def from_engine_args(cls, _args):
                return cls()

            def generate(self, prompt, _sampling, _request_id):
                observed_prompts.append(prompt)

                async def stream():
                    yield SimpleNamespace(
                        outputs=[SimpleNamespace(token_ids=[1] * 256, text="")]
                    )

                return stream()

        class FakeSamplingParams:
            def __init__(self, **_kwargs):
                pass

        transformers = types.ModuleType("transformers")
        transformers.AutoTokenizer = FakeAutoTokenizer
        vllm = types.ModuleType("vllm")
        vllm.AsyncEngineArgs = FakeAsyncEngineArgs
        vllm.AsyncLLMEngine = FakeAsyncLLMEngine
        vllm.SamplingParams = FakeSamplingParams

        sleep = mock.AsyncMock()
        with (
            mock.patch.dict(
                sys.modules,
                {"transformers": transformers, "vllm": vllm},
            ),
            mock.patch.object(module, "_gpu_headroom", return_value=50.0),
            mock.patch.object(module.asyncio, "sleep", new=sleep),
        ):
            asyncio.run(module._run(args))

        self.assertEqual(len(observed_prompts), 22)
        sleep.assert_not_awaited()
        self.assertTrue(
            all(
                set(prompt) == {"prompt_token_ids"}
                and len(prompt["prompt_token_ids"]) == 1024
                for prompt in observed_prompts
            )
        )


if __name__ == "__main__":
    unittest.main()
