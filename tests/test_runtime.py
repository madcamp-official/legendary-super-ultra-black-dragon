import unittest

from dure.command import CommandResult
from dure.planner import build_plan
from dure.runtime import ContainerRuntime

from .helpers import FakeRunner, profile


class RuntimeTests(unittest.TestCase):
    def test_stop_filters_by_exact_dure_deployment_label(self):
        runner = FakeRunner(
            responses={
                ("docker", "ps", "-q", "--filter", "label=dure.deployment=deploy-1"): (0, "abc\ndef", ""),
                ("docker", "stop", "--time", "30", "abc", "def"): (0, "abc\ndef", ""),
            }
        )
        check = ContainerRuntime(runner).stop_deployment("deploy-1")
        self.assertTrue(check.ok)
        self.assertNotIn(("docker", "stop", "--time", "30"), runner.calls)

    def test_ray_container_uses_explicit_entrypoint_and_no_shell(self):
        node = profile("camp-7", address="192.168.0.228")
        plan = build_plan(
            [node],
            model_id="qwen2.5-32b-awq",
            image="registry.example/vllm@sha256:abc",
        )
        assert plan is not None
        inspect = (
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}",
            f"dure-ray-{plan.deployment_id}",
        )
        runner = FakeRunner(
            executables={"docker"},
            responses={inspect: CommandResult(inspect, 1, stderr="not found")},
        )

        result = ContainerRuntime(runner).start_ray(
            node, plan, plan.assignments[0], replace=False
        )

        self.assertTrue(result.ok)
        run = runner.calls[-1]
        self.assertEqual(run[0:3], ("docker", "run", "-d"))
        self.assertIn("dure.model=qwen2.5-32b-awq", run)
        entrypoint = run.index("--entrypoint")
        self.assertEqual(run[entrypoint + 1], "ray")
        image = run.index("registry.example/vllm@sha256:abc")
        self.assertEqual(run[image + 1 : image + 4], ("start", "--block", "--head"))

    def test_api_container_uses_vllm_entrypoint(self):
        node = profile("camp-7", address="192.168.0.228")
        plan = build_plan(
            [node],
            model_id="qwen2.5-32b-awq",
            image="registry.example/vllm@sha256:abc",
        )
        assert plan is not None
        name = f"dure-api-{plan.deployment_id}"
        inspect = ("docker", "inspect", "--format", "{{.State.Status}}", name)
        runner = FakeRunner(
            responses={inspect: CommandResult(inspect, 1, stderr="not found")}
        )

        result = ContainerRuntime(runner).start_api(
            plan, plan.assignments[0], replace=False
        )

        self.assertTrue(result.ok)
        run = runner.calls[-1]
        self.assertIn("dure.generation=1", run)
        self.assertIn("dure.model=qwen2.5-32b-awq", run)
        entrypoint = run.index("--entrypoint")
        self.assertEqual(run[entrypoint + 1], "vllm")
        image = run.index("registry.example/vllm@sha256:abc")
        self.assertEqual(run[image + 1 : image + 3], ("serve", "/models/model"))


if __name__ == "__main__":
    unittest.main()
