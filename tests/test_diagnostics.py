from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from dure.diagnostics import CodexDiagnoser, refresh_node_profiles, select_inventory_nodes


def diagnosis_result() -> dict:
    return {
        "summary": "Use one GPU node and one utility node.",
        "confidence": "medium",
        "assumptions": ["No network benchmark is available."],
        "node_assessments": [
            {
                "node_id": "gpu-1",
                "hostname": "gpu-host",
                "recommended_role": "ray-head",
                "usable_now": True,
                "gpu_capacity_gib": 24,
                "existing_assets": [],
                "blockers": [],
                "notes": [],
            }
        ],
        "deployment_recommendations": [],
        "cpu_recommendations": [],
        "existing_model_findings": [],
        "warnings": [],
        "next_steps": ["Benchmark the network."],
    }


class FakeAdminClient:
    def __init__(self):
        self.requests = []

    def request(self, method, path, payload=None):
        self.requests.append((method, path, payload))
        if path == "/v1/admin/tasks":
            return {
                "tasks": [{"id": "task-1", "node_id": "gpu-1"}],
                "errors": {"offline": "not online"},
            }
        return {
            "task": {
                "id": "task-1",
                "node_id": "gpu-1",
                "status": "SUCCEEDED",
                "error": None,
            }
        }


class FakeProcessRunner:
    def __init__(self, *, logged_in=True):
        self.logged_in = logged_in
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if argv[1:3] == ["login", "status"]:
            return subprocess.CompletedProcess(
                argv,
                0 if self.logged_in else 1,
                stdout="Logged in" if self.logged_in else "",
                stderr="" if self.logged_in else "Not logged in",
            )
        output = Path(argv[argv.index("--output-last-message") + 1])
        output.write_text(json.dumps(diagnosis_result()), encoding="utf-8")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")


class DiagnosticsTests(unittest.TestCase):
    def test_inventory_selection_excludes_pending_and_rejects_unknown(self):
        inventory = {
            "generated_at": "now",
            "nodes": [
                {"id": "gpu-1", "approved": True},
                {"id": "pending", "approved": False},
            ],
        }

        selected = select_inventory_nodes(inventory)
        self.assertEqual([item["id"] for item in selected["nodes"]], ["gpu-1"])
        with self.assertRaisesRegex(ValueError, "pending"):
            select_inventory_nodes(inventory, ["pending"])

    def test_refresh_submits_closed_probe_task_and_waits_for_completion(self):
        client = FakeAdminClient()

        result = refresh_node_profiles(client, ["gpu-1"], timeout=5, poll_interval=0)

        self.assertEqual(result["tasks"][0]["status"], "SUCCEEDED")
        self.assertEqual(result["errors"], {"offline": "not online"})
        self.assertEqual(
            client.requests[0],
            (
                "POST",
                "/v1/admin/tasks",
                {
                    "node_ids": ["gpu-1"],
                    "type": "PROBE",
                    "deployment_id": None,
                    "options": {},
                },
            ),
        )

    def test_codex_runs_ephemeral_read_only_with_structured_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "codex"
            binary.write_text("", encoding="utf-8")
            runner = FakeProcessRunner()
            diagnoser = CodexDiagnoser(codex_binary=str(binary), process_runner=runner)

            result = diagnoser.diagnose(
                {"nodes": [{"id": "gpu-1", "hostname": "gpu-host"}]},
                model="test-model",
            )

        self.assertEqual(result["confidence"], "medium")
        command, options = runner.calls[1]
        self.assertIn("--ephemeral", command)
        self.assertIn("--ignore-user-config", command)
        self.assertEqual(command[command.index("--sandbox") + 1], "read-only")
        self.assertEqual(command[command.index("--model") + 1], "test-model")
        self.assertIn('"gpu-1"', options["input"])

    def test_codex_login_is_required(self):
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "codex"
            binary.write_text("", encoding="utf-8")
            diagnoser = CodexDiagnoser(
                codex_binary=str(binary), process_runner=FakeProcessRunner(logged_in=False)
            )
            with self.assertRaisesRegex(ValueError, "codex login"):
                diagnoser.diagnose({"nodes": []})


if __name__ == "__main__":
    unittest.main()
