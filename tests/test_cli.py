from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import call, patch

from dure.cli import main


class FakeJSONClient:
    calls: list[tuple[str, str, str, str, dict | None]] = []
    response: dict = {}

    def __init__(self, server: str, token: str):
        self.server = server
        self.token = token

    def request(self, method: str, path: str, payload: dict | None = None):
        self.calls.append((self.server, self.token, method, path, payload))
        return self.response


class DeploymentRecommendCLITests(unittest.TestCase):
    def setUp(self):
        FakeJSONClient.calls = []
        FakeJSONClient.response = {
            "recommendation": {
                "inventory_fingerprint": "sha256:inventory",
                "policy_version": "quality-first-v1",
                "selected": {"model_release_id": "release-1"},
            }
        }

    def run_cli(self, arguments: list[str]) -> tuple[int, str]:
        output = io.StringIO()
        with patch("dure.agent.resolve_join_settings", return_value=("https://packaged", False)), patch(
            "dure.http.JSONClient", FakeJSONClient
        ), redirect_stdout(output):
            result = main(arguments)
        return result, output.getvalue()

    def test_recommend_all_online_posts_read_only_request_and_prints_stable_json(self):
        result, output = self.run_cli(
            [
                "admin",
                "--server",
                "https://control.example",
                "--token",
                "admin-token",
                "deployment",
                "recommend",
                "--all-online",
            ]
        )

        self.assertEqual(result, 0)
        self.assertEqual(
            FakeJSONClient.calls,
            [
                (
                    "https://control.example",
                    "admin-token",
                    "POST",
                    "/v1/admin/deployment-recommendations",
                    {
                        "node_ids": [],
                        "all_online": True,
                        "objective": "quality-first",
                    },
                )
            ],
        )
        self.assertEqual(
            output,
            json.dumps(FakeJSONClient.response, indent=2, sort_keys=True) + "\n",
        )

    def test_recommend_flattens_repeated_node_lists_without_reordering(self):
        result, output = self.run_cli(
            [
                "admin",
                "--server",
                "https://control.example",
                "--token",
                "admin-token",
                "deployment",
                "recommend",
                "--nodes",
                "node-b",
                "node-a",
                "--nodes",
                "node-a",
                "node-c",
                "--objective",
                "quality-first",
            ]
        )

        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output), FakeJSONClient.response)
        self.assertEqual(
            FakeJSONClient.calls[0][4],
            {
                "node_ids": ["node-b", "node-a", "node-c"],
                "all_online": False,
                "objective": "quality-first",
            },
        )

    def test_recommend_requires_exactly_one_node_selection_mode(self):
        common = [
            "admin",
            "--server",
            "https://control.example",
            "--token",
            "admin-token",
            "deployment",
            "recommend",
        ]
        invalid_arguments = [
            common,
            [*common, "--all-online", "--nodes", "node-a"],
            [*common, "--all-online", "--objective", "unsupported"],
        ]

        for arguments in invalid_arguments:
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    main(arguments)
                self.assertEqual(raised.exception.code, 2)

        self.assertEqual(FakeJSONClient.calls, [])


class RecommendationCLITests(unittest.TestCase):
    def setUp(self):
        FakeJSONClient.calls = []
        FakeJSONClient.response = {
            "recommendation": {
                "id": "sha256:" + "a" * 64,
                "selected": {"model_release_id": "release-1"},
            },
            "deployment": None,
        }

    def run_cli(self, arguments: list[str]) -> tuple[int, str]:
        output = io.StringIO()
        with patch(
            "dure.agent.resolve_join_settings",
            return_value=("https://packaged", False),
        ), patch("dure.http.JSONClient", FakeJSONClient), redirect_stdout(output):
            result = main(arguments)
        return result, output.getvalue()

    def test_show_uses_exact_get_path_and_prints_stable_json(self):
        recommendation_id = "sha256:" + "b" * 64

        result, output = self.run_cli(
            [
                "admin",
                "--server",
                "https://control.example",
                "--token",
                "admin-token",
                "recommendation",
                "show",
                recommendation_id,
            ]
        )

        self.assertEqual(result, 0)
        self.assertEqual(
            FakeJSONClient.calls,
            [
                (
                    "https://control.example",
                    "admin-token",
                    "GET",
                    f"/v1/admin/deployment-recommendations/{recommendation_id}",
                    None,
                )
            ],
        )
        self.assertEqual(
            output,
            json.dumps(FakeJSONClient.response, indent=2, sort_keys=True) + "\n",
        )

    def test_accept_uses_exact_post_path_body_and_prints_stable_json(self):
        recommendation_id = "sha256:" + "c" * 64
        previous_id = "deployment-generation-1"

        for extra_arguments, expected_payload in (
            ([], {}),
            (
                ["--previous-generation", previous_id],
                {"previous_generation_id": previous_id},
            ),
        ):
            with self.subTest(extra_arguments=extra_arguments):
                FakeJSONClient.calls = []
                result, output = self.run_cli(
                    [
                        "admin",
                        "--server",
                        "https://control.example",
                        "--token",
                        "admin-token",
                        "recommendation",
                        "accept",
                        recommendation_id,
                        *extra_arguments,
                    ]
                )

                self.assertEqual(result, 0)
                self.assertEqual(
                    FakeJSONClient.calls,
                    [
                        (
                            "https://control.example",
                            "admin-token",
                            "POST",
                            f"/v1/admin/deployment-recommendations/{recommendation_id}/accept",
                            expected_payload,
                        )
                    ],
                )
                self.assertEqual(
                    output,
                    json.dumps(FakeJSONClient.response, indent=2, sort_keys=True)
                    + "\n",
                )

    def test_explicit_deployment_create_preserves_selected_model(self):
        class FakePlan:
            def to_dict(self):
                return {"deployment_id": "explicit-plan", "model_id": "model-32b"}

        FakeJSONClient.response = {"deployment": {"id": "deployment-1"}}
        profiles = [object()]
        with patch("dure.cli._load_profiles", return_value=profiles), patch(
            "dure.cli.build_plan", return_value=FakePlan()
        ) as build_plan:
            result, output = self.run_cli(
                [
                    "admin",
                    "--server",
                    "https://control.example",
                    "--token",
                    "admin-token",
                    "deployment",
                    "create",
                    "--profile",
                    "node.json",
                    "--model",
                    "model-32b",
                    "--image",
                    "registry.example/runtime@sha256:" + "d" * 64,
                ]
            )

        self.assertEqual(result, 0)
        self.assertEqual(output, "deployment-1\n")
        self.assertEqual(
            build_plan.call_args_list,
            [
                call(
                    profiles,
                    model_id="model-32b",
                    image="registry.example/runtime@sha256:" + "d" * 64,
                    network_interface=None,
                )
            ],
        )
        self.assertEqual(
            FakeJSONClient.calls,
            [
                (
                    "https://control.example",
                    "admin-token",
                    "POST",
                    "/v1/admin/deployments",
                    {
                        "plan": {
                            "deployment_id": "explicit-plan",
                            "model_id": "model-32b",
                        },
                        "accept_model_download": False,
                        "pull_image": False,
                    },
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
