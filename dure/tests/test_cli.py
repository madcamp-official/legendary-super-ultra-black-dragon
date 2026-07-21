from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
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


class AdminEnvFileCLITests(unittest.TestCase):
    def setUp(self):
        FakeJSONClient.calls = []
        FakeJSONClient.response = {"nodes": []}

    def _write_env(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        path.chmod(0o600)

    def _run(self, arguments: list[str], working_directory: Path) -> tuple[int, str]:
        error = io.StringIO()
        with patch.dict(
            os.environ,
            {
                "DURE_SERVER": "https://stale.example",
                "DURE_ADMIN_TOKEN": "stale-token",
            },
            clear=True,
        ), patch("dure.cli.Path.cwd", return_value=working_directory), patch(
            "dure.agent.resolve_join_settings",
            return_value=("https://packaged", False),
        ), patch("dure.http.JSONClient", FakeJSONClient), redirect_stderr(error):
            result = main(arguments)
        return result, error.getvalue()

    def test_admin_automatically_uses_nested_dure_env_as_one_credential_pair(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_env(
                root / "dure" / ".env",
                "DURE_SERVER=https://control.example\nDURE_ADMIN_TOKEN=fresh-token\n",
            )

            result, error = self._run(["admin", "nodes"], root)

        self.assertEqual(result, 0)
        self.assertEqual(error, "")
        self.assertEqual(
            FakeJSONClient.calls,
            [("https://control.example", "fresh-token", "GET", "/v1/admin/nodes", None)],
        )

    def test_admin_uses_current_project_env_and_accepts_export_syntax(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_env(
                root / ".env",
                "export DURE_SERVER='https://control.example'\n"
                'export DURE_ADMIN_TOKEN="fresh-token"\n',
            )

            result, error = self._run(["admin", "nodes"], root)

        self.assertEqual(result, 0)
        self.assertEqual(error, "")
        self.assertEqual(FakeJSONClient.calls[0][:2], ("https://control.example", "fresh-token"))

    def test_explicit_arguments_override_the_env_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_env(
                root / "admin.env",
                "DURE_SERVER=https://file.example\nDURE_ADMIN_TOKEN=file-token\n",
            )

            result, error = self._run(
                [
                    "admin",
                    "--env-file",
                    str(root / "admin.env"),
                    "--server",
                    "https://argument.example",
                    "--token",
                    "argument-token",
                    "nodes",
                ],
                root,
            )

        self.assertEqual(result, 0)
        self.assertEqual(error, "")
        self.assertEqual(FakeJSONClient.calls[0][:2], ("https://argument.example", "argument-token"))

    def test_complete_explicit_connection_ignores_automatic_env_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "dure" / ".env"
            self._write_env(
                path,
                "DURE_SERVER=https://file.example\nDURE_ADMIN_TOKEN=file-token\n",
            )
            path.chmod(0o644)

            result, error = self._run(
                [
                    "admin",
                    "--server",
                    "https://argument.example",
                    "--token",
                    "argument-token",
                    "nodes",
                ],
                root,
            )

        self.assertEqual(result, 0)
        self.assertEqual(error, "")
        self.assertEqual(FakeJSONClient.calls[0][:2], ("https://argument.example", "argument-token"))

    def test_admin_rejects_partial_or_group_readable_credential_files(self):
        cases = (
            ("DURE_ADMIN_TOKEN=token-only\n", 0o600, "must define"),
            (
                "DURE_SERVER=https://control.example\nDURE_ADMIN_TOKEN=token\n",
                0o640,
                "must not be accessible",
            ),
        )
        for content, mode, expected in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / "dure" / ".env"
                self._write_env(path, content)
                path.chmod(mode)
                FakeJSONClient.calls = []

                result, error = self._run(["admin", "nodes"], root)

                self.assertEqual(result, 2)
                self.assertIn(expected, error)
                self.assertEqual(FakeJSONClient.calls, [])

    def test_admin_rejects_a_symlinked_credential_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.env"
            self._write_env(
                target,
                "DURE_SERVER=https://control.example\nDURE_ADMIN_TOKEN=token\n",
            )
            path = root / "dure" / ".env"
            path.parent.mkdir(parents=True)
            path.symlink_to(target)

            result, error = self._run(["admin", "nodes"], root)

        self.assertEqual(result, 2)
        self.assertIn("not a safe readable file", error)
        self.assertEqual(FakeJSONClient.calls, [])


class ArtifactManifestCLITests(unittest.TestCase):
    def setUp(self):
        FakeJSONClient.calls = []
        FakeJSONClient.response = {
            "manifest": {
                "digest": "sha256:" + "a" * 64,
                "schema_version": 1,
            },
            "created": True,
        }

    def run_cli(self, arguments: list[str]) -> tuple[int, str, str]:
        output = io.StringIO()
        error = io.StringIO()
        with patch(
            "dure.agent.resolve_join_settings",
            return_value=("https://packaged", False),
        ), patch("dure.http.JSONClient", FakeJSONClient), redirect_stdout(
            output
        ), redirect_stderr(error):
            result = main(arguments)
        return result, output.getvalue(), error.getvalue()

    def command(self, *arguments: str) -> list[str]:
        return [
            "admin",
            "--server",
            "https://control.example",
            "--token",
            "admin-token",
            "artifact-manifest",
            *arguments,
        ]

    def test_register_reads_one_json_object_and_posts_the_closed_manifest(self):
        manifest = {
            "schema_version": 1,
            "files": [
                {
                    "path": "config.json",
                    "kind": "REGULAR",
                    "size_bytes": 2,
                    "sha256": "sha256:" + "b" * 64,
                    "chunks": [
                        {
                            "ordinal": 0,
                            "offset_bytes": 0,
                            "length_bytes": 2,
                            "sha256": "sha256:" + "c" * 64,
                        }
                    ],
                }
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "manifest.json"
            source.write_text(json.dumps(manifest), encoding="utf-8")

            result, output, error = self.run_cli(
                self.command("register", "artifact-1", "--file", str(source))
            )

        self.assertEqual(result, 0)
        self.assertEqual(error, "")
        self.assertEqual(
            FakeJSONClient.calls,
            [
                (
                    "https://control.example",
                    "admin-token",
                    "POST",
                    "/v1/admin/model-artifacts/artifact-1/manifest",
                    manifest,
                )
            ],
        )
        self.assertEqual(json.loads(output), FakeJSONClient.response)

    def test_show_uses_the_artifact_scoped_read_only_endpoint(self):
        result, output, error = self.run_cli(
            self.command("show", "artifact-1")
        )

        self.assertEqual(result, 0)
        self.assertEqual(error, "")
        self.assertEqual(
            FakeJSONClient.calls[0][2:],
            (
                "GET",
                "/v1/admin/model-artifacts/artifact-1/manifest",
                None,
            ),
        )
        self.assertEqual(json.loads(output), FakeJSONClient.response)

    def test_register_rejects_non_object_json_before_the_request(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "manifest.json"
            source.write_text("[]", encoding="utf-8")

            result, output, error = self.run_cli(
                self.command("register", "artifact-1", "--file", str(source))
            )

        self.assertEqual(result, 2)
        self.assertEqual(output, "")
        self.assertIn("artifact manifest JSON must be an object", error)
        self.assertEqual(FakeJSONClient.calls, [])


class ArtifactCacheCLITests(unittest.TestCase):
    def setUp(self):
        FakeJSONClient.calls = []
        FakeJSONClient.response = {"cache": {"id": "cache-1"}, "tasks": []}

    def run_cli(self, *arguments: str) -> tuple[int, dict]:
        output = io.StringIO()
        with patch(
            "dure.agent.resolve_join_settings",
            return_value=("https://packaged", False),
        ), patch("dure.http.JSONClient", FakeJSONClient), redirect_stdout(output):
            result = main(
                [
                    "admin",
                    "--server",
                    "https://control.example",
                    "--token",
                    "admin-token",
                    "artifact-cache",
                    *arguments,
                ]
            )
        return result, json.loads(output.getvalue())

    def test_list_show_and_verify_use_read_only_get_endpoints(self):
        for arguments, path in (
            (("list",), "/v1/admin/artifact-caches"),
            (("show", "cache-1"), "/v1/admin/artifact-caches/cache-1"),
            (("verify", "cache-1"), "/v1/admin/artifact-caches/cache-1/verify"),
        ):
            with self.subTest(arguments=arguments):
                FakeJSONClient.calls = []
                result, output = self.run_cli(*arguments)
                self.assertEqual(result, 0)
                self.assertEqual(output, FakeJSONClient.response)
                self.assertEqual(FakeJSONClient.calls[0][2:], ("GET", path, None))

    def test_quarantine_is_preview_by_default_and_apply_is_explicit(self):
        for arguments, apply in (
            (("quarantine", "cache-1"), False),
            (("quarantine", "cache-1", "--apply"), True),
        ):
            with self.subTest(arguments=arguments):
                FakeJSONClient.calls = []
                result, _output = self.run_cli(*arguments)
                self.assertEqual(result, 0)
                self.assertEqual(
                    FakeJSONClient.calls[0][2:],
                    (
                        "POST",
                        "/v1/admin/artifact-caches/cache-1/quarantine",
                        {"apply": apply},
                    ),
                )


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


class DeploymentGenerationCLITests(unittest.TestCase):
    def setUp(self):
        FakeJSONClient.calls = []
        FakeJSONClient.response = {"deployment": {"id": "generation-2"}}

    def run_cli(self, arguments: list[str]) -> tuple[int, str]:
        output = io.StringIO()
        with patch(
            "dure.agent.resolve_join_settings",
            return_value=("https://packaged", False),
        ), patch("dure.http.JSONClient", FakeJSONClient), redirect_stdout(output):
            result = main(arguments)
        return result, output.getvalue()

    def command(self, *arguments: str) -> list[str]:
        return [
            "admin",
            "--server",
            "https://control.example",
            "--token",
            "admin-token",
            "deployment",
            *arguments,
        ]

    def test_show_uses_generation_detail_endpoint(self):
        result, output = self.run_cli(self.command("show", "generation-2"))

        self.assertEqual(result, 0)
        self.assertEqual(
            FakeJSONClient.calls,
            [
                (
                    "https://control.example",
                    "admin-token",
                    "GET",
                    "/v1/admin/deployments/generation-2",
                    None,
                )
            ],
        )
        self.assertEqual(
            output,
            json.dumps(FakeJSONClient.response, indent=2, sort_keys=True) + "\n",
        )

    def test_generations_uses_lineage_endpoint(self):
        FakeJSONClient.response = {"generations": [{"id": "generation-1"}]}

        result, output = self.run_cli(
            self.command("generations", "generation-2")
        )

        self.assertEqual(result, 0)
        self.assertEqual(
            FakeJSONClient.calls[0][2:],
            (
                "GET",
                "/v1/admin/deployments/generation-2/generations",
                None,
            ),
        )
        self.assertEqual(json.loads(output), FakeJSONClient.response)

    def test_prepare_previews_by_default_and_applies_only_when_explicit(self):
        request_id = "79848aaa-c0cc-42cb-8944-c93e9466f8ef"
        FakeJSONClient.response = {
            "preparation": {"id": "preparation-1", "status": "PREPARED"},
            "tasks": [],
            "changed": True,
        }
        common = self.command(
            "prepare",
            "generation-2",
            "--request-id",
            request_id,
        )

        result, output = self.run_cli(common)

        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output), FakeJSONClient.response)
        self.assertEqual(
            FakeJSONClient.calls[-1][2:],
            (
                "POST",
                "/v1/admin/deployments/generation-2/prepare",
                {"request_id": request_id, "apply": False},
            ),
        )

        FakeJSONClient.calls = []
        result, output = self.run_cli([*common, "--apply"])

        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output), FakeJSONClient.response)
        self.assertEqual(
            FakeJSONClient.calls[-1][4],
            {"request_id": request_id, "apply": True},
        )

        stage_digest = "sha256:" + "a" * 64
        FakeJSONClient.calls = []
        result, output = self.run_cli(
            [*common, "--stage-variant", stage_digest]
        )

        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output), FakeJSONClient.response)
        self.assertEqual(
            FakeJSONClient.calls[-1][4],
            {
                "request_id": request_id,
                "apply": False,
                "artifact_set_digest": stage_digest,
            },
        )

    def test_preparation_preserves_success_partial_and_retry_progress_json(self):
        cases = (
            ("SUCCEEDED", "COMPLETE", 40, False, 0, "SUCCEEDED"),
            ("PARTIAL_FAILED", "FAILED", 20, False, 0, "FAILED"),
            ("QUEUED", "MODEL", 20, True, 1, "QUEUED"),
        )
        for (
            status,
            stage,
            verified_bytes,
            retrying,
            retry_count,
            model_status,
        ) in cases:
            with self.subTest(status=status):
                FakeJSONClient.calls = []
                FakeJSONClient.response = {
                    "preparation": {
                        "id": "preparation-1",
                        "status": status,
                        "progress": {
                            "expected_bytes": 40,
                            "verified_bytes": verified_bytes,
                            "bytes_source": "COMPLETED_MODEL_VERIFICATION",
                            "stage": stage,
                            "retrying": retrying,
                            "model_retry_count": retry_count,
                            "image_retry_count": 0,
                        },
                        "nodes": [
                            {
                                "node_id": "node-1",
                                "progress": {
                                    "expected_bytes": 40,
                                    "verified_bytes": verified_bytes,
                                    "bytes_source": (
                                        "COMPLETED_MODEL_VERIFICATION"
                                    ),
                                    "stage": stage,
                                    "retrying": retrying,
                                    "model": {
                                        "status": model_status,
                                        "current_attempt": retry_count + 1,
                                        "retry_count": retry_count,
                                        "failure_code": (
                                            "MODEL_STORE_DOWNLOAD_TIMEOUT"
                                            if model_status == "FAILED"
                                            else None
                                        ),
                                    },
                                    "image": {
                                        "status": (
                                            "SUCCEEDED"
                                            if stage == "COMPLETE"
                                            else "PREPARED"
                                        ),
                                        "current_attempt": (
                                            1 if stage == "COMPLETE" else 0
                                        ),
                                        "retry_count": 0,
                                        "failure_code": None,
                                    },
                                },
                            }
                        ],
                    }
                }

                result, output = self.run_cli(
                    self.command("preparation", "preparation-1")
                )

                self.assertEqual(result, 0)
                self.assertEqual(json.loads(output), FakeJSONClient.response)
                rendered = json.loads(output)["preparation"]
                self.assertEqual(rendered["progress"]["stage"], stage)
                self.assertEqual(
                    rendered["progress"]["verified_bytes"], verified_bytes
                )
                self.assertEqual(
                    rendered["nodes"][0]["progress"]["model"][
                        "retry_count"
                    ],
                    retry_count,
                )
                self.assertEqual(
                    FakeJSONClient.calls[-1][2:],
                    (
                        "GET",
                        "/v1/admin/deployment-preparations/preparation-1",
                        None,
                    ),
                )

    def test_prepare_rejects_a_missing_or_noncanonical_request_id(self):
        invalid_commands = (
            self.command("prepare", "generation-2"),
            self.command(
                "prepare",
                "generation-2",
                "--request-id",
                "not-a-uuid",
            ),
            self.command(
                "prepare",
                "generation-2",
                "--request-id",
                "79848AAA-C0CC-42CB-8944-C93E9466F8EF",
            ),
        )

        for arguments in invalid_commands:
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    main(arguments)
                self.assertEqual(raised.exception.code, 2)

        self.assertEqual(FakeJSONClient.calls, [])

    def test_rollback_prepares_by_default_and_applies_only_when_explicit(self):
        FakeJSONClient.response = {
            "operation": {"id": "operation-1", "status": "PREPARED"},
            "tasks": [],
            "changed": True,
        }
        common = self.command(
            "rollback",
            "generation-2",
            "--nodes",
            "node-b",
            "node-a",
            "node-b",
        )

        result, _ = self.run_cli(common)

        self.assertEqual(result, 0)
        self.assertEqual(
            FakeJSONClient.calls[-1][2:],
            (
                "POST",
                "/v1/admin/deployments/generation-2/rollback",
                {
                    "node_ids": ["node-b", "node-a"],
                    "apply": False,
                    "serve": False,
                },
            ),
        )

        FakeJSONClient.calls = []
        result, _ = self.run_cli([*common, "--apply", "--serve"])

        self.assertEqual(result, 0)
        self.assertEqual(
            FakeJSONClient.calls[-1][4],
            {
                "node_ids": ["node-b", "node-a"],
                "apply": True,
                "serve": True,
            },
        )


if __name__ == "__main__":
    unittest.main()
