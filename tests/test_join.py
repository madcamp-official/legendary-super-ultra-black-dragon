from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from dure.agent import join_control_plane, resolve_join_settings
from dure.cli import main
from dure.host_setup import acquire_host_setup_lock, release_host_setup_lock
from tests.helpers import FakeRunner, profile


class FakeJoinClient:
    requests = []

    def __init__(self, base_url, token=None, *, verify_tls=True):
        self.base_url = base_url
        self.verify_tls = verify_tls

    def request(self, method, path, payload=None):
        self.requests.append((method, path, payload))
        return {"node_id": "server-node-id", "credential": "node-secret", "status": "pending"}


class JoinTests(unittest.TestCase):
    def test_packaged_settings_resolve_without_command_arguments(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "client.env"
            config.write_text("DURE_SERVER=http://control:8081\nDURE_INSECURE=true\n", encoding="utf-8")
            self.assertEqual(resolve_join_settings(client_config=config), ("http://control:8081", True))

    @patch("dure.agent.os.geteuid", return_value=0)
    def test_join_registers_config_and_starts_agent(self, _geteuid):
        with tempfile.TemporaryDirectory() as temporary:
            client_config = Path(temporary) / "client.env"
            agent_config = Path(temporary) / "agent.json"
            service_started_after_config = []

            def verify_service_order(command):
                if command == ("systemctl", "enable", "--now", "dure-agent"):
                    service_started_after_config.append(agent_config.is_file())
                return None

            runner = FakeRunner(
                executables={"systemctl"}, response_factory=verify_service_order
            )
            client_config.write_text("DURE_SERVER=https://control.example\n", encoding="utf-8")
            FakeJoinClient.requests = []
            with patch("dure.agent.JSONClient", FakeJoinClient), patch(
                "dure.agent.NodeProbe.collect", return_value=profile("joined-host")
            ):
                result = join_control_plane(
                    config_path=agent_config,
                    client_config=client_config,
                    runner=runner,
                    setup_lock_path=Path(temporary) / "setup.lock",
                )
            stored = json.loads(agent_config.read_text(encoding="utf-8"))
            self.assertEqual(result, {"node_id": "server-node-id", "status": "pending"})
            self.assertEqual(stored["server"], "https://control.example")
            self.assertEqual(stored["credential"], "node-secret")
            self.assertIn(("systemctl", "enable", "--now", "dure-agent"), runner.calls)
            self.assertEqual(service_started_after_config, [True])
            self.assertEqual(FakeJoinClient.requests[0][1], "/v1/nodes/join")

    def test_http_server_requires_explicit_insecure_setting(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "client.env"
            config.write_text("DURE_SERVER=http://control:8081\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                resolve_join_settings(client_config=config)

    @patch("dure.agent.os.geteuid", return_value=0)
    def test_join_is_idempotent_and_restarts_agent(self, _geteuid):
        runner = FakeRunner(executables={"systemctl"})
        with tempfile.TemporaryDirectory() as temporary:
            client_config = Path(temporary) / "client.env"
            agent_config = Path(temporary) / "agent.json"
            client_config.write_text("DURE_SERVER=https://control.example\n", encoding="utf-8")
            agent_config.write_text(
                json.dumps({"server": "https://control.example", "node_id": "existing", "credential": "secret"}),
                encoding="utf-8",
            )
            result = join_control_plane(
                config_path=agent_config,
                client_config=client_config,
                runner=runner,
                setup_lock_path=Path(temporary) / "setup.lock",
            )
            self.assertEqual(result, {"node_id": "existing", "status": "already-joined"})
            self.assertIn(("systemctl", "enable", "--now", "dure-agent"), runner.calls)

    @patch("dure.agent.os.geteuid", return_value=1000)
    def test_join_requires_root(self, _geteuid):
        with self.assertRaisesRegex(PermissionError, "must run as root"):
            join_control_plane(start_service=False)

    @patch("dure.agent.os.geteuid", return_value=0)
    def test_join_cannot_race_with_bootstrap_host_setup(self, _geteuid):
        with tempfile.TemporaryDirectory() as temporary:
            lock_path = Path(temporary) / "setup.lock"
            descriptor = acquire_host_setup_lock(
                lock_path,
                require_root_owner=False,
            )
            try:
                with self.assertRaisesRegex(RuntimeError, "already running"):
                    join_control_plane(
                        start_service=False,
                        setup_lock_path=lock_path,
                    )
            finally:
                release_host_setup_lock(descriptor)

    def test_cli_reports_host_setup_lock_contention_without_traceback(self):
        error = io.StringIO()
        with patch(
            "dure.agent.join_control_plane",
            side_effect=RuntimeError("another Dure host setup or join operation is already running"),
        ), redirect_stderr(error):
            result = main(["join"])

        self.assertEqual(result, 2)
        self.assertIn("already running", error.getvalue())
        self.assertNotIn("Traceback", error.getvalue())
