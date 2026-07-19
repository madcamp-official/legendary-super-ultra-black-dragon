from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dure.agent import join_control_plane, resolve_join_settings
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
        runner = FakeRunner(executables={"systemctl"})
        with tempfile.TemporaryDirectory() as temporary:
            client_config = Path(temporary) / "client.env"
            agent_config = Path(temporary) / "agent.json"
            client_config.write_text("DURE_SERVER=https://control.example\n", encoding="utf-8")
            FakeJoinClient.requests = []
            with patch("dure.agent.JSONClient", FakeJoinClient), patch(
                "dure.agent.NodeProbe.collect", return_value=profile("joined-host")
            ):
                result = join_control_plane(
                    config_path=agent_config,
                    client_config=client_config,
                    runner=runner,
                )
            stored = json.loads(agent_config.read_text(encoding="utf-8"))
            self.assertEqual(result, {"node_id": "server-node-id", "status": "pending"})
            self.assertEqual(stored["server"], "https://control.example")
            self.assertEqual(stored["credential"], "node-secret")
            self.assertIn(("systemctl", "enable", "--now", "dure-agent"), runner.calls)
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
            )
            self.assertEqual(result, {"node_id": "existing", "status": "already-joined"})
            self.assertIn(("systemctl", "enable", "--now", "dure-agent"), runner.calls)

    @patch("dure.agent.os.geteuid", return_value=1000)
    def test_join_requires_root(self, _geteuid):
        with self.assertRaisesRegex(PermissionError, "must run as root"):
            join_control_plane(start_service=False)
