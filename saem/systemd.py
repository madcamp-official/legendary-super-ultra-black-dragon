"""Render + install the systemd unit that runs the assigned role.

This is what fixes the original weakness: every prior service was a bare
`nohup ... &`, so a VM reboot silently killed it. `saem register` (called
locally or remotely via the agent) always goes through here, so any
assigned role survives a reboot and gets auto-restarted on crash.
"""
from __future__ import annotations

import subprocess
from typing import Optional

UNIT_PATH = "/etc/systemd/system/saem-role.service"

UNIT_TEMPLATE = """[Unit]
Description=saem role service ({role})
After=network.target

[Service]
ExecStart={python} -m saem.run_role
Restart=always
RestartSec=5
Environment=SAEM_ROLE_PORT={port}

[Install]
WantedBy=multi-user.target
"""


def install_role_service(role: str, port: Optional[int], python: str = "/usr/bin/python3") -> None:
    unit = UNIT_TEMPLATE.format(role=role, python=python, port=port or "")
    with open(UNIT_PATH, "w", encoding="utf-8") as f:
        f.write(unit)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", "--now", "saem-role"], check=True)


def restart_role_service() -> None:
    subprocess.run(["systemctl", "restart", "saem-role"], check=True)
