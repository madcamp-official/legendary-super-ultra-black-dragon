"""Entry point invoked by the saem-role systemd unit (`python -m saem.run_role`).

Reads /etc/saem/role.yaml (written by register()) and dispatches to the
matching role's run(). Kept as a tiny separate module, rather than folded
into agent.py, so the systemd unit doesn't depend on the agent process.
"""
from saem.common.state import read_role
from saem.roles import ROLE_ENTRYPOINTS


def main() -> None:
    assignment = read_role()
    if assignment is None:
        raise SystemExit("no role assigned yet: /etc/saem/role.yaml is missing")
    entrypoint = ROLE_ENTRYPOINTS[assignment["role"]]
    entrypoint(assignment.get("port"))


if __name__ == "__main__":
    main()
