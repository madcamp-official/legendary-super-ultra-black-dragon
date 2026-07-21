"""Role entry-point registry.

Roles are imported lazily, by name, only when one is actually run. Importing
all five eagerly would mean every node needs every role's dependencies —
a head node would need trafilatura and fastembed it never calls, and one
missing package would stop `saem agent` from starting at all, leaving the
node unreachable for role assignment.

Adding a 6th role later is: write roles/<name>.py with a run(port) function,
add one line to ROLE_MODULES, add the name to common.state.ROLE_CHOICES.
Nothing else in agent.py / head.py / cli.py needs to change.
"""
from __future__ import annotations

import importlib
from typing import Callable, Optional

ROLE_MODULES = {
    "qdrant_primary": "saem.roles.qdrant_primary",
    "retrieval_gateway": "saem.roles.retrieval_gateway",
    "ingest_coordinator": "saem.roles.ingest_coordinator",
    "crawler": "saem.roles.crawler",
    "api_proxy": "saem.roles.api_proxy",
}


def get_entrypoint(role: str) -> Callable[[Optional[int]], None]:
    """Import the role's module on demand and hand back its run()."""
    if role not in ROLE_MODULES:
        raise KeyError(role)
    return importlib.import_module(ROLE_MODULES[role]).run


class _RoleRegistry:
    """Mapping-ish view over ROLE_MODULES: `role in ROLE_ENTRYPOINTS` and
    `ROLE_ENTRYPOINTS[role]` both work, but nothing is imported until an
    actual lookup happens."""

    def __contains__(self, role: object) -> bool:
        return role in ROLE_MODULES

    def __getitem__(self, role: str) -> Callable[[Optional[int]], None]:
        return get_entrypoint(role)

    def __iter__(self):
        return iter(ROLE_MODULES)

    def keys(self):
        return ROLE_MODULES.keys()


ROLE_ENTRYPOINTS = _RoleRegistry()
