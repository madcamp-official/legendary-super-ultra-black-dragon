"""Role entry-point registry.

Adding a 6th role later is: write roles/<name>.py with a run(port) function,
add one line here, add the name to common.state.ROLE_CHOICES. Nothing else
in agent.py / head.py / cli.py needs to change.
"""
from __future__ import annotations

from typing import Callable

from . import api_proxy, crawler, ingest_coordinator, qdrant_primary, retrieval_gateway

ROLE_ENTRYPOINTS: dict[str, Callable[[int | None], None]] = {
    "qdrant_primary": qdrant_primary.run,
    "retrieval_gateway": retrieval_gateway.run,
    "ingest_coordinator": ingest_coordinator.run,
    "crawler": crawler.run,
    "api_proxy": api_proxy.run,
}
