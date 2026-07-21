"""qdrant_primary: run the Qdrant binary this node owns.

Not a container: v1.12.6 is the compatibility ceiling for these VMs because
newer builds require GLIBC 2.38 and the VMs ship 2.35. Install the binary at
QDRANT_BINARY before assigning this role, or the service will crash-loop.
"""
from __future__ import annotations

import subprocess
from typing import Optional

from saem.common.config import QDRANT_BINARY


def run(port: Optional[int] = None) -> None:
    subprocess.run([QDRANT_BINARY], check=True)
