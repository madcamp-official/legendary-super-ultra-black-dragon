from __future__ import annotations

import fcntl
import os
import stat
from pathlib import Path


HOST_SETUP_LOCK_PATH = Path("/run/lock/dure-host-setup.lock")


class HostSetupLockError(RuntimeError):
    pass


def acquire_host_setup_lock(
    path: Path = HOST_SETUP_LOCK_PATH,
    *,
    require_root_owner: bool = True,
) -> int:
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags, 0o600)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise HostSetupLockError(f"refusing unsafe host setup lock file {path}")
        if require_root_owner and metadata.st_uid != 0:
            raise HostSetupLockError(f"host setup lock file is not root-owned: {path}")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return descriptor
    except BlockingIOError as exc:
        if descriptor is not None:
            os.close(descriptor)
        raise HostSetupLockError(
            "another Dure host setup or join operation is already running"
        ) from exc
    except (OSError, HostSetupLockError) as exc:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if isinstance(exc, HostSetupLockError):
            raise
        raise HostSetupLockError(f"cannot acquire host setup lock {path}: {exc}") from exc


def release_host_setup_lock(descriptor: int) -> None:
    try:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)
