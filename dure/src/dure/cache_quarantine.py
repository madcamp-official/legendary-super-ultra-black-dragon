from __future__ import annotations

import ctypes
import errno
import json
import os
import re
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path

from .command import Runner, SubprocessRunner
from .model_cache import MODEL_CACHE_KIND_FULL_SNAPSHOT, MODEL_CACHE_KIND_STAGE


DURE_MODEL_CACHE_ROOT = Path("/var/lib/dure/models")
DURE_CACHE_QUARANTINE_DIRECTORY = ".dure-quarantine"
DURE_CACHE_QUARANTINE_ROOT = (
    DURE_MODEL_CACHE_ROOT / DURE_CACHE_QUARANTINE_DIRECTORY
)
MAX_DURE_CONTAINERS = 200
AT_FDCWD = -100
RENAME_NOREPLACE = 1
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_CONTAINER_ID = re.compile(r"[0-9a-f]{12,64}")
ARTIFACT_CACHE_QUARANTINE_FAILURE_CODES = frozenset(
    {
        "CACHE_QUARANTINE_PAYLOAD_REJECTED",
        "CACHE_QUARANTINE_ROOT_UNSAFE",
        "CACHE_QUARANTINE_SOURCE_UNSAFE",
        "CACHE_QUARANTINE_SOURCE_MISSING",
        "CACHE_QUARANTINE_TARGET_EXISTS",
        "CACHE_QUARANTINE_ACTIVITY_UNKNOWN",
        "CACHE_QUARANTINE_CACHE_ACTIVE",
        "CACHE_QUARANTINE_ATOMIC_RENAME_UNAVAILABLE",
        "CACHE_QUARANTINE_IO_FAILED",
        "CACHE_QUARANTINE_EXECUTION_FAILED",
    }
)


class ArtifactCacheQuarantineError(ValueError):
    def __init__(self, failure_code: str):
        self.failure_code = failure_code
        super().__init__(failure_code)


def artifact_cache_quarantine_failure_code(exc: BaseException) -> str:
    value = getattr(exc, "failure_code", None)
    if value in ARTIFACT_CACHE_QUARANTINE_FAILURE_CODES:
        return value
    return "CACHE_QUARANTINE_EXECUTION_FAILED"


def validate_artifact_cache_quarantine_result(
    task: object,
    result: object,
    node_id: str,
) -> dict[str, object]:
    if type(task) is not dict or task.get("type") != "QUARANTINE_ARTIFACT_CACHE":
        raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_PAYLOAD_REJECTED")
    request = ArtifactCacheQuarantineRequest.from_payload(task.get("payload"))
    if request.node_id != node_id or (
        type(result) is not dict
        or set(result)
        != {
            "node_id",
            "cache_kind",
            "cache_identity_digest",
            "status",
        }
        or result.get("node_id") != node_id
        or result.get("cache_kind") != request.cache_kind
        or result.get("cache_identity_digest") != request.cache_identity_digest
        or result.get("status")
        not in {"QUARANTINED", "ALREADY_QUARANTINED"}
    ):
        raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_EXECUTION_FAILED")
    return dict(result)


@dataclass(frozen=True)
class ArtifactCacheQuarantineRequest:
    node_id: str
    cache_kind: str
    cache_identity_digest: str

    @classmethod
    def from_payload(cls, value: object) -> "ArtifactCacheQuarantineRequest":
        if (
            type(value) is not dict
            or any(type(key) is not str for key in value)
            or set(value)
            != {"node_id", "cache_kind", "cache_identity_digest"}
        ):
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_PAYLOAD_REJECTED")
        node_id = value["node_id"]
        try:
            parsed_node_id = uuid.UUID(node_id) if type(node_id) is str else None
        except ValueError as exc:
            raise ArtifactCacheQuarantineError(
                "CACHE_QUARANTINE_PAYLOAD_REJECTED"
            ) from exc
        if (
            parsed_node_id is None
            or str(parsed_node_id) != node_id
            or parsed_node_id.version != 4
            or value["cache_kind"]
            not in {MODEL_CACHE_KIND_FULL_SNAPSHOT, MODEL_CACHE_KIND_STAGE}
            or type(value["cache_identity_digest"]) is not str
            or _DIGEST.fullmatch(value["cache_identity_digest"]) is None
        ):
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_PAYLOAD_REJECTED")
        return cls(
            node_id=node_id,
            cache_kind=value["cache_kind"],
            cache_identity_digest=value["cache_identity_digest"],
        )


def _safe_owned_directory(path: Path) -> bool:
    try:
        observed = path.lstat()
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        return False
    return (
        stat.S_ISDIR(observed.st_mode)
        and observed.st_uid == os.geteuid()
        and not observed.st_mode & 0o022
        and resolved == Path(os.path.abspath(path))
    )


def _entry_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_IO_FAILED") from exc
    return True


def _fsync_directory(path: Path) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_DIRECTORY", 0),
        )
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_mode & 0o022
        ):
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_ROOT_UNSAFE")
        os.fsync(descriptor)
    except ArtifactCacheQuarantineError:
        raise
    except OSError as exc:
        raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_IO_FAILED") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _rename_noreplace(source: Path, target: Path) -> None:
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = libc.renameat2
    except (AttributeError, OSError) as exc:
        raise ArtifactCacheQuarantineError(
            "CACHE_QUARANTINE_ATOMIC_RENAME_UNAVAILABLE"
        ) from exc
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    if (
        renameat2(
            AT_FDCWD,
            os.fsencode(source),
            AT_FDCWD,
            os.fsencode(target),
            RENAME_NOREPLACE,
        )
        == 0
    ):
        return
    observed_errno = ctypes.get_errno()
    if observed_errno in {errno.EEXIST, errno.ENOTEMPTY}:
        raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_TARGET_EXISTS")
    if observed_errno in {errno.ENOSYS, errno.EINVAL, errno.ENOTSUP}:
        raise ArtifactCacheQuarantineError(
            "CACHE_QUARANTINE_ATOMIC_RENAME_UNAVAILABLE"
        )
    raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_IO_FAILED")


class ArtifactCacheQuarantineExecutor:
    """Move one exact, inactive canonical cache into a fixed retained area."""

    def __init__(
        self,
        node_id: str,
        *,
        runner: Runner | None = None,
        model_root: Path = DURE_MODEL_CACHE_ROOT,
    ) -> None:
        self.node_id = node_id
        self.runner = runner or SubprocessRunner()
        self.model_root = Path(model_root)

    def _paths(
        self, request: ArtifactCacheQuarantineRequest, task_id: str
    ) -> tuple[Path, Path, Path]:
        if not self.model_root.is_absolute():
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_ROOT_UNSAFE")
        suffix = request.cache_identity_digest.removeprefix("sha256:")
        source_root = (
            self.model_root / "stages"
            if request.cache_kind == MODEL_CACHE_KIND_STAGE
            else self.model_root
        )
        source = source_root / f"sha256-{suffix}"
        quarantine_root = self.model_root / DURE_CACHE_QUARANTINE_DIRECTORY
        target = quarantine_root / (
            f"{task_id}-{request.cache_kind.lower()}-sha256-{suffix}"
        )
        return source, quarantine_root, target

    def _require_inactive(self, source: Path) -> None:
        if not self.runner.exists("docker"):
            raise ArtifactCacheQuarantineError(
                "CACHE_QUARANTINE_ACTIVITY_UNKNOWN"
            )
        listed = self.runner.run(
            [
                "docker",
                "ps",
                "--filter",
                "label=dure.deployment",
                "--format",
                "{{.ID}}",
            ],
            timeout=15,
        )
        if not listed.ok:
            raise ArtifactCacheQuarantineError(
                "CACHE_QUARANTINE_ACTIVITY_UNKNOWN"
            )
        container_ids = [
            line.strip() for line in listed.stdout.splitlines() if line.strip()
        ]
        if (
            len(container_ids) > MAX_DURE_CONTAINERS
            or any(_CONTAINER_ID.fullmatch(item) is None for item in container_ids)
        ):
            raise ArtifactCacheQuarantineError(
                "CACHE_QUARANTINE_ACTIVITY_UNKNOWN"
            )
        source_value = str(source)
        for container_id in container_ids:
            inspected = self.runner.run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{json .Mounts}}",
                    container_id,
                ],
                timeout=15,
            )
            if (
                not inspected.ok
                or len(inspected.stdout.encode("utf-8")) > 1024 * 1024
            ):
                raise ArtifactCacheQuarantineError(
                    "CACHE_QUARANTINE_ACTIVITY_UNKNOWN"
                )
            try:
                mounts = json.loads(inspected.stdout)
            except (RecursionError, ValueError) as exc:
                raise ArtifactCacheQuarantineError(
                    "CACHE_QUARANTINE_ACTIVITY_UNKNOWN"
                ) from exc
            if type(mounts) is not list:
                raise ArtifactCacheQuarantineError(
                    "CACHE_QUARANTINE_ACTIVITY_UNKNOWN"
                )
            for mount in mounts:
                if type(mount) is not dict or type(mount.get("Source")) is not str:
                    raise ArtifactCacheQuarantineError(
                        "CACHE_QUARANTINE_ACTIVITY_UNKNOWN"
                    )
                mount_source = Path(mount["Source"])
                if not mount_source.is_absolute():
                    raise ArtifactCacheQuarantineError(
                        "CACHE_QUARANTINE_ACTIVITY_UNKNOWN"
                    )
                if (
                    mount["Source"] == source_value
                    or source.is_relative_to(mount_source)
                    or mount_source.is_relative_to(source)
                ):
                    raise ArtifactCacheQuarantineError(
                        "CACHE_QUARANTINE_CACHE_ACTIVE"
                    )

    def execute(self, task: object) -> dict[str, object]:
        if type(task) is not dict or task.get("type") != "QUARANTINE_ARTIFACT_CACHE":
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_PAYLOAD_REJECTED")
        task_id = task.get("id")
        try:
            parsed_task_id = uuid.UUID(task_id) if type(task_id) is str else None
        except ValueError as exc:
            raise ArtifactCacheQuarantineError(
                "CACHE_QUARANTINE_PAYLOAD_REJECTED"
            ) from exc
        if (
            parsed_task_id is None
            or str(parsed_task_id) != task_id
            or parsed_task_id.version != 4
        ):
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_PAYLOAD_REJECTED")
        request = ArtifactCacheQuarantineRequest.from_payload(task.get("payload"))
        if request.node_id != self.node_id:
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_PAYLOAD_REJECTED")
        source, quarantine_root, target = self._paths(request, task_id)
        if not _safe_owned_directory(self.model_root):
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_ROOT_UNSAFE")
        source_exists = _entry_exists(source)
        target_exists = _entry_exists(target)
        if not source_exists:
            if target_exists and _safe_owned_directory(target):
                if (
                    not _safe_owned_directory(quarantine_root)
                    or not _safe_owned_directory(source.parent)
                ):
                    raise ArtifactCacheQuarantineError(
                        "CACHE_QUARANTINE_ROOT_UNSAFE"
                    )
                _fsync_directory(quarantine_root)
                _fsync_directory(source.parent)
                return {
                    "node_id": request.node_id,
                    "cache_kind": request.cache_kind,
                    "cache_identity_digest": request.cache_identity_digest,
                    "status": "ALREADY_QUARANTINED",
                }
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_SOURCE_MISSING")
        if not _safe_owned_directory(source):
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_SOURCE_UNSAFE")
        if not _safe_owned_directory(source.parent):
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_ROOT_UNSAFE")
        if target_exists:
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_TARGET_EXISTS")
        self._require_inactive(source)
        try:
            quarantine_root.mkdir(mode=0o700, exist_ok=True)
        except OSError as exc:
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_IO_FAILED") from exc
        if not _safe_owned_directory(quarantine_root):
            raise ArtifactCacheQuarantineError("CACHE_QUARANTINE_ROOT_UNSAFE")
        _fsync_directory(self.model_root)
        _rename_noreplace(source, target)
        _fsync_directory(quarantine_root)
        _fsync_directory(source.parent)
        return {
            "node_id": request.node_id,
            "cache_kind": request.cache_kind,
            "cache_identity_digest": request.cache_identity_digest,
            "status": "QUARANTINED",
        }
