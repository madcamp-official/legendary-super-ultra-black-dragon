from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import json
import os
import secrets
import shutil
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Protocol

from .artifact_manifest import (
    CanonicalArtifactManifest,
    parse_artifact_manifest,
    require_sha256_digest,
)
from .model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_MARKER_FILE,
    ModelCacheMarkerError,
    build_model_cache_marker,
    read_model_cache_marker,
)


DURE_MODEL_STORE_ROOT = Path("/var/lib/dure/model-store")
DURE_MODEL_CACHE_ROOT = Path("/var/lib/dure/models")
DURE_MODEL_STAGING_DIRECTORY = ".dure-staging"
DURE_MODEL_STAGING_WORK_DIRECTORY = ".dure-work"
ATTEMPT_JOURNAL_SCHEMA_VERSION = 1
MAX_ATTEMPT_JOURNAL_BYTES = 16 * 1024
MAX_MODEL_CONFIG_BYTES = 1024 * 1024
MAX_TRACKED_BYTES = (1 << 63) - 1
HASH_BUFFER_BYTES = 1024 * 1024
DEFAULT_DISK_RESERVE_BYTES = 64 * 1024 * 1024
AT_FDCWD = -100
RENAME_NOREPLACE = 1

ATTEMPT_STATUSES = frozenset(
    {
        "PREPARING",
        "DOWNLOADING",
        "ASSEMBLING",
        "VERIFYING",
        "ACTIVATING",
        "SUCCEEDED",
        "FAILED",
    }
)
MODEL_STORE_FAILURE_CODES = frozenset(
    {
        "MODEL_STORE_INVALID",
        "MODEL_STORE_ROOT_UNSAFE",
        "MODEL_STORE_PATH_COLLISION",
        "MODEL_STORE_LOCK_BUSY",
        "MODEL_STORE_JOURNAL_CORRUPT",
        "MODEL_STORE_CHUNK_COLLISION",
        "MODEL_STORE_CHUNK_CORRUPT",
        "MODEL_STORE_IO_FAILED",
        "MODEL_STORE_DISK_INSUFFICIENT",
        "MODEL_STORE_DOWNLOAD_TIMEOUT",
        "MODEL_STORE_DOWNLOAD_INTERRUPTED",
        "MODEL_STORE_DOWNLOAD_REJECTED",
        "MODEL_STORE_DIGEST_MISMATCH",
        "MODEL_STORE_MANIFEST_MISMATCH",
        "MODEL_STORE_CACHE_KIND_UNSUPPORTED",
        "MODEL_STORE_FILE_INTEGRITY_FAILED",
        "MODEL_STORE_TARGET_COLLISION",
        "MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE",
    }
)
_JOURNAL_KEYS = frozenset(
    {
        "schema_version",
        "manifest_digest",
        "chunk_digest",
        "bytes_complete",
        "status",
        "failure_code",
    }
)
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_NONBLOCK = getattr(os, "O_NONBLOCK", 0)


class ModelStoreError(RuntimeError):
    _SAFE_MESSAGES = {
        "MODEL_STORE_INVALID": "model store input is invalid",
        "MODEL_STORE_ROOT_UNSAFE": "model store root is unsafe",
        "MODEL_STORE_PATH_COLLISION": "model store path collision detected",
        "MODEL_STORE_LOCK_BUSY": "model store lock is busy",
        "MODEL_STORE_JOURNAL_CORRUPT": "model store attempt journal is corrupt",
        "MODEL_STORE_CHUNK_COLLISION": "model store chunk path collision detected",
        "MODEL_STORE_CHUNK_CORRUPT": "model store chunk failed integrity validation",
        "MODEL_STORE_IO_FAILED": "model store I/O failed",
        "MODEL_STORE_DISK_INSUFFICIENT": "model store has insufficient disk space",
        "MODEL_STORE_DOWNLOAD_TIMEOUT": "model store download timed out",
        "MODEL_STORE_DOWNLOAD_INTERRUPTED": "model store download was interrupted",
        "MODEL_STORE_DOWNLOAD_REJECTED": "model store download response was rejected",
        "MODEL_STORE_DIGEST_MISMATCH": "model store content digest did not match",
        "MODEL_STORE_MANIFEST_MISMATCH": "model store manifest identity did not match",
        "MODEL_STORE_CACHE_KIND_UNSUPPORTED": "model store cache kind is not supported",
        "MODEL_STORE_FILE_INTEGRITY_FAILED": "model store file integrity validation failed",
        "MODEL_STORE_TARGET_COLLISION": "model store target collision detected",
        "MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE": "atomic no-replace activation is unavailable",
    }

    def __init__(self, code: str) -> None:
        if code not in MODEL_STORE_FAILURE_CODES:
            raise ValueError("unsupported model store failure code")
        self.code = code
        self.failure_code = code
        super().__init__(self._SAFE_MESSAGES[code])


@dataclass(frozen=True)
class AttemptJournal:
    manifest_digest: str
    chunk_digest: str | None
    bytes_complete: int
    status: str
    failure_code: str | None = None

    def __post_init__(self) -> None:
        try:
            require_sha256_digest(self.manifest_digest, field="manifest_digest")
            if self.chunk_digest is not None:
                require_sha256_digest(self.chunk_digest, field="chunk_digest")
        except ValueError as exc:
            raise ModelStoreError("MODEL_STORE_INVALID") from exc
        if (
            type(self.bytes_complete) is not int
            or not 0 <= self.bytes_complete <= MAX_TRACKED_BYTES
            or type(self.status) is not str
            or self.status not in ATTEMPT_STATUSES
            or (
                self.failure_code is not None
                and (
                    type(self.failure_code) is not str
                    or self.failure_code not in MODEL_STORE_FAILURE_CODES
                )
            )
        ):
            raise ModelStoreError("MODEL_STORE_INVALID")
        if (self.status == "FAILED") != (self.failure_code is not None):
            raise ModelStoreError("MODEL_STORE_INVALID")

    def to_dict(self) -> dict:
        return {
            "schema_version": ATTEMPT_JOURNAL_SCHEMA_VERSION,
            "manifest_digest": self.manifest_digest,
            "chunk_digest": self.chunk_digest,
            "bytes_complete": self.bytes_complete,
            "status": self.status,
            "failure_code": self.failure_code,
        }

    @classmethod
    def from_dict(cls, value: object) -> "AttemptJournal":
        if (
            type(value) is not dict
            or any(type(key) is not str for key in value)
            or set(value) != _JOURNAL_KEYS
            or type(value.get("schema_version")) is not int
            or value["schema_version"] != ATTEMPT_JOURNAL_SCHEMA_VERSION
        ):
            raise ModelStoreError("MODEL_STORE_JOURNAL_CORRUPT")
        try:
            return cls(
                manifest_digest=value["manifest_digest"],
                chunk_digest=value["chunk_digest"],
                bytes_complete=value["bytes_complete"],
                status=value["status"],
                failure_code=value["failure_code"],
            )
        except ModelStoreError as exc:
            raise ModelStoreError("MODEL_STORE_JOURNAL_CORRUPT") from exc


def _digest_hex(digest: object, *, field: str = "digest") -> str:
    try:
        normalized = require_sha256_digest(digest, field=field)
    except ValueError as exc:
        raise ModelStoreError("MODEL_STORE_INVALID") from exc
    return normalized.removeprefix("sha256:")


def _normalized_absolute(path: Path) -> Path:
    if not path.is_absolute():
        raise ModelStoreError("MODEL_STORE_ROOT_UNSAFE")
    return Path(os.path.abspath(path))


def _reject_symlink_ancestors(path: Path) -> None:
    normalized = _normalized_absolute(path)
    for candidate in reversed((normalized, *normalized.parents)):
        try:
            observed = candidate.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_ROOT_UNSAFE") from exc
        if stat.S_ISLNK(observed.st_mode):
            raise ModelStoreError("MODEL_STORE_ROOT_UNSAFE")


def _assert_safe_directory(path: Path, *, root: bool = False) -> None:
    try:
        observed = path.lstat()
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ModelStoreError("MODEL_STORE_ROOT_UNSAFE") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or resolved != _normalized_absolute(path)
        or observed.st_uid != os.geteuid()
        or observed.st_mode & 0o022
    ):
        code = "MODEL_STORE_ROOT_UNSAFE" if root else "MODEL_STORE_PATH_COLLISION"
        raise ModelStoreError(code)


def _ensure_safe_directory(path: Path, *, root: bool = False) -> None:
    normalized = _normalized_absolute(path)
    _reject_symlink_ancestors(normalized)
    missing: list[Path] = []
    candidate = normalized
    while True:
        try:
            candidate.lstat()
        except FileNotFoundError:
            missing.append(candidate)
            parent = candidate.parent
            if parent == candidate:
                raise ModelStoreError("MODEL_STORE_ROOT_UNSAFE")
            candidate = parent
            continue
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_ROOT_UNSAFE") from exc
        break

    _assert_safe_directory(candidate, root=True)
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            pass
        except OSError as exc:
            code = "MODEL_STORE_ROOT_UNSAFE" if root else "MODEL_STORE_IO_FAILED"
            raise ModelStoreError(code) from exc
        _assert_safe_directory(directory, root=root and directory == normalized)
        _fsync_directory(directory.parent)
    if normalized.parent != normalized:
        _assert_safe_directory(normalized.parent, root=True)
    _assert_safe_directory(normalized, root=root)


def _fsync_directory(path: Path) -> None:
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | _CLOEXEC | _NOFOLLOW)
        os.fsync(descriptor)
    except OSError as exc:
        raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


class ContentAddressedModelStore:
    """Dure-owned content-addressed chunk state.

    Root overrides exist for unit tests and local embedding only.  Task payloads
    must never be allowed to populate either root.
    """

    def __init__(
        self,
        *,
        store_root: Path = DURE_MODEL_STORE_ROOT,
        model_root: Path = DURE_MODEL_CACHE_ROOT,
    ) -> None:
        self.store_root = _normalized_absolute(Path(store_root))
        self.model_root = _normalized_absolute(Path(model_root))
        self.chunk_root = self.store_root / "chunks" / "sha256"
        self.artifact_lock_root = self.store_root / "locks" / "artifacts"
        self.chunk_lock_root = self.store_root / "locks" / "chunks"
        self.attempt_root = self.store_root / "attempts"
        self.model_staging_root = self.model_root / DURE_MODEL_STAGING_DIRECTORY

    def initialize(self) -> None:
        _ensure_safe_directory(self.store_root, root=True)
        for path in (
            self.chunk_root,
            self.artifact_lock_root,
            self.chunk_lock_root,
            self.attempt_root,
        ):
            _ensure_safe_directory(path)

    def initialize_model_layout(self) -> None:
        _ensure_safe_directory(self.model_root, root=True)
        _ensure_safe_directory(self.model_staging_root)

    def model_cache_path(self, manifest_digest: str) -> Path:
        hexadecimal = _digest_hex(manifest_digest, field="manifest_digest")
        return self.model_root / f"sha256-{hexadecimal}"

    def model_staging_path(self, manifest_digest: str) -> Path:
        hexadecimal = _digest_hex(manifest_digest, field="manifest_digest")
        return self.model_staging_root / f"{hexadecimal}.assembling"

    def create_model_staging_directory(self, manifest_digest: str) -> Path:
        self.initialize_model_layout()
        candidate = self.model_staging_path(manifest_digest)
        created = False
        try:
            candidate.mkdir(mode=0o700)
            created = True
        except FileExistsError:
            pass
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        _assert_safe_directory(candidate)
        if created:
            _fsync_directory(self.model_staging_root)
        return candidate

    def chunk_path(self, digest: str) -> Path:
        hexadecimal = _digest_hex(digest, field="chunk_digest")
        return self.chunk_root / hexadecimal[:2] / hexadecimal

    def chunk_partial_path(self, digest: str) -> Path:
        path = self.chunk_path(digest)
        return path.with_name(f"{path.name}.part")

    def ensure_chunk_directory(self, digest: str) -> Path:
        self.initialize()
        directory = self.chunk_path(digest).parent
        _ensure_safe_directory(directory)
        return directory

    def _lock_path(self, kind: str, digest: str) -> Path:
        hexadecimal = _digest_hex(digest)
        if kind == "artifact":
            return self.artifact_lock_root / f"{hexadecimal}.lock"
        if kind == "chunk":
            return self.chunk_lock_root / f"{hexadecimal}.lock"
        raise ModelStoreError("MODEL_STORE_INVALID")

    @contextmanager
    def _lock(
        self,
        kind: str,
        digest: str,
        *,
        blocking: bool,
    ) -> Iterator[Path]:
        self.initialize()
        path = self._lock_path(kind, digest)
        descriptor = -1
        acquired = False
        try:
            descriptor = os.open(
                path,
                os.O_RDWR | os.O_CREAT | _CLOEXEC | _NOFOLLOW,
                0o600,
            )
            observed = os.fstat(descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or observed.st_nlink != 1
                or observed.st_mode & 0o077
            ):
                raise ModelStoreError("MODEL_STORE_PATH_COLLISION")
            operation = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
            try:
                fcntl.flock(descriptor, operation)
            except BlockingIOError as exc:
                raise ModelStoreError("MODEL_STORE_LOCK_BUSY") from exc
            acquired = True
            yield path
        except ModelStoreError:
            raise
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.EISDIR, errno.ENOTDIR}:
                raise ModelStoreError("MODEL_STORE_PATH_COLLISION") from exc
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        finally:
            if descriptor >= 0:
                if acquired:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                    except OSError:
                        pass
                os.close(descriptor)

    def artifact_lock(
        self, manifest_digest: str, *, blocking: bool = True
    ) -> Iterator[Path]:
        return self._lock("artifact", manifest_digest, blocking=blocking)

    def chunk_lock(
        self, chunk_digest: str, *, blocking: bool = True
    ) -> Iterator[Path]:
        return self._lock("chunk", chunk_digest, blocking=blocking)

    def _verified_chunk_without_lock(
        self,
        chunk_digest: str,
        expected_size: int,
        *,
        allowed_link_counts: frozenset[int] = frozenset({1}),
    ) -> Path | None:
        if (
            type(expected_size) is not int
            or not 1 <= expected_size <= MAX_TRACKED_BYTES
        ):
            raise ModelStoreError("MODEL_STORE_INVALID")
        path = self.chunk_path(chunk_digest)
        try:
            parent_state = path.parent.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        if not stat.S_ISDIR(parent_state.st_mode):
            raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION")
        try:
            _assert_safe_directory(path.parent)
        except ModelStoreError as exc:
            raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION") from exc
        try:
            path_state = path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        if path_state.st_nlink == 2 and allowed_link_counts == frozenset({1}):
            partial = self.chunk_partial_path(chunk_digest)
            try:
                partial_state = partial.lstat()
            except FileNotFoundError:
                partial_state = None
            except OSError as exc:
                raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
            if (
                partial_state is not None
                and stat.S_ISREG(partial_state.st_mode)
                and partial_state.st_dev == path_state.st_dev
                and partial_state.st_ino == path_state.st_ino
                and partial_state.st_nlink == 2
            ):
                self._verified_chunk_without_lock(
                    chunk_digest,
                    expected_size,
                    allowed_link_counts=frozenset({2}),
                )
                self._unlink_verified_partial(partial)
                return self._verified_chunk_without_lock(
                    chunk_digest, expected_size
                )
        if (
            not stat.S_ISREG(path_state.st_mode)
            or path_state.st_uid != os.geteuid()
            or path_state.st_nlink not in allowed_link_counts
            or path_state.st_mode & 0o022
            or path_state.st_size != expected_size
        ):
            raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION")

        descriptor = -1
        try:
            descriptor = os.open(
                path, os.O_RDONLY | _CLOEXEC | _NOFOLLOW | _NONBLOCK
            )
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_dev != path_state.st_dev
                or before.st_ino != path_state.st_ino
                or before.st_uid != os.geteuid()
                or before.st_nlink not in allowed_link_counts
                or before.st_mode & 0o022
                or before.st_size != expected_size
            ):
                raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION")
            digest = hashlib.sha256()
            while True:
                chunk = os.read(descriptor, HASH_BUFFER_BYTES)
                if not chunk:
                    break
                digest.update(chunk)
            after = os.fstat(descriptor)
        except ModelStoreError:
            raise
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.EISDIR, errno.ENOTDIR}:
                raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION") from exc
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

        identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        observed_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if identity != observed_after:
            raise ModelStoreError("MODEL_STORE_CHUNK_CORRUPT")
        if digest.hexdigest() != _digest_hex(chunk_digest, field="chunk_digest"):
            raise ModelStoreError("MODEL_STORE_CHUNK_CORRUPT")
        return path

    def verified_chunk(
        self,
        chunk_digest: str,
        expected_size: int,
        *,
        blocking: bool = True,
    ) -> Path | None:
        with self.chunk_lock(chunk_digest, blocking=blocking):
            return self._verified_chunk_without_lock(chunk_digest, expected_size)

    def _required_chunk_allocation_without_lock(
        self, chunk_digest: str, expected_size: int
    ) -> int:
        if self._verified_chunk_without_lock(chunk_digest, expected_size) is not None:
            return 0
        partial = self.chunk_partial_path(chunk_digest)
        descriptor = -1
        try:
            observed = partial.lstat()
        except FileNotFoundError:
            return expected_size
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        try:
            descriptor = os.open(
                partial, os.O_RDONLY | _CLOEXEC | _NOFOLLOW | _NONBLOCK
            )
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or observed.st_nlink != 1
                or observed.st_mode & 0o077
                or observed.st_size > expected_size
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_dev != observed.st_dev
                or opened.st_ino != observed.st_ino
                or opened.st_uid != os.geteuid()
                or opened.st_nlink != 1
                or opened.st_mode & 0o077
                or opened.st_size != observed.st_size
                or type(opened.st_blocks) is not int
                or opened.st_blocks < 0
            ):
                raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION")
            allocated = min(expected_size, opened.st_blocks * 512)
            return expected_size - allocated
        except ModelStoreError:
            raise
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.EISDIR, errno.ENOTDIR}:
                raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION") from exc
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def open_chunk_partial(
        self, chunk_digest: str, expected_size: int
    ) -> tuple[Path, int, int]:
        if (
            type(expected_size) is not int
            or not 1 <= expected_size <= MAX_TRACKED_BYTES
        ):
            raise ModelStoreError("MODEL_STORE_INVALID")
        self.ensure_chunk_directory(chunk_digest)
        path = self.chunk_partial_path(chunk_digest)
        descriptor = -1
        try:
            descriptor = os.open(
                path,
                os.O_RDWR | os.O_CREAT | _CLOEXEC | _NOFOLLOW,
                0o600,
            )
            observed = os.fstat(descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or observed.st_nlink != 1
                or observed.st_mode & 0o077
                or observed.st_size > expected_size
            ):
                raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION")
            return path, descriptor, observed.st_size
        except ModelStoreError:
            if descriptor >= 0:
                os.close(descriptor)
            raise
        except OSError as exc:
            if descriptor >= 0:
                os.close(descriptor)
            if exc.errno in {errno.ELOOP, errno.EISDIR, errno.ENOTDIR}:
                raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION") from exc
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc

    def _unlink_verified_partial(self, path: Path) -> None:
        try:
            path.unlink()
            _fsync_directory(path.parent)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc

    def publish_chunk_partial(
        self, chunk_digest: str, expected_size: int
    ) -> Path:
        """Publish an already fsynced and verified ``.part`` without overwrite.

        The caller must hold the matching chunk lock.  This method revalidates
        both paths so a collision is preserved for operator inspection.
        """

        partial = self.chunk_partial_path(chunk_digest)
        final = self.chunk_path(chunk_digest)
        try:
            partial_state = partial.lstat()
        except FileNotFoundError as exc:
            raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION") from exc
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        if (
            not stat.S_ISREG(partial_state.st_mode)
            or partial_state.st_uid != os.geteuid()
            or partial_state.st_nlink not in {1, 2}
            or partial_state.st_mode & 0o077
            or partial_state.st_size != expected_size
        ):
            raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION")

        try:
            final_state = final.lstat()
        except FileNotFoundError:
            final_state = None
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc

        if final_state is not None:
            same_publication = (
                stat.S_ISREG(final_state.st_mode)
                and final_state.st_dev == partial_state.st_dev
                and final_state.st_ino == partial_state.st_ino
                and final_state.st_nlink == partial_state.st_nlink == 2
            )
            if same_publication:
                self._verified_chunk_without_lock(
                    chunk_digest,
                    expected_size,
                    allowed_link_counts=frozenset({2}),
                )
                self._unlink_verified_partial(partial)
                verified = self._verified_chunk_without_lock(
                    chunk_digest, expected_size
                )
                if verified is None:  # pragma: no cover - lock excludes Dure races
                    raise ModelStoreError("MODEL_STORE_IO_FAILED")
                return verified

            if partial_state.st_nlink != 1:
                raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION")

            verified = self._verified_chunk_without_lock(
                chunk_digest, expected_size
            )
            if verified is None:  # pragma: no cover - lstat observed it above
                raise ModelStoreError("MODEL_STORE_IO_FAILED")
            self._unlink_verified_partial(partial)
            return verified

        if partial_state.st_nlink != 1:
            raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION")

        try:
            os.link(partial, final, follow_symlinks=False)
            _fsync_directory(final.parent)
        except FileExistsError:
            verified = self._verified_chunk_without_lock(
                chunk_digest, expected_size
            )
            if verified is None:  # pragma: no cover - EEXIST guarantees an entry
                raise ModelStoreError("MODEL_STORE_IO_FAILED")
            self._unlink_verified_partial(partial)
            return verified
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc

        self._unlink_verified_partial(partial)
        verified = self._verified_chunk_without_lock(chunk_digest, expected_size)
        if verified is None:  # pragma: no cover - published under the chunk lock
            raise ModelStoreError("MODEL_STORE_IO_FAILED")
        return verified

    def attempt_journal_path(self, manifest_digest: str) -> Path:
        hexadecimal = _digest_hex(manifest_digest, field="manifest_digest")
        return self.attempt_root / hexadecimal / "journal.json"

    def _attempt_directory(self, manifest_digest: str) -> Path:
        self.initialize()
        path = self.attempt_journal_path(manifest_digest).parent
        _ensure_safe_directory(path)
        return path

    def read_attempt(self, manifest_digest: str) -> AttemptJournal | None:
        directory = self._attempt_directory(manifest_digest)
        path = directory / "journal.json"
        try:
            observed = path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or observed.st_mode & 0o077
            or observed.st_size <= 0
            or observed.st_size > MAX_ATTEMPT_JOURNAL_BYTES
        ):
            raise ModelStoreError("MODEL_STORE_JOURNAL_CORRUPT")

        descriptor = -1
        try:
            descriptor = os.open(
                path, os.O_RDONLY | _CLOEXEC | _NOFOLLOW | _NONBLOCK
            )
            payload = os.read(descriptor, MAX_ATTEMPT_JOURNAL_BYTES + 1)
            if os.read(descriptor, 1) or len(payload) > MAX_ATTEMPT_JOURNAL_BYTES:
                raise ModelStoreError("MODEL_STORE_JOURNAL_CORRUPT")

            def unique_object(pairs):
                value = {}
                for key, item in pairs:
                    if key in value:
                        raise ValueError("duplicate JSON key")
                    value[key] = item
                return value

            value = json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=unique_object,
            )
        except ModelStoreError:
            raise
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise ModelStoreError("MODEL_STORE_JOURNAL_CORRUPT") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        journal = AttemptJournal.from_dict(value)
        if journal.manifest_digest != manifest_digest:
            raise ModelStoreError("MODEL_STORE_JOURNAL_CORRUPT")
        return journal

    def write_attempt(self, journal: AttemptJournal) -> Path:
        if type(journal) is not AttemptJournal:
            raise ModelStoreError("MODEL_STORE_INVALID")
        directory = self._attempt_directory(journal.manifest_digest)
        path = directory / "journal.json"
        try:
            path_state = path.lstat()
        except FileNotFoundError:
            path_state = None
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        if path_state is not None:
            if (
                not stat.S_ISREG(path_state.st_mode)
                or path_state.st_uid != os.geteuid()
                or path_state.st_nlink != 1
                or path_state.st_mode & 0o077
            ):
                raise ModelStoreError("MODEL_STORE_PATH_COLLISION")
            self.read_attempt(journal.manifest_digest)

        payload = (
            json.dumps(
                journal.to_dict(),
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        if len(payload) > MAX_ATTEMPT_JOURNAL_BYTES:
            raise ModelStoreError("MODEL_STORE_INVALID")
        temporary = directory / f".journal.{secrets.token_hex(8)}.tmp"
        descriptor = -1
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | _CLOEXEC | _NOFOLLOW,
                0o600,
            )
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError(errno.EIO, "short journal write")
                view = view[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary, path)
            _fsync_directory(directory)
        except ModelStoreError:
            raise
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass
        return path


@dataclass(frozen=True)
class CacheIdentity:
    repository: str
    revision: str
    manifest_digest: str
    quantization: str
    cache_kind: str = MODEL_CACHE_KIND_FULL_SNAPSHOT

    def __post_init__(self) -> None:
        try:
            build_model_cache_marker(
                repository=self.repository,
                revision=self.revision,
                manifest_digest=self.manifest_digest,
                quantization=self.quantization,
                cache_kind=self.cache_kind,
            )
        except (ModelCacheMarkerError, TypeError, ValueError) as exc:
            raise ModelStoreError("MODEL_STORE_INVALID") from exc

    def marker(self) -> dict[str, str | int]:
        return build_model_cache_marker(
            repository=self.repository,
            revision=self.revision,
            manifest_digest=self.manifest_digest,
            quantization=self.quantization,
            cache_kind=self.cache_kind,
        )


@dataclass(frozen=True)
class PreparedModelCache:
    path: Path
    identity: CacheIdentity
    reused: bool
    file_count: int
    total_size_bytes: int


class LockedChunkDownloader(Protocol):
    def download_chunk_locked(
        self,
        *,
        origin: object,
        manifest_digest: str,
        chunk_digest: str,
        expected_size: int,
    ) -> str: ...


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        try:
            written = os.write(descriptor, view)
        except OSError as exc:
            if exc.errno == errno.ENOSPC:
                raise ModelStoreError("MODEL_STORE_DISK_INSUFFICIENT") from exc
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        if written <= 0:
            raise ModelStoreError("MODEL_STORE_IO_FAILED")
        view = view[written:]


def _safe_regular_digest(path: Path, expected_size: int) -> str:
    descriptor = -1
    try:
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or observed.st_mode & 0o022
            or observed.st_size != expected_size
        ):
            raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
        descriptor = os.open(
            path, os.O_RDONLY | _CLOEXEC | _NOFOLLOW | _NONBLOCK
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_dev != observed.st_dev
            or before.st_ino != observed.st_ino
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or before.st_size != expected_size
        ):
            raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
        digest = hashlib.sha256()
        count = 0
        while count <= expected_size:
            block = os.read(
                descriptor,
                min(HASH_BUFFER_BYTES, expected_size - count + 1),
            )
            if not block:
                break
            count += len(block)
            if count > expected_size:
                raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
            digest.update(block)
        after = os.fstat(descriptor)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if count != expected_size or identity_before != identity_after:
            raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
        return digest.hexdigest()
    except ModelStoreError:
        raise
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EISDIR, errno.ENOTDIR}:
            raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED") from exc
        raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_verified_model_config(path: Path, item: dict) -> dict:
    expected_size = item["size_bytes"]
    if expected_size > MAX_MODEL_CONFIG_BYTES:
        raise ModelStoreError("MODEL_STORE_MANIFEST_MISMATCH")
    descriptor = -1
    try:
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or observed.st_mode & 0o022
            or observed.st_size != expected_size
        ):
            raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
        descriptor = os.open(
            path, os.O_RDONLY | _CLOEXEC | _NOFOLLOW | _NONBLOCK
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_dev != observed.st_dev
            or before.st_ino != observed.st_ino
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or before.st_size != expected_size
        ):
            raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
        payload = bytearray()
        while len(payload) <= MAX_MODEL_CONFIG_BYTES:
            block = os.read(
                descriptor,
                min(8192, MAX_MODEL_CONFIG_BYTES + 1 - len(payload)),
            )
            if not block:
                break
            payload.extend(block)
        after = os.fstat(descriptor)
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if (
            len(payload) != expected_size
            or before_identity != after_identity
            or hashlib.sha256(payload).hexdigest()
            != item["sha256"].removeprefix("sha256:")
        ):
            raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")

        def unique_object(pairs: list[tuple[str, object]]) -> dict:
            result: dict = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate model config key")
                result[key] = value
            return result

        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=unique_object,
        )
        if type(value) is not dict:
            raise ValueError("model config must be an object")
        return value
    except ModelStoreError:
        raise
    except (OSError, RecursionError, UnicodeError, ValueError) as exc:
        raise ModelStoreError("MODEL_STORE_MANIFEST_MISMATCH") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _validate_model_config(
    staging: Path,
    manifest: CanonicalArtifactManifest,
    identity: CacheIdentity,
) -> None:
    item = next(
        entry
        for entry in manifest.document["files"]
        if entry["path"] == "config.json"
    )
    config = _read_verified_model_config(staging / "config.json", item)
    quantization = config.get("quantization_config")
    if quantization is None:
        return
    if type(quantization) is not dict:
        raise ModelStoreError("MODEL_STORE_MANIFEST_MISMATCH")
    primary = quantization.get("quant_method")
    alternate = quantization.get("quantization_method")
    if (
        primary is not None
        and alternate is not None
        and primary != alternate
    ):
        raise ModelStoreError("MODEL_STORE_MANIFEST_MISMATCH")
    method = primary if primary is not None else alternate
    if type(method) is not str or method != identity.quantization:
        raise ModelStoreError("MODEL_STORE_MANIFEST_MISMATCH")


def _expected_tree_paths(
    manifest: CanonicalArtifactManifest,
) -> tuple[set[str], set[str]]:
    files = {item["path"] for item in manifest.document["files"]}
    files.add(MODEL_CACHE_MARKER_FILE)
    directories: set[str] = set()
    for file_path in files:
        parts = Path(file_path).parts
        for length in range(1, len(parts)):
            directories.add(Path(*parts[:length]).as_posix())
    return files, directories


def _staging_part_relative(item: dict) -> str:
    path_digest = hashlib.sha256(item["path"].encode("utf-8")).hexdigest()
    return f"{DURE_MODEL_STAGING_WORK_DIRECTORY}/{path_digest}.part"


def _safe_partial_allocation(path: Path, expected_size: int) -> tuple[int, int]:
    descriptor = -1
    try:
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or observed.st_mode & 0o077
            or not 0 <= observed.st_size <= expected_size
        ):
            raise ModelStoreError("MODEL_STORE_PATH_COLLISION")
        descriptor = os.open(
            path, os.O_RDONLY | _CLOEXEC | _NOFOLLOW | _NONBLOCK
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_dev != observed.st_dev
            or opened.st_ino != observed.st_ino
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or opened.st_mode & 0o077
            or opened.st_size != observed.st_size
            or type(opened.st_blocks) is not int
            or opened.st_blocks < 0
        ):
            raise ModelStoreError("MODEL_STORE_PATH_COLLISION")
        return opened.st_size, min(expected_size, opened.st_blocks * 512)
    except ModelStoreError:
        raise
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EISDIR, errno.ENOTDIR}:
            raise ModelStoreError("MODEL_STORE_PATH_COLLISION") from exc
        raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _scan_exact_tree(root: Path) -> tuple[set[str], set[str]]:
    try:
        root_state = root.lstat()
        resolved_root = root.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ModelStoreError("MODEL_STORE_TARGET_COLLISION") from exc
    if (
        not stat.S_ISDIR(root_state.st_mode)
        or root_state.st_uid != os.geteuid()
        or root_state.st_mode & 0o022
    ):
        raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")

    files: set[str] = set()
    directories: set[str] = set()
    pending: list[tuple[Path, str]] = [(root, "")]
    while pending:
        directory, prefix = pending.pop()
        try:
            state = directory.lstat()
            resolved = directory.resolve(strict=True)
            if (
                not stat.S_ISDIR(state.st_mode)
                or state.st_uid != os.geteuid()
                or state.st_mode & 0o022
                or not resolved.is_relative_to(resolved_root)
            ):
                raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")
            entries = list(os.scandir(directory))
        except ModelStoreError:
            raise
        except (OSError, RuntimeError, ValueError) as exc:
            raise ModelStoreError("MODEL_STORE_TARGET_COLLISION") from exc
        for entry in entries:
            relative = f"{prefix}/{entry.name}".lstrip("/")
            try:
                entry_state = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise ModelStoreError("MODEL_STORE_TARGET_COLLISION") from exc
            if stat.S_ISDIR(entry_state.st_mode):
                directories.add(relative)
                pending.append((Path(entry.path), relative))
            elif stat.S_ISREG(entry_state.st_mode):
                if (
                    entry_state.st_uid != os.geteuid()
                    or entry_state.st_nlink != 1
                    or entry_state.st_mode & 0o022
                ):
                    raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")
                files.add(relative)
            else:
                raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")
    return files, directories


def _verified_staging_bytes(
    root: Path,
    manifest: CanonicalArtifactManifest,
    identity: CacheIdentity,
) -> tuple[int, int]:
    try:
        root.lstat()
    except FileNotFoundError:
        return 0, 0
    except OSError as exc:
        raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc

    expected_files, expected_directories = _expected_tree_paths(manifest)
    items = {item["path"]: item for item in manifest.document["files"]}
    parts = {_staging_part_relative(item): item for item in items.values()}
    actual_files, actual_directories = _scan_exact_tree(root)
    allowed_files = expected_files | set(parts)
    allowed_directories = expected_directories | {
        DURE_MODEL_STAGING_WORK_DIRECTORY
    }
    if not actual_files <= allowed_files or not actual_directories <= allowed_directories:
        raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")

    if MODEL_CACHE_MARKER_FILE in actual_files:
        if actual_files != expected_files or actual_directories != expected_directories:
            raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")
        _verify_cache_tree(root, manifest, identity)
        return manifest.total_size_bytes, 0

    completed = 0
    partial_allocation = 0
    for relative, item in items.items():
        part_relative = _staging_part_relative(item)
        if relative in actual_files and part_relative in actual_files:
            raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")
        if relative not in actual_files:
            continue
        observed = _safe_regular_digest(root / relative, item["size_bytes"])
        if observed != item["sha256"].removeprefix("sha256:"):
            raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
        completed += item["size_bytes"]
    for relative, item in parts.items():
        if relative in actual_files:
            _, allocated = _safe_partial_allocation(
                root / relative, item["size_bytes"]
            )
            partial_allocation += allocated
    if completed > manifest.total_size_bytes:
        raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
    partial_allocation = min(
        partial_allocation,
        manifest.total_size_bytes - completed,
    )
    return completed, partial_allocation


def _verify_cache_tree(
    root: Path,
    manifest: CanonicalArtifactManifest,
    identity: CacheIdentity,
) -> None:
    expected_files, expected_directories = _expected_tree_paths(manifest)
    actual_files, actual_directories = _scan_exact_tree(root)
    if actual_files != expected_files or actual_directories != expected_directories:
        raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")
    try:
        marker = read_model_cache_marker(root / MODEL_CACHE_MARKER_FILE)
    except ModelCacheMarkerError as exc:
        raise ModelStoreError("MODEL_STORE_TARGET_COLLISION") from exc
    if marker.to_dict() != identity.marker():
        raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")
    for item in manifest.document["files"]:
        observed = _safe_regular_digest(root / item["path"], item["size_bytes"])
        if observed != item["sha256"].removeprefix("sha256:"):
            raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
    _validate_model_config(root, manifest, identity)


def _rename_noreplace(source: Path, target: Path) -> None:
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = libc.renameat2
    except (AttributeError, OSError) as exc:
        raise ModelStoreError(
            "MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE"
        ) from exc
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    result = renameat2(
        AT_FDCWD,
        os.fsencode(source),
        AT_FDCWD,
        os.fsencode(target),
        RENAME_NOREPLACE,
    )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")
    if error in {errno.ENOSYS, errno.EINVAL, errno.ENOTSUP}:
        raise ModelStoreError("MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE")
    raise ModelStoreError("MODEL_STORE_IO_FAILED")


class ModelCachePreparer:
    def __init__(
        self,
        store: ContentAddressedModelStore,
        downloader: LockedChunkDownloader,
        *,
        disk_usage: Callable[[Path], object] = shutil.disk_usage,
        disk_reserve_bytes: int = DEFAULT_DISK_RESERVE_BYTES,
    ) -> None:
        if type(store) is not ContentAddressedModelStore or not hasattr(
            downloader, "download_chunk_locked"
        ):
            raise ValueError("model cache preparer dependencies are invalid")
        if (
            type(disk_reserve_bytes) is not int
            or not 0 <= disk_reserve_bytes <= MAX_TRACKED_BYTES
        ):
            raise ValueError("model cache disk reserve is invalid")
        self.store = store
        self.downloader = downloader
        self.disk_usage = disk_usage
        self.disk_reserve_bytes = disk_reserve_bytes

    @staticmethod
    def _journal(
        identity: CacheIdentity,
        status: str,
        bytes_complete: int,
        failure_code: str | None = None,
    ) -> AttemptJournal:
        return AttemptJournal(
            manifest_digest=identity.manifest_digest,
            chunk_digest=None,
            bytes_complete=bytes_complete,
            status=status,
            failure_code=failure_code,
        )

    def _free_bytes(self, path: Path) -> int:
        try:
            usage = self.disk_usage(path)
            free = usage.free
        except (AttributeError, OSError, TypeError, ValueError) as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        if type(free) is not int or free < 0:
            raise ModelStoreError("MODEL_STORE_IO_FAILED")
        return free

    def _missing_chunk_bytes(
        self, chunks: dict[str, int]
    ) -> int:
        missing = 0
        for digest, size in sorted(chunks.items()):
            with self.store.chunk_lock(digest):
                missing += self.store._required_chunk_allocation_without_lock(
                    digest, size
                )
        return missing

    def _check_disk(
        self,
        *,
        missing_chunk_bytes: int,
        assembly_bytes: int,
    ) -> None:
        try:
            store_device = self.store.store_root.stat().st_dev
            model_device = self.store.model_root.stat().st_dev
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        marker_bytes = 64 * 1024
        if store_device == model_device:
            required = (
                missing_chunk_bytes
                + assembly_bytes
                + marker_bytes
                + self.disk_reserve_bytes
            )
            if self._free_bytes(self.store.store_root) < required:
                raise ModelStoreError("MODEL_STORE_DISK_INSUFFICIENT")
            return
        if self._free_bytes(self.store.store_root) < (
            missing_chunk_bytes + self.disk_reserve_bytes
        ):
            raise ModelStoreError("MODEL_STORE_DISK_INSUFFICIENT")
        if self._free_bytes(self.store.model_root) < (
            assembly_bytes + marker_bytes + self.disk_reserve_bytes
        ):
            raise ModelStoreError("MODEL_STORE_DISK_INSUFFICIENT")

    def _create_directories(
        self, staging: Path, manifest: CanonicalArtifactManifest
    ) -> tuple[list[Path], Path]:
        _, expected = _expected_tree_paths(manifest)
        directories = [staging]
        for relative in sorted(expected, key=lambda value: (value.count("/"), value)):
            path = staging / relative
            try:
                path.mkdir(mode=0o700, exist_ok=True)
            except OSError as exc:
                raise ModelStoreError("MODEL_STORE_PATH_COLLISION") from exc
            _assert_safe_directory(path)
            directories.append(path)
        work = staging / DURE_MODEL_STAGING_WORK_DIRECTORY
        try:
            work.mkdir(mode=0o700, exist_ok=True)
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_PATH_COLLISION") from exc
        _assert_safe_directory(work)
        return directories, work

    def _assemble_file(self, staging: Path, work: Path, item: dict) -> None:
        target = staging / item["path"]
        part = staging / _staging_part_relative(item)
        try:
            target_state = target.lstat()
        except FileNotFoundError:
            target_state = None
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        if target_state is not None:
            try:
                part.lstat()
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
            else:
                raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")
            observed = _safe_regular_digest(target, item["size_bytes"])
            if observed != item["sha256"].removeprefix("sha256:"):
                raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
            return

        descriptor = -1
        try:
            descriptor = os.open(
                part,
                os.O_RDWR | os.O_CREAT | _CLOEXEC | _NOFOLLOW,
                0o600,
            )
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.geteuid()
                or before.st_nlink != 1
                or before.st_mode & 0o077
                or not 0 <= before.st_size <= item["size_bytes"]
            ):
                raise ModelStoreError("MODEL_STORE_PATH_COLLISION")
            os.lseek(descriptor, 0, os.SEEK_SET)
            existing_remaining = before.st_size
            file_digest = hashlib.sha256()
            written_total = 0
            for chunk in item["chunks"]:
                with self.store.chunk_lock(chunk["sha256"]):
                    source = self.store._verified_chunk_without_lock(
                        chunk["sha256"], chunk["length_bytes"]
                    )
                    if source is None:
                        raise ModelStoreError("MODEL_STORE_CHUNK_CORRUPT")
                    source_descriptor = -1
                    try:
                        source_descriptor = os.open(
                            source,
                            os.O_RDONLY | _CLOEXEC | _NOFOLLOW | _NONBLOCK,
                        )
                        source_state = os.fstat(source_descriptor)
                        if (
                            not stat.S_ISREG(source_state.st_mode)
                            or source_state.st_uid != os.geteuid()
                            or source_state.st_nlink != 1
                            or source_state.st_mode & 0o022
                            or source_state.st_size != chunk["length_bytes"]
                        ):
                            raise ModelStoreError("MODEL_STORE_CHUNK_CORRUPT")
                        remaining = chunk["length_bytes"]
                        while remaining:
                            block = os.read(
                                source_descriptor,
                                min(HASH_BUFFER_BYTES, remaining),
                            )
                            if not block:
                                raise ModelStoreError(
                                    "MODEL_STORE_CHUNK_CORRUPT"
                                )
                            reused = min(existing_remaining, len(block))
                            if reused:
                                existing = os.read(descriptor, reused)
                                if existing != block[:reused]:
                                    raise ModelStoreError(
                                        "MODEL_STORE_FILE_INTEGRITY_FAILED"
                                    )
                                file_digest.update(existing)
                                existing_remaining -= reused
                            if reused < len(block):
                                suffix = block[reused:]
                                _write_all(descriptor, suffix)
                                file_digest.update(suffix)
                            written_total += len(block)
                            remaining -= len(block)
                        if os.read(source_descriptor, 1):
                            raise ModelStoreError("MODEL_STORE_CHUNK_CORRUPT")
                    finally:
                        if source_descriptor >= 0:
                            os.close(source_descriptor)
            if existing_remaining:
                raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
            os.fsync(descriptor)
            observed = os.fstat(descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_dev != before.st_dev
                or observed.st_ino != before.st_ino
                or observed.st_uid != os.geteuid()
                or observed.st_nlink != 1
                or observed.st_mode & 0o077
                or observed.st_size != item["size_bytes"]
                or written_total != item["size_bytes"]
                or file_digest.hexdigest()
                != item["sha256"].removeprefix("sha256:")
            ):
                raise ModelStoreError("MODEL_STORE_FILE_INTEGRITY_FAILED")
            _rename_noreplace(part, target)
            _fsync_directory(target.parent)
            _fsync_directory(work)
        except ModelStoreError:
            raise
        except OSError as exc:
            if exc.errno == errno.ENOSPC:
                raise ModelStoreError("MODEL_STORE_DISK_INSUFFICIENT") from exc
            if exc.errno in {errno.ELOOP, errno.EEXIST, errno.EISDIR, errno.ENOTDIR}:
                raise ModelStoreError("MODEL_STORE_PATH_COLLISION") from exc
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @staticmethod
    def _write_marker(staging: Path, identity: CacheIdentity) -> None:
        payload = (
            json.dumps(
                identity.marker(),
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        path = staging / MODEL_CACHE_MARKER_FILE
        try:
            path.lstat()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        else:
            try:
                marker = read_model_cache_marker(path)
            except ModelCacheMarkerError as exc:
                raise ModelStoreError("MODEL_STORE_TARGET_COLLISION") from exc
            if marker.to_dict() != identity.marker():
                raise ModelStoreError("MODEL_STORE_TARGET_COLLISION")
            return
        descriptor = -1
        try:
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | _CLOEXEC | _NOFOLLOW,
                0o600,
            )
            _write_all(descriptor, payload)
            os.fsync(descriptor)
        except ModelStoreError:
            raise
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _assemble(
        self,
        manifest: CanonicalArtifactManifest,
        identity: CacheIdentity,
    ) -> Path:
        staging = self.store.create_model_staging_directory(
            identity.manifest_digest
        )
        directories, work = self._create_directories(staging, manifest)
        for item in manifest.document["files"]:
            self._assemble_file(staging, work, item)
        _validate_model_config(staging, manifest, identity)
        try:
            _fsync_directory(work)
            work.rmdir()
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_TARGET_COLLISION") from exc
        for directory in sorted(
            directories, key=lambda value: len(value.parts), reverse=True
        ):
            _fsync_directory(directory)
        self._write_marker(staging, identity)
        _fsync_directory(staging)
        return staging

    @staticmethod
    def _validated_manifest(
        identity: CacheIdentity, manifest: dict
    ) -> CanonicalArtifactManifest:
        if identity.cache_kind == MODEL_CACHE_KIND_STAGE:
            raise ModelStoreError("MODEL_STORE_CACHE_KIND_UNSUPPORTED")
        if identity.cache_kind != MODEL_CACHE_KIND_FULL_SNAPSHOT:
            raise ModelStoreError("MODEL_STORE_CACHE_KIND_UNSUPPORTED")
        try:
            parsed = parse_artifact_manifest(
                manifest,
                reserved_paths={
                    MODEL_CACHE_MARKER_FILE,
                    DURE_MODEL_STAGING_WORK_DIRECTORY,
                },
            )
        except ValueError as exc:
            raise ModelStoreError("MODEL_STORE_MANIFEST_MISMATCH") from exc
        paths = {item["path"] for item in parsed.document["files"]}
        if (
            parsed.digest != identity.manifest_digest
            or "config.json" not in paths
            or any(
                path.startswith(f"{reserved}/")
                for path in paths
                for reserved in (
                    MODEL_CACHE_MARKER_FILE,
                    DURE_MODEL_STAGING_WORK_DIRECTORY,
                )
            )
        ):
            raise ModelStoreError("MODEL_STORE_MANIFEST_MISMATCH")
        for path in paths:
            parts = Path(path).parts
            if any(
                Path(*parts[:length]).as_posix() in paths
                for length in range(1, len(parts))
            ):
                raise ModelStoreError("MODEL_STORE_MANIFEST_MISMATCH")
        return parsed

    def prepare_full_snapshot(
        self,
        *,
        identity: CacheIdentity,
        manifest: dict,
        origin: object,
    ) -> PreparedModelCache:
        if type(identity) is not CacheIdentity:
            raise ModelStoreError("MODEL_STORE_INVALID")
        with self.store.artifact_lock(identity.manifest_digest):
            completed_bytes = 0
            try:
                parsed = self._validated_manifest(identity, manifest)
                final = self.store.model_cache_path(identity.manifest_digest)
                self.store.initialize()
                self.store.initialize_model_layout()
                try:
                    final.lstat()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    raise ModelStoreError("MODEL_STORE_IO_FAILED") from exc
                else:
                    try:
                        _verify_cache_tree(final, parsed, identity)
                    except ModelStoreError as exc:
                        raise ModelStoreError("MODEL_STORE_TARGET_COLLISION") from exc
                    self.store.write_attempt(
                        self._journal(
                            identity,
                            "SUCCEEDED",
                            parsed.total_size_bytes,
                        )
                    )
                    return PreparedModelCache(
                        path=final,
                        identity=identity,
                        reused=True,
                        file_count=parsed.file_count,
                        total_size_bytes=parsed.total_size_bytes,
                    )

                staged_bytes, staged_allocation = _verified_staging_bytes(
                    self.store.model_staging_path(identity.manifest_digest),
                    parsed,
                    identity,
                )
                completed_bytes = staged_bytes
                chunks = parsed.unique_chunks()
                missing = 0 if staged_bytes == parsed.total_size_bytes else self._missing_chunk_bytes(chunks)
                self._check_disk(
                    missing_chunk_bytes=missing,
                    assembly_bytes=max(
                        0,
                        parsed.total_size_bytes
                        - staged_bytes
                        - staged_allocation,
                    ),
                )
                if staged_bytes != parsed.total_size_bytes:
                    for digest, size in sorted(chunks.items()):
                        self.downloader.download_chunk_locked(
                            origin=origin,
                            manifest_digest=identity.manifest_digest,
                            chunk_digest=digest,
                            expected_size=size,
                        )

                self.store.write_attempt(
                    self._journal(identity, "ASSEMBLING", staged_bytes)
                )
                staging = self._assemble(parsed, identity)
                completed_bytes = parsed.total_size_bytes
                self.store.write_attempt(
                    self._journal(identity, "VERIFYING", completed_bytes)
                )
                _verify_cache_tree(staging, parsed, identity)
                self.store.write_attempt(
                    self._journal(identity, "ACTIVATING", completed_bytes)
                )
                _rename_noreplace(staging, final)
                _fsync_directory(self.store.model_root)
                _fsync_directory(self.store.model_staging_root)
                _verify_cache_tree(final, parsed, identity)
                self.store.write_attempt(
                    self._journal(identity, "SUCCEEDED", completed_bytes)
                )
                return PreparedModelCache(
                    path=final,
                    identity=identity,
                    reused=False,
                    file_count=parsed.file_count,
                    total_size_bytes=parsed.total_size_bytes,
                )
            except ModelStoreError as exc:
                self.store.write_attempt(
                    self._journal(
                        identity,
                        "FAILED",
                        completed_bytes,
                        failure_code=exc.code,
                    )
                )
                raise
