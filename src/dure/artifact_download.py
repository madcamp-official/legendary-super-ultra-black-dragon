from __future__ import annotations

import errno
import hashlib
import ipaddress
import os
import re
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Mapping, Protocol

from .artifact_manifest import require_sha256_digest
from .model_store import (
    AttemptJournal,
    ContentAddressedModelStore,
    MAX_TRACKED_BYTES,
    ModelStoreError,
)


DEFAULT_DOWNLOAD_ATTEMPTS = 3
DEFAULT_DOWNLOAD_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_REDIRECTS = 3
DEFAULT_MAX_CHUNK_BYTES = 64 * 1024**3
DOWNLOAD_BUFFER_BYTES = 1024 * 1024
MAX_ARTIFACT_URL_LENGTH = 8192
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_SAFE_BASE_PATH = re.compile(r"(?:/[A-Za-z0-9._~-]+)*")
_CONTENT_RANGE = re.compile(r"bytes ([0-9]+)-([0-9]+)/([0-9]+)")


def _safe_hostname(value: str | None) -> str:
    if not value:
        raise ValueError("trusted HTTPS origin requires a host")
    try:
        normalized = value.encode("idna").decode("ascii").lower().rstrip(".")
    except UnicodeError as exc:
        raise ValueError("trusted HTTPS origin host is invalid") from exc
    if not normalized:
        raise ValueError("trusted HTTPS origin host is invalid")
    if ":" in normalized:
        try:
            ipaddress.IPv6Address(normalized)
        except ValueError as exc:
            raise ValueError("trusted HTTPS origin host is invalid") from exc
    elif (
        len(normalized) > 253
        or re.fullmatch(r"[a-z0-9.-]+", normalized) is None
        or any(not label or len(label) > 63 for label in normalized.split("."))
    ):
        raise ValueError("trusted HTTPS origin host is invalid")
    return normalized


def _authority(parts: urllib.parse.SplitResult) -> tuple[str, int]:
    if parts.username is not None or parts.password is not None:
        raise ValueError("trusted HTTPS URLs must not contain userinfo")
    host = _safe_hostname(parts.hostname)
    try:
        port = parts.port or 443
    except ValueError as exc:
        raise ValueError("trusted HTTPS origin port is invalid") from exc
    if not 1 <= port <= 65535:
        raise ValueError("trusted HTTPS origin port is invalid")
    return host, port


def _netloc(host: str, port: int) -> str:
    rendered_host = f"[{host}]" if ":" in host else host
    return rendered_host if port == 443 else f"{rendered_host}:{port}"


@dataclass(frozen=True)
class TrustedHTTPSOrigin:
    base_url: str
    allowed_redirect_hosts: tuple[str, ...] = ()
    _allowed_authorities: frozenset[tuple[str, int]] = field(
        init=False, repr=False
    )

    def __post_init__(self) -> None:
        if type(self.base_url) is not str or not self.base_url:
            raise ValueError("trusted HTTPS origin base URL is invalid")
        if (
            len(self.base_url) > MAX_ARTIFACT_URL_LENGTH
            or any(ord(character) < 0x20 for character in self.base_url)
            or "\\" in self.base_url
        ):
            raise ValueError("trusted HTTPS origin base URL is invalid")
        parts = urllib.parse.urlsplit(self.base_url)
        base_path = parts.path.rstrip("/")
        if (
            parts.scheme.lower() != "https"
            or parts.query
            or parts.fragment
            or not _SAFE_BASE_PATH.fullmatch(base_path)
            or any(segment in {".", ".."} for segment in base_path.split("/"))
        ):
            raise ValueError("trusted HTTPS origin must be a plain HTTPS base URL")
        authority = _authority(parts)
        path = base_path
        normalized_base = f"https://{_netloc(*authority)}{path}"

        if type(self.allowed_redirect_hosts) not in {tuple, list}:
            raise ValueError("allowed redirect hosts must be a local closed list")
        allowed = {authority}
        normalized_hosts: list[str] = []
        for entry in self.allowed_redirect_hosts:
            if (
                type(entry) is not str
                or not entry
                or any(marker in entry for marker in ("/", "?", "#", "@", "\\"))
            ):
                raise ValueError("allowed redirect host is invalid")
            redirect_parts = urllib.parse.urlsplit(f"//{entry}")
            redirect_authority = _authority(redirect_parts)
            allowed.add(redirect_authority)
            normalized_hosts.append(_netloc(*redirect_authority))

        object.__setattr__(self, "base_url", normalized_base)
        object.__setattr__(self, "allowed_redirect_hosts", tuple(normalized_hosts))
        object.__setattr__(self, "_allowed_authorities", frozenset(allowed))

    def object_url(self, chunk_digest: str) -> str:
        digest = require_sha256_digest(chunk_digest, field="chunk_digest")
        return f"{self.base_url}/chunks/sha256/{digest.removeprefix('sha256:')}"

    def validated_redirect(self, current_url: str, location: str) -> str:
        if (
            type(location) is not str
            or not location
            or len(location) > MAX_ARTIFACT_URL_LENGTH
            or any(ord(character) < 0x20 for character in location)
            or "\\" in location
        ):
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
        try:
            target = urllib.parse.urljoin(current_url, location)
            parts = urllib.parse.urlsplit(target)
            target_authority = _authority(parts)
        except (TypeError, ValueError) as exc:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED") from None
        if (
            len(target) > MAX_ARTIFACT_URL_LENGTH
            or parts.scheme.lower() != "https"
            or parts.query
            or parts.fragment
            or target_authority not in self._allowed_authorities
        ):
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
        return target


class ArtifactDownloadResponse(Protocol):
    status: int

    def header_values(self, name: str) -> tuple[str, ...]: ...

    def read(self, size: int) -> bytes: ...

    def close(self) -> None: ...


class ArtifactDownloadTransport(Protocol):
    def open(
        self,
        origin: TrustedHTTPSOrigin,
        object_url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> ArtifactDownloadResponse: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _UrllibResponse:
    def __init__(self, response) -> None:
        self._response = response
        self.status = int(response.getcode())

    def header_values(self, name: str) -> tuple[str, ...]:
        values = self._response.headers.get_all(name, [])
        return tuple(str(value).strip() for value in values)

    def read(self, size: int) -> bytes:
        return self._response.read(size)

    def close(self) -> None:
        self._response.close()


class UrllibHTTPSArtifactTransport:
    def __init__(self, *, max_redirects: int = DEFAULT_MAX_REDIRECTS) -> None:
        if type(max_redirects) is not int or not 0 <= max_redirects <= 5:
            raise ValueError("artifact redirect limit is invalid")
        self.max_redirects = max_redirects
        context = ssl.create_default_context()
        self._opener = urllib.request.build_opener(
            _NoRedirect(),
            urllib.request.HTTPSHandler(context=context),
        )

    def open(
        self,
        origin: TrustedHTTPSOrigin,
        object_url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> ArtifactDownloadResponse:
        if type(origin) is not TrustedHTTPSOrigin:
            raise ModelStoreError("MODEL_STORE_INVALID")
        try:
            expected_url = origin.object_url(
                "sha256:" + object_url.rsplit("/", 1)[-1]
            )
        except (TypeError, ValueError) as exc:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED") from None
        if object_url != expected_url:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
        allowed_headers = {"Accept", "Accept-Encoding", "Range", "User-Agent"}
        if (
            type(headers) is not dict
            or not set(headers).issubset(allowed_headers)
            or any(
                type(value) is not str or "\r" in value or "\n" in value
                for value in headers.values()
            )
        ):
            raise ModelStoreError("MODEL_STORE_INVALID")

        current = object_url
        redirects = 0
        while True:
            request = urllib.request.Request(
                current,
                headers=dict(headers),
                method="GET",
            )
            try:
                response = self._opener.open(request, timeout=timeout_seconds)
            except urllib.error.HTTPError as exc:
                if exc.code not in _REDIRECT_STATUSES:
                    return _UrllibResponse(exc)
                locations = tuple(exc.headers.get_all("Location", []))
                exc.close()
                if len(locations) != 1 or redirects >= self.max_redirects:
                    raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
                current = origin.validated_redirect(current, str(locations[0]))
                redirects += 1
                continue
            except (TimeoutError, socket.timeout) as exc:
                raise ModelStoreError("MODEL_STORE_DOWNLOAD_TIMEOUT") from None
            except urllib.error.URLError as exc:
                if isinstance(exc.reason, (TimeoutError, socket.timeout)):
                    raise ModelStoreError("MODEL_STORE_DOWNLOAD_TIMEOUT") from None
                raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED") from None
            except OSError as exc:
                raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED") from None
            return _UrllibResponse(response)


def _single_header(response: ArtifactDownloadResponse, name: str) -> str | None:
    try:
        values = response.header_values(name)
    except ModelStoreError:
        raise
    except Exception:
        raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED") from None
    if not values:
        return None
    if len(values) != 1:
        raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
    return values[0]


def _content_length(response: ArtifactDownloadResponse) -> int:
    value = _single_header(response, "Content-Length")
    if value is None or re.fullmatch(r"[0-9]+", value) is None:
        raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
    return int(value)


def _validate_response(
    response: ArtifactDownloadResponse,
    *,
    offset: int,
    expected_size: int,
) -> int:
    encoding = _single_header(response, "Content-Encoding")
    if encoding is not None and encoding.lower() != "identity":
        raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
    try:
        transfer_encoding = response.header_values("Transfer-Encoding")
        status = response.status
    except ModelStoreError:
        raise
    except Exception:
        raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED") from None
    if transfer_encoding:
        raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
    remaining = expected_size - offset
    if offset == 0:
        try:
            content_ranges = response.header_values("Content-Range")
        except ModelStoreError:
            raise
        except Exception:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED") from None
        if status != 200 or content_ranges:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
    else:
        if status != 206:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
        content_range = _single_header(response, "Content-Range")
        match = _CONTENT_RANGE.fullmatch(content_range or "")
        if (
            match is None
            or int(match.group(1)) != offset
            or int(match.group(2)) != expected_size - 1
            or int(match.group(3)) != expected_size
        ):
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
    if _content_length(response) != remaining:
        raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
    return remaining


class ArtifactChunkDownloader:
    def __init__(
        self,
        store: ContentAddressedModelStore,
        *,
        transport: ArtifactDownloadTransport | None = None,
        attempts: int = DEFAULT_DOWNLOAD_ATTEMPTS,
        timeout_seconds: float = DEFAULT_DOWNLOAD_TIMEOUT_SECONDS,
        max_chunk_bytes: int = DEFAULT_MAX_CHUNK_BYTES,
    ) -> None:
        if type(store) is not ContentAddressedModelStore:
            raise ValueError("artifact downloader requires a model store")
        if (
            type(attempts) is not int
            or not 1 <= attempts <= 5
            or type(timeout_seconds) not in {int, float}
            or not 0 < float(timeout_seconds) <= 300
            or type(max_chunk_bytes) is not int
            or not 1 <= max_chunk_bytes <= MAX_TRACKED_BYTES
        ):
            raise ValueError("artifact downloader local limits are invalid")
        self.store = store
        self.transport = transport or UrllibHTTPSArtifactTransport()
        self.attempts = attempts
        self.timeout_seconds = float(timeout_seconds)
        self.max_chunk_bytes = max_chunk_bytes

    @staticmethod
    def _journal(
        manifest_digest: str,
        chunk_digest: str,
        bytes_complete: int,
        *,
        status: str = "DOWNLOADING",
        failure_code: str | None = None,
    ) -> AttemptJournal:
        return AttemptJournal(
            manifest_digest=manifest_digest,
            chunk_digest=chunk_digest,
            bytes_complete=bytes_complete,
            status=status,
            failure_code=failure_code,
        )

    @staticmethod
    def _hash_partial(descriptor: int, expected_size: int) -> str:
        try:
            os.lseek(descriptor, 0, os.SEEK_SET)
            digest = hashlib.sha256()
            observed = 0
            while observed <= expected_size:
                block = os.read(
                    descriptor,
                    min(DOWNLOAD_BUFFER_BYTES, expected_size - observed + 1),
                )
                if not block:
                    break
                observed += len(block)
                if observed > expected_size:
                    raise ModelStoreError("MODEL_STORE_CHUNK_COLLISION")
                digest.update(block)
        except ModelStoreError:
            raise
        except OSError:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from None
        if observed != expected_size:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_INTERRUPTED")
        return digest.hexdigest()

    def _download_once(
        self,
        origin: TrustedHTTPSOrigin,
        chunk_digest: str,
        expected_size: int,
        descriptor: int,
        offset: int,
    ) -> int:
        headers = {
            "Accept": "application/octet-stream",
            "Accept-Encoding": "identity",
            "User-Agent": "Dure-artifact/1",
        }
        if offset:
            headers["Range"] = f"bytes={offset}-"
        try:
            response = self.transport.open(
                origin,
                origin.object_url(chunk_digest),
                headers=headers,
                timeout_seconds=self.timeout_seconds,
            )
        except ModelStoreError:
            raise
        except (TimeoutError, socket.timeout) as exc:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_TIMEOUT") from None
        except OSError as exc:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_INTERRUPTED") from None
        except Exception:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED") from None
        deadline = time.monotonic() + self.timeout_seconds
        body_completed = False
        try:
            try:
                remaining = _validate_response(
                    response,
                    offset=offset,
                    expected_size=expected_size,
                )
            except ModelStoreError:
                raise
            except Exception:
                raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED") from None
            os.lseek(descriptor, offset, os.SEEK_SET)
            received = 0
            while received <= remaining:
                if time.monotonic() >= deadline:
                    raise ModelStoreError("MODEL_STORE_DOWNLOAD_TIMEOUT")
                try:
                    block = response.read(
                        min(DOWNLOAD_BUFFER_BYTES, remaining - received + 1)
                    )
                except (TimeoutError, socket.timeout) as exc:
                    raise ModelStoreError("MODEL_STORE_DOWNLOAD_TIMEOUT") from None
                except OSError as exc:
                    raise ModelStoreError("MODEL_STORE_DOWNLOAD_INTERRUPTED") from None
                except Exception:
                    raise ModelStoreError("MODEL_STORE_DOWNLOAD_INTERRUPTED") from None
                if time.monotonic() >= deadline:
                    raise ModelStoreError("MODEL_STORE_DOWNLOAD_TIMEOUT")
                if not block:
                    break
                if type(block) is not bytes or received + len(block) > remaining:
                    raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
                view = memoryview(block)
                while view:
                    try:
                        written = os.write(descriptor, view)
                    except OSError as exc:
                        if exc.errno == errno.ENOSPC:
                            raise ModelStoreError(
                                "MODEL_STORE_DISK_INSUFFICIENT"
                            ) from None
                        raise ModelStoreError("MODEL_STORE_IO_FAILED") from None
                    if written <= 0:
                        raise ModelStoreError("MODEL_STORE_IO_FAILED")
                    view = view[written:]
                received += len(block)
            if received != remaining:
                raise ModelStoreError("MODEL_STORE_DOWNLOAD_INTERRUPTED")
            os.fsync(descriptor)
            body_completed = True
            return offset + received
        except ModelStoreError:
            raise
        except OSError:
            raise ModelStoreError("MODEL_STORE_IO_FAILED") from None
        except Exception:
            raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED") from None
        finally:
            try:
                response.close()
            except Exception:
                if body_completed:
                    raise ModelStoreError(
                        "MODEL_STORE_DOWNLOAD_INTERRUPTED"
                    ) from None

    def download_chunk(
        self,
        *,
        origin: TrustedHTTPSOrigin,
        manifest_digest: str,
        chunk_digest: str,
        expected_size: int,
    ) -> str:
        try:
            require_sha256_digest(manifest_digest, field="manifest_digest")
        except ValueError:
            raise ModelStoreError("MODEL_STORE_INVALID") from None
        with self.store.artifact_lock(manifest_digest):
            return self.download_chunk_locked(
                origin=origin,
                manifest_digest=manifest_digest,
                chunk_digest=chunk_digest,
                expected_size=expected_size,
            )

    def download_chunk_locked(
        self,
        *,
        origin: TrustedHTTPSOrigin,
        manifest_digest: str,
        chunk_digest: str,
        expected_size: int,
    ) -> str:
        """Prepare one chunk while the caller holds the artifact lock."""

        if type(origin) is not TrustedHTTPSOrigin:
            raise ModelStoreError("MODEL_STORE_INVALID")
        try:
            require_sha256_digest(manifest_digest, field="manifest_digest")
            expected_hex = require_sha256_digest(
                chunk_digest, field="chunk_digest"
            ).removeprefix("sha256:")
        except ValueError as exc:
            raise ModelStoreError("MODEL_STORE_INVALID") from None
        if (
            type(expected_size) is not int
            or not 1 <= expected_size <= self.max_chunk_bytes
        ):
            raise ModelStoreError("MODEL_STORE_INVALID")

        with self.store.chunk_lock(chunk_digest):
            try:
                existing = self.store._verified_chunk_without_lock(
                    chunk_digest, expected_size
                )
            except ModelStoreError as exc:
                self.store.write_attempt(
                    self._journal(
                        manifest_digest,
                        chunk_digest,
                        0,
                        status="FAILED",
                        failure_code=exc.code,
                    )
                )
                raise
            if existing is not None:
                self.store.write_attempt(
                    self._journal(
                        manifest_digest,
                        chunk_digest,
                        expected_size,
                        status="SUCCEEDED",
                    )
                )
                return str(existing)

            try:
                _, descriptor, offset = self.store.open_chunk_partial(
                    chunk_digest, expected_size
                )
            except ModelStoreError as exc:
                try:
                    self.store.write_attempt(
                        self._journal(
                            manifest_digest,
                            chunk_digest,
                            0,
                            status="FAILED",
                            failure_code=exc.code,
                        )
                    )
                except ModelStoreError:
                    pass
                raise
            try:
                last_error: ModelStoreError | None = None
                for attempt in range(self.attempts):
                    self.store.write_attempt(
                        self._journal(manifest_digest, chunk_digest, offset)
                    )
                    try:
                        if offset < expected_size:
                            offset = self._download_once(
                                origin,
                                chunk_digest,
                                expected_size,
                                descriptor,
                                offset,
                            )
                        try:
                            os.fsync(descriptor)
                        except OSError:
                            raise ModelStoreError("MODEL_STORE_IO_FAILED") from None
                        if self._hash_partial(descriptor, expected_size) != expected_hex:
                            raise ModelStoreError("MODEL_STORE_DIGEST_MISMATCH")
                        try:
                            os.close(descriptor)
                        except OSError:
                            raise ModelStoreError("MODEL_STORE_IO_FAILED") from None
                        descriptor = -1
                        published = self.store.publish_chunk_partial(
                            chunk_digest, expected_size
                        )
                        self.store.write_attempt(
                            self._journal(
                                manifest_digest,
                                chunk_digest,
                                expected_size,
                                status="SUCCEEDED",
                            )
                        )
                        return str(published)
                    except ModelStoreError as exc:
                        last_error = exc
                        if descriptor >= 0:
                            try:
                                os.fsync(descriptor)
                                offset = os.fstat(descriptor).st_size
                            except OSError:
                                raise ModelStoreError(
                                    "MODEL_STORE_IO_FAILED"
                                ) from exc
                            if exc.code in {
                                "MODEL_STORE_DOWNLOAD_REJECTED",
                                "MODEL_STORE_DIGEST_MISMATCH",
                            }:
                                try:
                                    os.ftruncate(descriptor, 0)
                                    os.fsync(descriptor)
                                except OSError as reset_exc:
                                    raise ModelStoreError(
                                        "MODEL_STORE_IO_FAILED"
                                    ) from reset_exc
                                offset = 0
                        self.store.write_attempt(
                            self._journal(
                                manifest_digest,
                                chunk_digest,
                                offset,
                                status="FAILED",
                                failure_code=exc.code,
                            )
                        )
                        if attempt + 1 >= self.attempts or exc.code not in {
                            "MODEL_STORE_DOWNLOAD_TIMEOUT",
                            "MODEL_STORE_DOWNLOAD_INTERRUPTED",
                            "MODEL_STORE_DOWNLOAD_REJECTED",
                            "MODEL_STORE_DIGEST_MISMATCH",
                        }:
                            raise
                if last_error is not None:  # pragma: no cover - loop always exits above
                    raise last_error
                raise ModelStoreError("MODEL_STORE_DOWNLOAD_REJECTED")
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
