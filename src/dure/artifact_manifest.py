from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable


ARTIFACT_MANIFEST_SCHEMA_VERSION = 1
SHA256_DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")
_MANIFEST_KEYS = frozenset({"schema_version", "files"})
_FILE_KEYS = frozenset({"path", "kind", "size_bytes", "sha256", "chunks"})
_CHUNK_KEYS = frozenset(
    {"ordinal", "offset_bytes", "length_bytes", "sha256"}
)


@dataclass(frozen=True)
class ArtifactManifestLimits:
    max_files: int = 100_000
    max_chunks: int = 1_000_000
    max_path_length: int = 1024
    max_file_bytes: int = 1 << 50
    max_total_bytes: int = 1 << 50

    def __post_init__(self) -> None:
        values = (
            self.max_files,
            self.max_chunks,
            self.max_path_length,
            self.max_file_bytes,
            self.max_total_bytes,
        )
        if any(type(value) is not int or value <= 0 for value in values):
            raise ValueError("artifact manifest limits must be positive integers")


@dataclass(frozen=True)
class CanonicalArtifactManifest:
    document: dict
    canonical_json: str
    digest: str
    total_size_bytes: int
    file_count: int
    chunk_count: int

    def as_legacy_tuple(self) -> tuple[dict, str, str, int, int, int]:
        return (
            self.document,
            self.canonical_json,
            self.digest,
            self.total_size_bytes,
            self.file_count,
            self.chunk_count,
        )

    def unique_chunks(self) -> dict[str, int]:
        chunks: dict[str, int] = {}
        for file_item in self.document["files"]:
            for chunk in file_item["chunks"]:
                chunks[chunk["sha256"]] = chunk["length_bytes"]
        return chunks


def require_sha256_digest(value: object, *, field: str) -> str:
    if type(value) is not str or SHA256_DIGEST_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field} must be an immutable sha256 digest")
    return value


def _exact_object(
    value: object,
    *,
    expected: frozenset[str],
    field: str,
) -> dict:
    if type(value) is not dict:
        raise ValueError(f"{field} must be an object")
    if any(type(key) is not str for key in value):
        raise ValueError(f"{field} keys must be strings")
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        detail = []
        if missing:
            detail.append(f"missing={','.join(missing)}")
        if unknown:
            detail.append(f"unknown={','.join(unknown)}")
        raise ValueError(f"{field} has invalid fields ({'; '.join(detail)})")
    return value


def _bounded_integer(
    value: object,
    *,
    field: str,
    minimum: int,
    maximum: int,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{field} must be an integer in range")
    return value


def _relative_path(value: object, *, maximum: int) -> str:
    if type(value) is not str:
        raise ValueError("file.path must be a relative string")
    if (
        not value
        or "\\" in value
        or value.startswith("/")
        or any(unicodedata.category(character).startswith("C") for character in value)
    ):
        raise ValueError("file.path must be a safe relative path")
    if re.match(r"^[A-Za-z]:", value):
        raise ValueError("file.path must not be an absolute drive path")
    normalized = unicodedata.normalize("NFC", value)
    if len(normalized) > maximum:
        raise ValueError("file.path exceeds the maximum length")
    if any(segment in {"", ".", ".."} for segment in normalized.split("/")):
        raise ValueError("file.path must not contain empty, dot, or parent segments")
    return normalized


def parse_artifact_manifest(
    manifest: dict,
    *,
    limits: ArtifactManifestLimits | None = None,
    reserved_paths: Iterable[str] = (),
) -> CanonicalArtifactManifest:
    limits = limits or ArtifactManifestLimits()
    source = _exact_object(manifest, expected=_MANIFEST_KEYS, field="manifest")
    if (
        type(source["schema_version"]) is not int
        or source["schema_version"] != ARTIFACT_MANIFEST_SCHEMA_VERSION
    ):
        raise ValueError("manifest.schema_version must be exactly 1")
    source_files = source["files"]
    if type(source_files) is not list:
        raise ValueError("manifest.files must be a list")
    if not 1 <= len(source_files) <= limits.max_files:
        raise ValueError("manifest file count is out of range")

    reserved = frozenset(reserved_paths)
    if any(type(path) is not str for path in reserved):
        raise ValueError("reserved artifact paths must be strings")

    files: list[dict] = []
    paths: set[str] = set()
    chunk_sizes: dict[str, int] = {}
    total_size = 0
    chunk_count = 0
    for file_index, raw_file in enumerate(source_files):
        item = _exact_object(
            raw_file,
            expected=_FILE_KEYS,
            field=f"manifest.files[{file_index}]",
        )
        path = _relative_path(item["path"], maximum=limits.max_path_length)
        if path in paths:
            raise ValueError("manifest contains a duplicate normalized file path")
        if path in reserved:
            raise ValueError("manifest contains a reserved file path")
        paths.add(path)
        if type(item["kind"]) is not str or item["kind"] != "REGULAR":
            raise ValueError("file.kind must be REGULAR")
        size_bytes = _bounded_integer(
            item["size_bytes"],
            field="file.size_bytes",
            minimum=0,
            maximum=limits.max_file_bytes,
        )
        if total_size > limits.max_total_bytes - size_bytes:
            raise ValueError("manifest total size exceeds the maximum")
        total_size += size_bytes
        file_digest = require_sha256_digest(item["sha256"], field="file.sha256")
        raw_chunks = item["chunks"]
        if type(raw_chunks) is not list:
            raise ValueError("file.chunks must be a list")
        if len(raw_chunks) > limits.max_chunks - chunk_count:
            raise ValueError("manifest chunk count exceeds the maximum")

        chunks: list[dict] = []
        for chunk_index, raw_chunk in enumerate(raw_chunks):
            chunk = _exact_object(
                raw_chunk,
                expected=_CHUNK_KEYS,
                field=f"file.chunks[{chunk_index}]",
            )
            ordinal = _bounded_integer(
                chunk["ordinal"],
                field="chunk.ordinal",
                minimum=0,
                maximum=limits.max_chunks - 1,
            )
            offset_bytes = _bounded_integer(
                chunk["offset_bytes"],
                field="chunk.offset_bytes",
                minimum=0,
                maximum=limits.max_file_bytes,
            )
            length_bytes = _bounded_integer(
                chunk["length_bytes"],
                field="chunk.length_bytes",
                minimum=1,
                maximum=limits.max_file_bytes,
            )
            chunk_digest = require_sha256_digest(
                chunk["sha256"], field="chunk.sha256"
            )
            known_size = chunk_sizes.setdefault(chunk_digest, length_bytes)
            if known_size != length_bytes:
                raise ValueError("a shared chunk digest has inconsistent lengths")
            chunks.append(
                {
                    "ordinal": ordinal,
                    "offset_bytes": offset_bytes,
                    "length_bytes": length_bytes,
                    "sha256": chunk_digest,
                }
            )
        chunks.sort(key=lambda value: value["ordinal"])
        if size_bytes == 0 and chunks:
            raise ValueError("an empty file must not contain chunks")
        if size_bytes > 0 and not chunks:
            raise ValueError("a non-empty file must contain chunks")
        cursor = 0
        for expected_ordinal, chunk in enumerate(chunks):
            if chunk["ordinal"] != expected_ordinal:
                raise ValueError("chunk ordinals must be contiguous from zero")
            if chunk["offset_bytes"] != cursor:
                raise ValueError("chunk ranges must be contiguous without gaps or overlap")
            cursor += chunk["length_bytes"]
            if cursor > size_bytes:
                raise ValueError("chunk ranges exceed the file size")
        if cursor != size_bytes:
            raise ValueError("chunk ranges must cover the exact file size")
        chunk_count += len(chunks)
        files.append(
            {
                "path": path,
                "kind": "REGULAR",
                "size_bytes": size_bytes,
                "sha256": file_digest,
                "chunks": chunks,
            }
        )

    if total_size <= 0 or chunk_count <= 0:
        raise ValueError("manifest must contain non-empty regular file content")
    files.sort(key=lambda value: value["path"])
    canonical = {
        "schema_version": ARTIFACT_MANIFEST_SCHEMA_VERSION,
        "files": files,
    }
    canonical_json = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    digest = "sha256:" + hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return CanonicalArtifactManifest(
        document=canonical,
        canonical_json=canonical_json,
        digest=digest,
        total_size_bytes=total_size,
        file_count=len(files),
        chunk_count=chunk_count,
    )


def canonical_artifact_manifest(
    manifest: dict,
    *,
    limits: ArtifactManifestLimits | None = None,
    reserved_paths: Iterable[str] = (),
) -> tuple[dict, str, str, int, int, int]:
    return parse_artifact_manifest(
        manifest,
        limits=limits,
        reserved_paths=reserved_paths,
    ).as_legacy_tuple()


def canonical_artifact_manifest_digest(
    manifest: dict,
    *,
    limits: ArtifactManifestLimits | None = None,
) -> str:
    return parse_artifact_manifest(manifest, limits=limits).digest
