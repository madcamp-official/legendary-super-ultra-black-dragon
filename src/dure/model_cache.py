from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


MODEL_CACHE_MARKER_FILE = ".dure-model.json"
MODEL_CACHE_MARKER_MAX_BYTES = 64 * 1024
MODEL_CACHE_SCHEMA_V1 = "dure-model-cache-v1"
MODEL_CACHE_SCHEMA_V2 = "dure-model-cache-v2"
MODEL_CACHE_KIND_FULL_SNAPSHOT = "FULL_SNAPSHOT"
MODEL_CACHE_KIND_STAGE = "STAGE"
MODEL_CACHE_VERIFICATION_VERSION = 1

_MODEL_CACHE_KINDS = frozenset(
    {MODEL_CACHE_KIND_FULL_SNAPSHOT, MODEL_CACHE_KIND_STAGE}
)
_V1_FIELDS = frozenset(
    {"schema", "repository", "revision", "manifest_digest", "quantization"}
)
_V2_FIELDS = _V1_FIELDS | frozenset({"cache_kind", "verification_version"})
_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_REVISION = re.compile(r"[0-9a-f]{40,64}")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_QUANTIZATION = re.compile(r"[a-z0-9][a-z0-9._-]{1,39}")


class ModelCacheMarkerError(ValueError):
    pass


@dataclass(frozen=True)
class ModelCacheMarker:
    schema: str
    repository: str
    revision: str
    manifest_digest: str
    quantization: str
    cache_kind: str
    verification_version: int

    def to_dict(self) -> dict[str, str | int]:
        value: dict[str, str | int] = {
            "schema": self.schema,
            "repository": self.repository,
            "revision": self.revision,
            "manifest_digest": self.manifest_digest,
            "quantization": self.quantization,
        }
        if self.schema == MODEL_CACHE_SCHEMA_V2:
            value["cache_kind"] = self.cache_kind
            value["verification_version"] = self.verification_version
        return value


def _required_string(value: Any, field: str, pattern: re.Pattern[str]) -> str:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise ModelCacheMarkerError(f"invalid model cache marker {field}")
    return value


def parse_model_cache_marker(value: Any) -> ModelCacheMarker:
    if type(value) is not dict:
        raise ModelCacheMarkerError("model cache marker must be an object")
    schema = value.get("schema")
    if schema == MODEL_CACHE_SCHEMA_V1:
        if set(value) != _V1_FIELDS:
            raise ModelCacheMarkerError("model cache v1 marker has invalid fields")
        cache_kind = MODEL_CACHE_KIND_FULL_SNAPSHOT
        verification_version = MODEL_CACHE_VERIFICATION_VERSION
    elif schema == MODEL_CACHE_SCHEMA_V2:
        if set(value) != _V2_FIELDS:
            raise ModelCacheMarkerError("model cache v2 marker has invalid fields")
        cache_kind = value.get("cache_kind")
        if type(cache_kind) is not str or cache_kind not in _MODEL_CACHE_KINDS:
            raise ModelCacheMarkerError("invalid model cache marker cache_kind")
        verification_version = value.get("verification_version")
        if (
            type(verification_version) is not int
            or verification_version != MODEL_CACHE_VERIFICATION_VERSION
        ):
            raise ModelCacheMarkerError(
                "unsupported model cache marker verification_version"
            )
    else:
        raise ModelCacheMarkerError("unsupported model cache marker schema")

    return ModelCacheMarker(
        schema=schema,
        repository=_required_string(value.get("repository"), "repository", _REPOSITORY),
        revision=_required_string(value.get("revision"), "revision", _REVISION),
        manifest_digest=_required_string(
            value.get("manifest_digest"), "manifest_digest", _DIGEST
        ),
        quantization=_required_string(
            value.get("quantization"), "quantization", _QUANTIZATION
        ),
        cache_kind=cache_kind,
        verification_version=verification_version,
    )


def decode_model_cache_marker(value: str) -> ModelCacheMarker:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ModelCacheMarkerError("model cache marker has a duplicate JSON key")
            result[key] = item
        return result

    try:
        decoded = json.loads(value, object_pairs_hook=unique_object)
    except json.JSONDecodeError as exc:
        raise ModelCacheMarkerError("model cache marker is not valid JSON") from exc
    return parse_model_cache_marker(decoded)


def build_model_cache_marker(
    *,
    repository: str,
    revision: str,
    manifest_digest: str,
    quantization: str,
    cache_kind: str = MODEL_CACHE_KIND_FULL_SNAPSHOT,
) -> dict[str, str | int]:
    value: dict[str, str | int] = {
        "schema": MODEL_CACHE_SCHEMA_V2,
        "repository": repository,
        "revision": revision,
        "manifest_digest": manifest_digest,
        "quantization": quantization,
        "cache_kind": cache_kind,
        "verification_version": MODEL_CACHE_VERIFICATION_VERSION,
    }
    return parse_model_cache_marker(value).to_dict()
