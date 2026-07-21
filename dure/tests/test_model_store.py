from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import stat
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dure import model_store as model_store_module
from dure import stage_cache as stage_cache_module
from dure.artifact_download import ArtifactChunkDownloader, TrustedHTTPSOrigin
from dure.artifact_manifest import ArtifactManifestLimits, parse_artifact_manifest
from dure.model_cache import (
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_MARKER_FILE,
    MODEL_CACHE_SCHEMA_V2,
)
from dure.model_store import (
    AttemptJournal,
    CacheIdentity,
    ContentAddressedModelStore,
    DURE_MODEL_STAGING_DIRECTORY,
    DURE_MODEL_STAGING_MARKER_PART_FILE,
    DURE_MODEL_STAGING_WORK_DIRECTORY,
    MAX_MODEL_CONFIG_BYTES,
    ModelCachePreparer,
    ModelStoreError,
    _rename_noreplace,
)
from dure.stage_cache import (
    STAGE_CACHE_MANIFEST_FILE,
    StageCacheError,
    StageCacheIdentity,
    canonical_stage_manifest,
    stage_cache_path,
    stage_contract_identity_digest,
    validate_materialized_stage_cache,
    validate_stage_marker_document,
)
from dure.probe import NodeProbe
from tests.helpers import FakeRunner


ORIGIN = TrustedHTTPSOrigin("https://artifacts.example.test/models")
CONFIG_BYTES = b'{"model_type":"dure-test"}'
WEIGHT_BYTES = b"0123456789abcdef"


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _regular_file(
    path: str,
    payload: bytes,
    *,
    file_digest: str | None = None,
    chunk_size: int | None = None,
) -> dict:
    chunks = []
    if payload:
        width = chunk_size or len(payload)
        for ordinal, offset in enumerate(range(0, len(payload), width)):
            chunk = payload[offset : offset + width]
            chunks.append(
                {
                    "ordinal": ordinal,
                    "offset_bytes": offset,
                    "length_bytes": len(chunk),
                    "sha256": _digest(chunk),
                }
            )
    return {
        "path": path,
        "kind": "REGULAR",
        "size_bytes": len(payload),
        "sha256": file_digest or _digest(payload),
        "chunks": chunks,
    }


def _full_manifest(
    *,
    config_payload: bytes = CONFIG_BYTES,
    config_digest: str | None = None,
) -> dict:
    return {
        "schema_version": 1,
        "files": [
            _regular_file(
                "weights/model.bin",
                WEIGHT_BYTES,
                chunk_size=8,
            ),
            _regular_file(
                "config.json",
                config_payload,
                file_digest=config_digest,
            ),
        ],
    }


def _identity(
    manifest: dict,
    *,
    cache_kind: str | None = None,
) -> CacheIdentity:
    digest = parse_artifact_manifest(manifest).digest
    values = {
        "repository": "Example/Dure-Model",
        "revision": "a" * 40,
        "manifest_digest": digest,
        "quantization": "fp16",
    }
    if cache_kind is not None:
        values["cache_kind"] = cache_kind
    return CacheIdentity(**values)


def _objects() -> dict[str, bytes]:
    values = [
        CONFIG_BYTES,
        WEIGHT_BYTES[:8],
        WEIGHT_BYTES[8:],
    ]
    return {_digest(value): value for value in values}


def _tensor_digest(keys: list[str]) -> str:
    encoded = json.dumps(
        {"schema_version": 1, "tensor_keys": keys},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _digest(encoded)


def _stage_fixture(
    *,
    pipeline_rank: int = 1,
    artifact_set_character: str = "c",
) -> tuple[dict, StageCacheIdentity, dict[str, bytes]]:
    tensor_keys = [f"model.layers.{pipeline_rank}.weight"]
    tensor_keys_digest = _tensor_digest(tensor_keys)
    runtime_image = "registry.example/vllm@sha256:" + "f" * 64
    contract = {
        "schema_version": 1,
        "source_manifest_digest": "sha256:" + "e" * 64,
        "runtime_image": runtime_image,
        "exporter_build_digest": "sha256:" + "1" * 64,
        "model_family": "qwen2.5",
        "architecture": "Qwen2ForCausalLM",
        "quantization": "awq",
        "tensor_parallel_size": 1,
        "pipeline_parallel_size": 3,
        "loader_format": "sharded_state",
        "vllm_version": "0.9.0",
        "max_part_bytes": 5 * 1024**3,
        "trust_remote_code": False,
        "enable_lora": False,
        "is_moe": False,
        "is_multimodal": False,
    }
    marker = json.dumps(
        {
            "schema_version": 1,
            "kind": "VLLM_SHARDED_STATE_PIPELINE_STAGE",
            "contract": contract,
            "pipeline_rank": pipeline_rank,
            "weight_pattern": "model-rank-0-part-*.safetensors",
            "metadata_files": [
                "config.json",
                "tokenizer.json",
                "tokenizer_config.json",
            ],
            "tensors": [
                {
                    "name": tensor_keys[0],
                    "dtype": "F16",
                    "shape": [1],
                }
            ],
            "tensor_key_digest": tensor_keys_digest,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    files = {
        "config.json": b'{"model_type":"qwen2","quantization_config":{"quant_method":"awq"}}',
        "tokenizer.json": b'{"version":"1.0"}',
        "tokenizer_config.json": b'{"model_max_length":4096}',
        "dure-stage.json": marker,
        "model-rank-0-part-0.safetensors": b"stage-weight-" + bytes([pipeline_rank]),
    }
    manifest = {
        "schema_version": 1,
        "files": [_regular_file(path, payload) for path, payload in files.items()],
    }
    manifest_digest = parse_artifact_manifest(manifest).digest
    identity = StageCacheIdentity(
        repository="Example/Dure-Model",
        revision="a" * 40,
        manifest_digest=manifest_digest,
        quantization="awq",
        artifact_set_digest="sha256:" + artifact_set_character * 64,
        contract_identity_digest=stage_contract_identity_digest(
            source_manifest_digest=contract["source_manifest_digest"],
            runtime_image=runtime_image,
            vllm_version="0.9.0",
            exporter_build_digest=contract["exporter_build_digest"],
            architecture="Qwen2ForCausalLM",
            quantization="awq",
            tensor_parallel_size=1,
            pipeline_parallel_size=3,
            loader_format="VLLM_SHARDED_STATE_V1",
        ),
        source_manifest_digest=contract["source_manifest_digest"],
        runtime_image=runtime_image,
        vllm_version="0.9.0",
        exporter_build_digest=contract["exporter_build_digest"],
        architecture="Qwen2ForCausalLM",
        loader_format="VLLM_SHARDED_STATE_V1",
        tensor_parallel_size=1,
        pipeline_parallel_size=3,
        pipeline_rank=pipeline_rank,
        tensor_rank=0,
        tensor_keys_digest=tensor_keys_digest,
    )
    return manifest, identity, {_digest(payload): payload for payload in files.values()}


class MemoryResponse:
    def __init__(
        self,
        payload: bytes,
        *,
        status: int,
        headers: dict[str, tuple[str, ...]],
    ) -> None:
        self.payload = payload
        self.status = status
        self.headers = headers
        self.offset = 0

    def header_values(self, name: str) -> tuple[str, ...]:
        return self.headers.get(name.lower(), ())

    def read(self, size: int) -> bytes:
        value = self.payload[self.offset : self.offset + size]
        self.offset += len(value)
        return value

    def close(self) -> None:
        return None


class MemoryTransport:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or _objects())
        self.calls: list[tuple[str, dict[str, str]]] = []
        self._lock = threading.Lock()

    def open(
        self,
        origin: TrustedHTTPSOrigin,
        object_url: str,
        *,
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> MemoryResponse:
        del origin, timeout_seconds
        with self._lock:
            self.calls.append((object_url, dict(headers)))
        digest = "sha256:" + object_url.rsplit("/", 1)[-1]
        payload = self.objects[digest]
        range_header = headers.get("Range")
        if range_header is None:
            return MemoryResponse(
                payload,
                status=200,
                headers={"content-length": (str(len(payload)),)},
            )
        prefix = "bytes="
        if not range_header.startswith(prefix) or not range_header.endswith("-"):
            raise AssertionError("unexpected Range header")
        offset = int(range_header[len(prefix) : -1])
        remaining = payload[offset:]
        return MemoryResponse(
            remaining,
            status=206,
            headers={
                "content-length": (str(len(remaining)),),
                "content-range": (
                    f"bytes {offset}-{len(payload) - 1}/{len(payload)}",
                ),
            },
        )


class CacheHarness:
    def __init__(
        self,
        *,
        manifest: dict | None = None,
        identity: CacheIdentity | None = None,
        objects: dict[str, bytes] | None = None,
        disk_usage=None,
    ) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = ContentAddressedModelStore(
            store_root=self.root / "store",
            model_root=self.root / "models",
        )
        self.manifest = copy.deepcopy(manifest or _full_manifest())
        self.parsed = parse_artifact_manifest(self.manifest)
        self.identity = identity or _identity(self.manifest)
        self.transport = MemoryTransport(objects)
        self.downloader = ArtifactChunkDownloader(
            self.store,
            transport=self.transport,
            attempts=1,
        )
        options = {"disk_reserve_bytes": 0}
        if disk_usage is not None:
            options["disk_usage"] = disk_usage
        self.preparer = ModelCachePreparer(
            self.store,
            self.downloader,
            **options,
        )

    def close(self) -> None:
        self.temporary.cleanup()


class StageCachePreparationTests(unittest.TestCase):
    def test_stage_marker_rejects_boolean_integer_and_flag_aliases(self):
        manifest, identity, objects = _stage_fixture()
        canonical = canonical_stage_manifest(manifest, identity)
        marker_item = next(
            item for item in manifest["files"] if item["path"] == "dure-stage.json"
        )
        marker = json.loads(objects[marker_item["sha256"]])
        mutations = (
            lambda value: value.__setitem__("schema_version", True),
            lambda value: value.__setitem__("pipeline_rank", True),
            lambda value: value["contract"].__setitem__("schema_version", True),
            lambda value: value["contract"].__setitem__(
                "tensor_parallel_size", True
            ),
            lambda value: value["contract"].__setitem__(
                "trust_remote_code", 0
            ),
        )

        for mutate in mutations:
            with self.subTest(mutation=mutate):
                value = copy.deepcopy(marker)
                mutate(value)
                with self.assertRaises(StageCacheError):
                    validate_stage_marker_document(value, identity, canonical)

    def test_runtime_stage_hashing_uses_bounded_streaming_reads(self):
        payload = b"x" * (2 * 1024 * 1024 + 17)
        requests: list[int] = []
        real_read = os.read

        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "stage-weight.safetensors"
            path.write_bytes(payload)

            def observed_read(descriptor: int, size: int) -> bytes:
                requests.append(size)
                return real_read(descriptor, size)

            with patch.object(
                stage_cache_module.os,
                "read",
                side_effect=observed_read,
            ):
                observed = stage_cache_module._safe_regular_digest(
                    path,
                    len(payload),
                )

        self.assertEqual(observed, _digest(payload))
        self.assertGreaterEqual(len(requests), 3)
        self.assertLessEqual(max(requests), 1024 * 1024)

    def test_stage_materialization_is_atomic_reusable_and_runtime_verifiable(self):
        manifest, identity, objects = _stage_fixture()
        harness = CacheHarness(
            manifest=manifest,
            identity=identity,
            objects=objects,
        )
        try:
            first = harness.preparer.prepare_stage(
                identity=identity,
                manifest=manifest,
                origin=ORIGIN,
            )
            calls_after_first = len(harness.transport.calls)
            second = harness.preparer.prepare_stage(
                identity=identity,
                manifest=manifest,
                origin=ORIGIN,
            )

            self.assertEqual(
                first.path,
                stage_cache_path(identity, model_root=harness.store.model_root),
            )
            self.assertFalse(first.reused)
            self.assertTrue(second.reused)
            self.assertEqual(len(harness.transport.calls), calls_after_first)
            self.assertTrue((first.path / STAGE_CACHE_MANIFEST_FILE).is_file())
            validation = validate_materialized_stage_cache(first.path, identity)
            self.assertEqual(validation.manifest_digest, identity.manifest_digest)
            self.assertEqual(validation.total_size_bytes, first.total_size_bytes)
            self.assertEqual(validation.file_count, first.file_count)
            journal = harness.store.read_attempt(
                identity.cache_identity_digest,
                manifest_digest=identity.manifest_digest,
            )
            self.assertEqual(journal.status, "SUCCEEDED")
            self.assertEqual(
                journal.cache_identity_digest, identity.cache_identity_digest
            )
        except ModelStoreError as exc:
            if exc.code == "MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE":
                self.skipTest("Linux renameat2(RENAME_NOREPLACE) is unavailable")
            raise
        finally:
            harness.close()

    def test_stage_composite_identity_separates_same_manifest_contracts(self):
        manifest, first, objects = _stage_fixture()
        second = replace(
            first,
            artifact_set_digest="sha256:" + "9" * 64,
        )
        harness = CacheHarness(
            manifest=manifest,
            identity=first,
            objects=objects,
        )
        try:
            first_result = harness.preparer.prepare_stage(
                identity=first, manifest=manifest, origin=ORIGIN
            )
            second_result = harness.preparer.prepare_stage(
                identity=second, manifest=manifest, origin=ORIGIN
            )
            self.assertNotEqual(first.cache_identity_digest, second.cache_identity_digest)
            self.assertNotEqual(first_result.path, second_result.path)
            self.assertTrue(first_result.path.is_dir())
            self.assertTrue(second_result.path.is_dir())
        except ModelStoreError as exc:
            if exc.code == "MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE":
                self.skipTest("Linux renameat2(RENAME_NOREPLACE) is unavailable")
            raise
        finally:
            harness.close()

    def test_stage_marker_contract_tampering_never_activates(self):
        manifest, identity, objects = _stage_fixture()
        tampered = copy.deepcopy(manifest)
        marker_item = next(
            item for item in tampered["files"] if item["path"] == "dure-stage.json"
        )
        marker = json.loads(objects[marker_item["sha256"]])
        marker["pipeline_rank"] = 2
        tampered_marker = json.dumps(
            marker, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        replacement = _regular_file("dure-stage.json", tampered_marker)
        tampered["files"] = [
            replacement if item["path"] == "dure-stage.json" else item
            for item in tampered["files"]
        ]
        tampered_identity = replace(
            identity,
            manifest_digest=parse_artifact_manifest(tampered).digest,
        )
        tampered_objects = dict(objects)
        tampered_objects[_digest(tampered_marker)] = tampered_marker
        harness = CacheHarness(
            manifest=tampered,
            identity=tampered_identity,
            objects=tampered_objects,
        )
        try:
            with self.assertRaises(ModelStoreError) as caught:
                harness.preparer.prepare_stage(
                    identity=tampered_identity,
                    manifest=tampered,
                    origin=ORIGIN,
                )
            self.assertEqual(caught.exception.code, "MODEL_STORE_MANIFEST_MISMATCH")
            self.assertFalse(harness.store.stage_cache_path(tampered_identity).exists())
        finally:
            harness.close()

    def test_stage_marker_rejects_reserved_safetensors_metadata_name(self):
        manifest, identity, objects = _stage_fixture()
        tampered = copy.deepcopy(manifest)
        marker_item = next(
            item for item in tampered["files"] if item["path"] == "dure-stage.json"
        )
        marker = json.loads(objects[marker_item["sha256"]])
        marker["tensors"][0]["name"] = "__metadata__"
        marker["tensor_key_digest"] = _tensor_digest(["__metadata__"])
        tampered_marker = json.dumps(
            marker, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        tampered["files"] = [
            _regular_file("dure-stage.json", tampered_marker)
            if item["path"] == "dure-stage.json"
            else item
            for item in tampered["files"]
        ]
        tampered_identity = replace(
            identity,
            manifest_digest=parse_artifact_manifest(tampered).digest,
            tensor_keys_digest=marker["tensor_key_digest"],
        )
        tampered_objects = dict(objects)
        tampered_objects[_digest(tampered_marker)] = tampered_marker
        harness = CacheHarness(
            manifest=tampered,
            identity=tampered_identity,
            objects=tampered_objects,
        )
        try:
            with self.assertRaises(ModelStoreError) as caught:
                harness.preparer.prepare_stage(
                    identity=tampered_identity,
                    manifest=tampered,
                    origin=ORIGIN,
                )
            self.assertEqual(caught.exception.code, "MODEL_STORE_MANIFEST_MISMATCH")
            self.assertFalse(harness.store.stage_cache_path(tampered_identity).exists())
        finally:
            harness.close()

    def test_full_and_stage_preparers_do_not_fallback_between_cache_kinds(self):
        manifest, identity, objects = _stage_fixture()
        harness = CacheHarness(
            manifest=manifest,
            identity=identity,
            objects=objects,
        )
        try:
            with self.assertRaises(ModelStoreError) as full_rejects_stage:
                harness.preparer.prepare_full_snapshot(
                    identity=identity,
                    manifest=manifest,
                    origin=ORIGIN,
                )
            self.assertEqual(
                full_rejects_stage.exception.code,
                "MODEL_STORE_CACHE_KIND_UNSUPPORTED",
            )
            with self.assertRaises(ModelStoreError) as stage_rejects_full:
                harness.preparer.prepare_stage(
                    identity=_identity(_full_manifest()),
                    manifest=_full_manifest(),
                    origin=ORIGIN,
                )
            self.assertEqual(
                stage_rejects_full.exception.code,
                "MODEL_STORE_CACHE_KIND_UNSUPPORTED",
            )
            self.assertEqual(harness.transport.calls, [])
        finally:
            harness.close()


class ArtifactManifestBoundaryTests(unittest.TestCase):
    def test_canonical_order_is_stable_for_files_and_chunks(self):
        manifest = _full_manifest()
        reordered = copy.deepcopy(manifest)
        reordered["files"].reverse()
        for item in reordered["files"]:
            item["chunks"].reverse()

        first = parse_artifact_manifest(manifest)
        second = parse_artifact_manifest(reordered)

        self.assertEqual(first.digest, second.digest)
        self.assertEqual(first.canonical_json, second.canonical_json)
        self.assertEqual(
            [item["path"] for item in first.document["files"]],
            ["config.json", "weights/model.bin"],
        )
        self.assertEqual(
            [
                chunk["ordinal"]
                for chunk in first.document["files"][1]["chunks"]
            ],
            [0, 1],
        )

    def test_reserved_marker_and_unsafe_ranges_are_rejected(self):
        reserved = {
            "schema_version": 1,
            "files": [
                _regular_file(MODEL_CACHE_MARKER_FILE, b"reserved"),
            ],
        }
        with self.assertRaises(ValueError):
            parse_artifact_manifest(
                reserved,
                reserved_paths={MODEL_CACHE_MARKER_FILE},
            )

        base = {
            "schema_version": 1,
            "files": [_regular_file("weights.bin", b"abcd", chunk_size=2)],
        }
        invalid = []
        value = copy.deepcopy(base)
        value["files"][0]["chunks"][1]["ordinal"] = 2
        invalid.append(value)
        value = copy.deepcopy(base)
        value["files"][0]["chunks"][1]["offset_bytes"] = 3
        invalid.append(value)
        value = copy.deepcopy(base)
        value["files"][0]["chunks"][1]["offset_bytes"] = 1
        invalid.append(value)
        value = copy.deepcopy(base)
        value["files"][0]["size_bytes"] = 5
        invalid.append(value)
        value = copy.deepcopy(base)
        value["files"][0]["chunks"][0]["length_bytes"] = 0
        invalid.append(value)

        for manifest in invalid:
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                parse_artifact_manifest(manifest)

    def test_every_manifest_limit_fails_before_materialization(self):
        manifest = _full_manifest()
        limits = ArtifactManifestLimits()
        invalid_limits = (
            replace(limits, max_files=1),
            replace(limits, max_chunks=2),
            replace(limits, max_path_length=5),
            replace(limits, max_file_bytes=8),
            replace(limits, max_total_bytes=16),
        )

        for bounded in invalid_limits:
            with self.subTest(limits=bounded), self.assertRaises(ValueError):
                parse_artifact_manifest(manifest, limits=bounded)


class AttemptJournalTests(unittest.TestCase):
    def test_schema_version_boolean_is_not_accepted_as_v1(self):
        value = AttemptJournal(
            manifest_digest="sha256:" + "1" * 64,
            chunk_digest=None,
            bytes_complete=0,
            status="ASSEMBLING",
        ).to_dict()
        value["schema_version"] = True

        with self.assertRaises(ModelStoreError) as caught:
            AttemptJournal.from_dict(value)

        self.assertEqual(caught.exception.code, "MODEL_STORE_JOURNAL_CORRUPT")


class ModelStoreLockAndCASTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.store = ContentAddressedModelStore(
            store_root=root / "store",
            model_root=root / "models",
        )

    def tearDown(self):
        self.temporary.cleanup()

    def test_same_artifact_lock_contends_but_another_artifact_does_not(self):
        first = _digest(b"artifact-one")
        second = _digest(b"artifact-two")

        with self.store.artifact_lock(first):
            with self.assertRaises(ModelStoreError) as caught:
                with self.store.artifact_lock(first, blocking=False):
                    self.fail("the same artifact lock was acquired twice")
            self.assertEqual(caught.exception.code, "MODEL_STORE_LOCK_BUSY")
            with self.store.artifact_lock(second, blocking=False):
                pass

    def test_different_artifacts_contending_for_one_chunk_download_once(self):
        payload = b"one-shared-chunk"
        chunk_digest = _digest(payload)
        second_attempted = threading.Event()

        class CoordinatedTransport(MemoryTransport):
            def open(inner_self, *args, **kwargs):
                if not second_attempted.wait(5):
                    raise AssertionError("second artifact never reached the chunk lock")
                return super().open(*args, **kwargs)

        transport = CoordinatedTransport({chunk_digest: payload})
        downloader = ArtifactChunkDownloader(
            self.store,
            transport=transport,
            attempts=1,
        )
        original_chunk_lock = self.store.chunk_lock
        worker_state = threading.local()

        @contextmanager
        def observed_chunk_lock(digest, *, blocking=True):
            if getattr(worker_state, "second", False):
                second_attempted.set()
            with original_chunk_lock(digest, blocking=blocking) as path:
                yield path

        self.store.chunk_lock = observed_chunk_lock

        def download(manifest_digest: str, *, second: bool) -> str:
            worker_state.second = second
            return downloader.download_chunk(
                origin=ORIGIN,
                manifest_digest=manifest_digest,
                chunk_digest=chunk_digest,
                expected_size=len(payload),
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(
                download,
                _digest(b"manifest-one"),
                second=False,
            )
            second = executor.submit(
                download,
                _digest(b"manifest-two"),
                second=True,
            )
            first_result = first.result(timeout=5)
            second_result = second.result(timeout=5)

        self.assertEqual(first_result, second_result)
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(Path(first_result).read_bytes(), payload)

    def test_exact_chunk_is_reused_without_network_and_corruption_is_preserved(self):
        payload = b"already-verified"
        chunk_digest = _digest(payload)
        with self.store.chunk_lock(chunk_digest):
            _, descriptor, _ = self.store.open_chunk_partial(
                chunk_digest, len(payload)
            )
            try:
                os.write(descriptor, payload)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            published = self.store.publish_chunk_partial(
                chunk_digest, len(payload)
            )

        class NoNetworkTransport:
            def open(self, *args, **kwargs):
                raise AssertionError("verified CAS content used the network")

        downloader = ArtifactChunkDownloader(
            self.store,
            transport=NoNetworkTransport(),
            attempts=1,
        )
        progress = []
        reused = downloader.download_chunk(
            origin=ORIGIN,
            manifest_digest=_digest(b"verified-manifest"),
            chunk_digest=chunk_digest,
            expected_size=len(payload),
            progress_callback=lambda *values: progress.append(values),
        )
        self.assertEqual(Path(reused), published)
        self.assertEqual(
            progress,
            [(chunk_digest, len(payload), len(payload))],
        )

        published.write_bytes(b"X" * len(payload))
        with self.assertRaises(ModelStoreError) as caught:
            downloader.download_chunk(
                origin=ORIGIN,
                manifest_digest=_digest(b"corrupt-manifest"),
                chunk_digest=chunk_digest,
                expected_size=len(payload),
            )
        self.assertEqual(caught.exception.code, "MODEL_STORE_CHUNK_CORRUPT")
        self.assertEqual(published.read_bytes(), b"X" * len(payload))


class ModelStoreRootBoundaryTests(unittest.TestCase):
    def test_world_writable_nearest_ancestor_is_rejected_before_creation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            unsafe = root / "unsafe-parent"
            unsafe.mkdir()
            unsafe.chmod(0o777)
            target = unsafe / "store"
            store = ContentAddressedModelStore(
                store_root=target,
                model_root=root / "models",
            )

            with self.assertRaises(ModelStoreError) as caught:
                store.initialize()

            self.assertEqual(caught.exception.code, "MODEL_STORE_ROOT_UNSAFE")
            self.assertFalse(target.exists())

    def test_symlink_ancestor_is_rejected_without_writing_through_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            outside = root / "outside"
            outside.mkdir()
            linked = root / "linked"
            linked.symlink_to(outside, target_is_directory=True)
            store = ContentAddressedModelStore(
                store_root=linked / "store",
                model_root=root / "models",
            )

            with self.assertRaises(ModelStoreError) as caught:
                store.initialize()

            self.assertEqual(caught.exception.code, "MODEL_STORE_ROOT_UNSAFE")
            self.assertFalse((outside / "store").exists())

    def test_debian_postinstall_keeps_server_and_agent_ownership_separate(self):
        repository = Path(__file__).resolve().parents[1]
        script = (
            repository / "debian" / "dure.postinst"
        ).read_text(encoding="utf-8")
        server_unit = (repository / "packaging" / "dure-server.service").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            "install -d -m 0750 -o root -g dure /var/lib/dure\n",
            script,
        )
        self.assertIn(
            "install -d -m 0750 -o dure -g dure /var/lib/dure/server\n",
            script,
        )
        self.assertIn("ProtectSystem=strict\n", server_unit)
        self.assertIn("ReadWritePaths=/var/lib/dure/server\n", server_unit)


class FullSnapshotPreparationTests(unittest.TestCase):
    def setUp(self):
        self.harness = CacheHarness()

    def tearDown(self):
        self.harness.close()

    def _prepare_or_skip(self, harness: CacheHarness | None = None):
        selected = harness or self.harness
        try:
            return selected.preparer.prepare_full_snapshot(
                identity=selected.identity,
                manifest=selected.manifest,
                origin=ORIGIN,
            )
        except ModelStoreError as exc:
            if exc.code == "MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE":
                self.skipTest("Linux renameat2(RENAME_NOREPLACE) is unavailable")
            raise

    def test_success_writes_marker_last_exact_tree_and_closed_journal(self):
        observations: list[Path] = []
        journals = []
        original_marker = ModelCachePreparer._write_marker
        original_journal = self.harness.store.write_attempt

        def checked_marker(staging: Path, identity: CacheIdentity) -> None:
            self.assertFalse((staging / MODEL_CACHE_MARKER_FILE).exists())
            self.assertEqual((staging / "config.json").read_bytes(), CONFIG_BYTES)
            self.assertEqual(
                (staging / "weights/model.bin").read_bytes(),
                WEIGHT_BYTES,
            )
            observations.append(staging)
            original_marker(staging, identity)

        def collect_journal(journal):
            journals.append(journal)
            return original_journal(journal)

        with patch.object(
            ModelCachePreparer,
            "_write_marker",
            staticmethod(checked_marker),
        ), patch.object(
            self.harness.store,
            "write_attempt",
            side_effect=collect_journal,
        ):
            result = self._prepare_or_skip()

        self.assertFalse(result.reused)
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].parent, self.harness.store.model_staging_root)
        self.assertFalse(observations[0].exists())
        self.assertEqual(
            sorted(
                path.relative_to(result.path).as_posix()
                for path in result.path.rglob("*")
                if path.is_file()
            ),
            [
                MODEL_CACHE_MARKER_FILE,
                "config.json",
                "weights/model.bin",
            ],
        )
        marker = json.loads(
            (result.path / MODEL_CACHE_MARKER_FILE).read_text(encoding="utf-8")
        )
        self.assertEqual(marker, self.harness.identity.marker())
        self.assertEqual(marker["schema"], MODEL_CACHE_SCHEMA_V2)
        self.assertEqual(
            [journal.status for journal in journals[-4:]],
            ["ASSEMBLING", "VERIFYING", "ACTIVATING", "SUCCEEDED"],
        )
        final_journal = self.harness.store.read_attempt(
            self.harness.identity.manifest_digest
        )
        self.assertEqual(final_journal.status, "SUCCEEDED")
        self.assertIsNone(final_journal.chunk_digest)
        self.assertEqual(
            final_journal.bytes_complete,
            self.harness.parsed.total_size_bytes,
        )
        self.assertEqual(list(self.harness.store.model_staging_root.iterdir()), [])

    def test_idempotent_exact_cache_reuses_every_file_without_network(self):
        first = self._prepare_or_skip()
        call_count = len(self.harness.transport.calls)

        second = self._prepare_or_skip()

        self.assertTrue(second.reused)
        self.assertEqual(second.path, first.path)
        self.assertEqual(len(self.harness.transport.calls), call_count)

    def test_disk_shortage_fails_before_network_or_staging_creation(self):
        self.harness.close()
        self.harness = CacheHarness(
            disk_usage=lambda path: SimpleNamespace(free=0),
        )

        with self.assertRaises(ModelStoreError) as caught:
            self.harness.preparer.prepare_full_snapshot(
                identity=self.harness.identity,
                manifest=self.harness.manifest,
                origin=ORIGIN,
            )

        self.assertEqual(caught.exception.code, "MODEL_STORE_DISK_INSUFFICIENT")
        self.assertEqual(self.harness.transport.calls, [])
        self.assertFalse(
            self.harness.store.model_cache_path(
                self.harness.identity.manifest_digest
            ).exists()
        )
        self.assertEqual(list(self.harness.store.model_staging_root.iterdir()), [])
        journal = self.harness.store.read_attempt(
            self.harness.identity.manifest_digest
        )
        self.assertEqual(journal.status, "FAILED")
        self.assertEqual(
            journal.failure_code,
            "MODEL_STORE_DISK_INSUFFICIENT",
        )

    def test_file_digest_mismatch_never_creates_marker_or_final_cache(self):
        self.harness.close()
        manifest = _full_manifest(config_digest="sha256:" + "0" * 64)
        self.harness = CacheHarness(manifest=manifest)

        with self.assertRaises(ModelStoreError) as caught:
            self.harness.preparer.prepare_full_snapshot(
                identity=self.harness.identity,
                manifest=self.harness.manifest,
                origin=ORIGIN,
            )

        self.assertEqual(
            caught.exception.code,
            "MODEL_STORE_FILE_INTEGRITY_FAILED",
        )
        final = self.harness.store.model_cache_path(
            self.harness.identity.manifest_digest
        )
        self.assertFalse(final.exists())
        stages = list(self.harness.store.model_staging_root.iterdir())
        self.assertEqual(len(stages), 1)
        self.assertFalse((stages[0] / MODEL_CACHE_MARKER_FILE).exists())
        parts = [
            path
            for path in stages[0].rglob("*")
            if path.is_file() and path.name.endswith(".part")
        ]
        self.assertEqual(len(parts), 1)
        self.assertFalse((stages[0] / "config.json").exists())
        journal = self.harness.store.read_attempt(
            self.harness.identity.manifest_digest
        )
        self.assertEqual(journal.status, "FAILED")
        self.assertEqual(
            journal.failure_code,
            "MODEL_STORE_FILE_INTEGRITY_FAILED",
        )

    def test_repeated_integrity_failure_reuses_one_bounded_staging_tree(self):
        self.harness.close()
        manifest = _full_manifest(config_digest="sha256:" + "0" * 64)
        self.harness = CacheHarness(manifest=manifest)

        with self.assertRaises(ModelStoreError) as first:
            self.harness.preparer.prepare_full_snapshot(
                identity=self.harness.identity,
                manifest=self.harness.manifest,
                origin=ORIGIN,
            )
        self.assertEqual(
            first.exception.code,
            "MODEL_STORE_FILE_INTEGRITY_FAILED",
        )
        stages_before = list(self.harness.store.model_staging_root.iterdir())
        self.assertEqual(len(stages_before), 1)
        files_before = {
            path.relative_to(stages_before[0]).as_posix(): (
                path.stat().st_size,
                path.stat().st_blocks,
            )
            for path in stages_before[0].rglob("*")
            if path.is_file()
        }
        calls_before = len(self.harness.transport.calls)

        with self.assertRaises(ModelStoreError) as second:
            self.harness.preparer.prepare_full_snapshot(
                identity=self.harness.identity,
                manifest=self.harness.manifest,
                origin=ORIGIN,
            )

        self.assertEqual(
            second.exception.code,
            "MODEL_STORE_FILE_INTEGRITY_FAILED",
        )
        stages_after = list(self.harness.store.model_staging_root.iterdir())
        self.assertEqual(stages_after, stages_before)
        files_after = {
            path.relative_to(stages_after[0]).as_posix(): (
                path.stat().st_size,
                path.stat().st_blocks,
            )
            for path in stages_after[0].rglob("*")
            if path.is_file()
        }
        self.assertEqual(files_after, files_before)
        self.assertEqual(len(self.harness.transport.calls), calls_before)
        self.assertFalse(
            (
                self.harness.store.model_cache_path(
                    self.harness.identity.manifest_digest
                )
                / MODEL_CACHE_MARKER_FILE
            ).exists()
        )

    def test_verified_partial_staging_file_is_reused_after_interruption(self):
        original_assemble_file = self.harness.preparer._assemble_file

        def interrupt_before_weights(
            staging: Path,
            work: Path,
            item: dict,
        ) -> None:
            if item["path"] == "weights/model.bin":
                raise ModelStoreError("MODEL_STORE_IO_FAILED")
            original_assemble_file(staging, work, item)

        with patch.object(
            self.harness.preparer,
            "_assemble_file",
            side_effect=interrupt_before_weights,
        ):
            with self.assertRaises(ModelStoreError) as interrupted:
                self.harness.preparer.prepare_full_snapshot(
                    identity=self.harness.identity,
                    manifest=self.harness.manifest,
                    origin=ORIGIN,
                )
        self.assertEqual(interrupted.exception.code, "MODEL_STORE_IO_FAILED")
        stages = list(self.harness.store.model_staging_root.iterdir())
        self.assertEqual(len(stages), 1)
        staged_config = stages[0] / "config.json"
        staged_identity = (staged_config.stat().st_dev, staged_config.stat().st_ino)
        self.assertFalse((stages[0] / MODEL_CACHE_MARKER_FILE).exists())
        calls_before = len(self.harness.transport.calls)

        result = self._prepare_or_skip()

        final_config = result.path / "config.json"
        self.assertEqual(
            (final_config.stat().st_dev, final_config.stat().st_ino),
            staged_identity,
        )
        self.assertEqual(len(self.harness.transport.calls), calls_before)
        self.assertEqual(list(self.harness.store.model_staging_root.iterdir()), [])

    def test_partial_staging_bytes_reduce_resume_disk_requirement(self):
        original_write_all = model_store_module._write_all
        interrupted = False

        def write_prefix_then_stop(descriptor: int, payload: bytes) -> None:
            nonlocal interrupted
            if not interrupted:
                interrupted = True
                prefix = payload[: max(1, len(payload) // 2)]
                os.write(descriptor, prefix)
                os.fsync(descriptor)
                raise ModelStoreError("MODEL_STORE_DISK_INSUFFICIENT")
            original_write_all(descriptor, payload)

        with patch.object(
            model_store_module,
            "_write_all",
            side_effect=write_prefix_then_stop,
        ):
            with self.assertRaises(ModelStoreError) as first:
                self.harness.preparer.prepare_full_snapshot(
                    identity=self.harness.identity,
                    manifest=self.harness.manifest,
                    origin=ORIGIN,
                )
        self.assertEqual(
            first.exception.code,
            "MODEL_STORE_DISK_INSUFFICIENT",
        )
        stages = list(self.harness.store.model_staging_root.iterdir())
        self.assertEqual(len(stages), 1)
        parts = [
            path
            for path in stages[0].rglob("*")
            if path.is_file() and path.name.endswith(".part")
        ]
        self.assertEqual(len(parts), 1)
        partial_bytes = parts[0].stat().st_size
        self.assertGreater(partial_bytes, 0)
        self.assertLess(partial_bytes, self.harness.parsed.total_size_bytes)
        calls_before = len(self.harness.transport.calls)
        free_bytes = (
            self.harness.parsed.total_size_bytes
            - partial_bytes
            + 64 * 1024
        )
        self.harness.preparer = ModelCachePreparer(
            self.harness.store,
            self.harness.downloader,
            disk_usage=lambda path: SimpleNamespace(free=free_bytes),
            disk_reserve_bytes=0,
        )

        result = self._prepare_or_skip()

        self.assertEqual((result.path / "config.json").read_bytes(), CONFIG_BYTES)
        self.assertEqual(len(self.harness.transport.calls), calls_before)
        self.assertEqual(list(self.harness.store.model_staging_root.iterdir()), [])

    def test_partial_marker_write_is_retried_without_manual_cleanup(self):
        original_write_all = model_store_module._write_all
        marker_payload = (
            json.dumps(
                self.harness.identity.marker(),
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        interrupted = False

        def interrupt_marker(descriptor: int, payload: bytes) -> None:
            nonlocal interrupted
            if payload == marker_payload and not interrupted:
                interrupted = True
                os.write(descriptor, payload[: max(1, len(payload) // 2)])
                os.fsync(descriptor)
                raise ModelStoreError("MODEL_STORE_DISK_INSUFFICIENT")
            original_write_all(descriptor, payload)

        with patch.object(
            model_store_module,
            "_write_all",
            side_effect=interrupt_marker,
        ):
            with self.assertRaises(ModelStoreError) as caught:
                self.harness.preparer.prepare_full_snapshot(
                    identity=self.harness.identity,
                    manifest=self.harness.manifest,
                    origin=ORIGIN,
                )

        self.assertEqual(
            caught.exception.code,
            "MODEL_STORE_DISK_INSUFFICIENT",
        )
        stages = list(self.harness.store.model_staging_root.iterdir())
        self.assertEqual(len(stages), 1)
        partial = stages[0] / DURE_MODEL_STAGING_MARKER_PART_FILE
        self.assertTrue(partial.is_file())
        self.assertFalse((stages[0] / MODEL_CACHE_MARKER_FILE).exists())
        self.assertFalse(
            self.harness.store.model_cache_path(
                self.harness.identity.manifest_digest
            ).exists()
        )
        calls_before = len(self.harness.transport.calls)

        result = self._prepare_or_skip()

        self.assertTrue((result.path / MODEL_CACHE_MARKER_FILE).is_file())
        self.assertFalse(partial.exists())
        self.assertEqual(len(self.harness.transport.calls), calls_before)
        self.assertEqual(list(self.harness.store.model_staging_root.iterdir()), [])

    def test_stage_and_reserved_marker_paths_fail_before_network(self):
        stage_identity = _identity(
            self.harness.manifest,
            cache_kind=MODEL_CACHE_KIND_STAGE,
        )
        with self.assertRaises(ModelStoreError) as caught:
            self.harness.preparer.prepare_full_snapshot(
                identity=stage_identity,
                manifest=self.harness.manifest,
                origin=ORIGIN,
            )
        self.assertEqual(
            caught.exception.code,
            "MODEL_STORE_CACHE_KIND_UNSUPPORTED",
        )
        self.assertEqual(self.harness.transport.calls, [])

        reserved_paths = (
            MODEL_CACHE_MARKER_FILE,
            f"{MODEL_CACHE_MARKER_FILE}/child",
            DURE_MODEL_STAGING_MARKER_PART_FILE,
            f"{DURE_MODEL_STAGING_MARKER_PART_FILE}/child",
            DURE_MODEL_STAGING_WORK_DIRECTORY,
            f"{DURE_MODEL_STAGING_WORK_DIRECTORY}/child",
        )
        for path in reserved_paths:
            with self.subTest(path=path):
                manifest = {
                    "schema_version": 1,
                    "files": [
                        _regular_file("config.json", CONFIG_BYTES),
                        _regular_file(path, b"reserved"),
                    ],
                }
                objects = {
                    _digest(CONFIG_BYTES): CONFIG_BYTES,
                    _digest(b"reserved"): b"reserved",
                }
                harness = CacheHarness(manifest=manifest, objects=objects)
                try:
                    with self.assertRaises(ModelStoreError) as reserved_caught:
                        harness.preparer.prepare_full_snapshot(
                            identity=harness.identity,
                            manifest=harness.manifest,
                            origin=ORIGIN,
                        )
                    self.assertEqual(
                        reserved_caught.exception.code,
                        "MODEL_STORE_MANIFEST_MISMATCH",
                    )
                    self.assertEqual(harness.transport.calls, [])
                finally:
                    harness.close()

    def test_invalid_or_mismatched_model_config_never_activates(self):
        oversized = b'{' + b'"padding":"' + (
            b"x" * MAX_MODEL_CONFIG_BYTES
        ) + b'"}'
        cases = {
            "invalid-json": b"{",
            "oversized": oversized,
            "non-object": b"[]",
            "duplicate-key": b'{"model_type":"a","model_type":"b"}',
            "quantization-mismatch": json.dumps(
                {
                    "model_type": "dure-test",
                    "quantization_config": {"quant_method": "awq"},
                },
                separators=(",", ":"),
            ).encode("utf-8"),
        }

        for name, config in cases.items():
            with self.subTest(name=name):
                manifest = _full_manifest(config_payload=config)
                objects = _objects()
                objects.pop(_digest(CONFIG_BYTES))
                objects[_digest(config)] = config
                harness = CacheHarness(manifest=manifest, objects=objects)
                try:
                    with self.assertRaises(ModelStoreError) as caught:
                        harness.preparer.prepare_full_snapshot(
                            identity=harness.identity,
                            manifest=harness.manifest,
                            origin=ORIGIN,
                        )
                    self.assertEqual(
                        caught.exception.code,
                        "MODEL_STORE_MANIFEST_MISMATCH",
                    )
                    final = harness.store.model_cache_path(
                        harness.identity.manifest_digest
                    )
                    self.assertFalse(final.exists())
                    stages = list(harness.store.model_staging_root.iterdir())
                    self.assertEqual(len(stages), 1)
                    self.assertFalse(
                        (stages[0] / MODEL_CACHE_MARKER_FILE).exists()
                    )
                finally:
                    harness.close()

    def test_existing_exact_cache_with_mismatched_quantization_is_preserved(self):
        config = json.dumps(
            {
                "model_type": "dure-test",
                "quantization_config": {"quant_method": "awq"},
            },
            separators=(",", ":"),
        ).encode("utf-8")
        manifest = _full_manifest(config_payload=config)
        parsed = parse_artifact_manifest(manifest)
        identity = CacheIdentity(
            repository="Example/Dure-Model",
            revision="a" * 40,
            manifest_digest=parsed.digest,
            quantization="gptq",
        )
        objects = _objects()
        objects.pop(_digest(CONFIG_BYTES))
        objects[_digest(config)] = config
        harness = CacheHarness(
            manifest=manifest,
            identity=identity,
            objects=objects,
        )
        try:
            harness.store.initialize_model_layout()
            final = harness.store.model_cache_path(identity.manifest_digest)
            (final / "weights").mkdir(parents=True)
            (final / "config.json").write_bytes(config)
            (final / "weights/model.bin").write_bytes(WEIGHT_BYTES)
            (final / MODEL_CACHE_MARKER_FILE).write_text(
                json.dumps(identity.marker(), sort_keys=True) + "\n",
                encoding="utf-8",
            )
            before = {
                path.relative_to(final).as_posix(): path.read_bytes()
                for path in final.rglob("*")
                if path.is_file()
            }

            with self.assertRaises(ModelStoreError) as caught:
                harness.preparer.prepare_full_snapshot(
                    identity=identity,
                    manifest=manifest,
                    origin=ORIGIN,
                )

            self.assertEqual(
                caught.exception.code,
                "MODEL_STORE_TARGET_COLLISION",
            )
            after = {
                path.relative_to(final).as_posix(): path.read_bytes()
                for path in final.rglob("*")
                if path.is_file()
            }
            self.assertEqual(after, before)
            self.assertEqual(harness.transport.calls, [])
            journal = harness.store.read_attempt(identity.manifest_digest)
            self.assertEqual(journal.status, "FAILED")
            self.assertEqual(
                journal.failure_code,
                "MODEL_STORE_TARGET_COLLISION",
            )
        finally:
            harness.close()

    def test_path_escape_manifests_fail_without_writing_outside_the_root(self):
        outside = self.harness.root / "outside.bin"
        for index, path in enumerate(
            ("../outside.bin", "/outside.bin", "a/../outside.bin", "a\\b.bin")
        ):
            with self.subTest(path=path):
                manifest = {
                    "schema_version": 1,
                    "files": [
                        _regular_file("config.json", CONFIG_BYTES),
                        _regular_file(path, b"escape"),
                    ],
                }
                identity = CacheIdentity(
                    repository="Example/Dure-Model",
                    revision="a" * 40,
                    manifest_digest=_digest(f"invalid-{index}".encode()),
                    quantization="fp16",
                )
                with self.assertRaises(ModelStoreError) as caught:
                    self.harness.preparer.prepare_full_snapshot(
                        identity=identity,
                        manifest=manifest,
                        origin=ORIGIN,
                    )
                self.assertEqual(
                    caught.exception.code,
                    "MODEL_STORE_MANIFEST_MISMATCH",
                )
        self.assertFalse(outside.exists())
        self.assertEqual(self.harness.transport.calls, [])

    def test_polluted_deterministic_staging_is_preserved_and_rejected(self):
        self.harness.store.initialize_model_layout()
        staging = self.harness.store.model_staging_path(
            self.harness.identity.manifest_digest
        )
        staging.mkdir(mode=0o700)
        outside = self.harness.root / "operator-file"
        outside.write_bytes(b"preserve")
        polluted = staging / "config.json"
        polluted.symlink_to(outside)

        with self.assertRaises(ModelStoreError) as caught:
            self.harness.preparer.prepare_full_snapshot(
                identity=self.harness.identity,
                manifest=self.harness.manifest,
                origin=ORIGIN,
            )

        self.assertEqual(caught.exception.code, "MODEL_STORE_TARGET_COLLISION")
        self.assertEqual(self.harness.transport.calls, [])
        self.assertTrue(polluted.is_symlink())
        self.assertEqual(outside.read_bytes(), b"preserve")
        self.assertFalse(
            self.harness.store.model_cache_path(
                self.harness.identity.manifest_digest
            ).exists()
        )

    def test_full_staging_rejects_stage_sidecar_partial_before_network(self):
        self.harness.store.initialize_model_layout()
        staging = self.harness.store.model_staging_path(
            self.harness.identity.manifest_digest
        )
        staging.mkdir(mode=0o700)
        sidecar_partial = staging / f"{STAGE_CACHE_MANIFEST_FILE}.part"
        sidecar_partial.write_bytes(b"operator-owned")
        sidecar_partial.chmod(0o600)

        with self.assertRaises(ModelStoreError) as caught:
            self.harness.preparer.prepare_full_snapshot(
                identity=self.harness.identity,
                manifest=self.harness.manifest,
                origin=ORIGIN,
            )

        self.assertEqual(caught.exception.code, "MODEL_STORE_TARGET_COLLISION")
        self.assertEqual(self.harness.transport.calls, [])
        self.assertEqual(sidecar_partial.read_bytes(), b"operator-owned")

    def test_existing_empty_foreign_symlink_and_special_targets_are_preserved(self):
        scenarios = ["empty", "foreign", "symlink"]
        if hasattr(os, "mkfifo"):
            scenarios.append("fifo")

        for scenario in scenarios:
            with self.subTest(scenario=scenario):
                harness = CacheHarness()
                try:
                    harness.store.initialize_model_layout()
                    final = harness.store.model_cache_path(
                        harness.identity.manifest_digest
                    )
                    outside = harness.root / "outside"
                    if scenario == "empty":
                        final.mkdir()
                    elif scenario == "foreign":
                        final.mkdir()
                        (final / "operator-sentinel").write_text(
                            "preserve",
                            encoding="utf-8",
                        )
                    elif scenario == "symlink":
                        outside.mkdir()
                        (outside / "operator-sentinel").write_text(
                            "preserve",
                            encoding="utf-8",
                        )
                        final.symlink_to(outside, target_is_directory=True)
                    else:
                        os.mkfifo(final)

                    with self.assertRaises(ModelStoreError) as caught:
                        harness.preparer.prepare_full_snapshot(
                            identity=harness.identity,
                            manifest=harness.manifest,
                            origin=ORIGIN,
                        )
                    self.assertEqual(
                        caught.exception.code,
                        "MODEL_STORE_TARGET_COLLISION",
                    )
                    self.assertEqual(harness.transport.calls, [])
                    if scenario == "empty":
                        self.assertTrue(final.is_dir())
                        self.assertEqual(list(final.iterdir()), [])
                    elif scenario == "foreign":
                        self.assertEqual(
                            (final / "operator-sentinel").read_text(
                                encoding="utf-8"
                            ),
                            "preserve",
                        )
                    elif scenario == "symlink":
                        self.assertTrue(final.is_symlink())
                        self.assertEqual(
                            (outside / "operator-sentinel").read_text(
                                encoding="utf-8"
                            ),
                            "preserve",
                        )
                    else:
                        self.assertTrue(stat.S_ISFIFO(final.lstat().st_mode))
                finally:
                    harness.close()

    def test_existing_exact_tree_rejects_mutation_and_extra_entries(self):
        scenarios = ["content", "extra", "symlink", "hardlink"]
        if hasattr(os, "mkfifo"):
            scenarios.append("fifo")

        for scenario in scenarios:
            with self.subTest(scenario=scenario):
                harness = CacheHarness()
                try:
                    result = self._prepare_or_skip(harness)
                    call_count = len(harness.transport.calls)
                    weight = result.path / "weights/model.bin"
                    if scenario == "content":
                        weight.write_bytes(b"X" * len(WEIGHT_BYTES))
                    elif scenario == "extra":
                        (result.path / "operator-extra").write_text(
                            "preserve",
                            encoding="utf-8",
                        )
                    elif scenario == "symlink":
                        outside = harness.root / "outside.bin"
                        outside.write_bytes(WEIGHT_BYTES)
                        weight.unlink()
                        weight.symlink_to(outside)
                    elif scenario == "hardlink":
                        os.link(weight, harness.root / "outside-hardlink.bin")
                    else:
                        weight.unlink()
                        os.mkfifo(weight)

                    with self.assertRaises(ModelStoreError) as caught:
                        harness.preparer.prepare_full_snapshot(
                            identity=harness.identity,
                            manifest=harness.manifest,
                            origin=ORIGIN,
                        )
                    self.assertEqual(
                        caught.exception.code,
                        "MODEL_STORE_TARGET_COLLISION",
                    )
                    self.assertEqual(len(harness.transport.calls), call_count)
                    self.assertTrue(result.path.exists())
                finally:
                    harness.close()

    def test_exact_target_created_during_activation_is_not_replaced(self):
        original_rename = _rename_noreplace

        def collide(source: Path, target: Path) -> None:
            if not source.is_dir():
                original_rename(source, target)
                return
            shutil.copytree(source, target, copy_function=shutil.copy2)
            original_rename(source, target)

        with patch.object(
            model_store_module,
            "_rename_noreplace",
            side_effect=collide,
        ):
            with self.assertRaises(ModelStoreError) as caught:
                self.harness.preparer.prepare_full_snapshot(
                    identity=self.harness.identity,
                    manifest=self.harness.manifest,
                    origin=ORIGIN,
                )

        if caught.exception.code == "MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE":
            self.skipTest("Linux renameat2(RENAME_NOREPLACE) is unavailable")
        self.assertEqual(caught.exception.code, "MODEL_STORE_TARGET_COLLISION")
        final = self.harness.store.model_cache_path(
            self.harness.identity.manifest_digest
        )
        self.assertTrue((final / MODEL_CACHE_MARKER_FILE).is_file())
        stages = list(self.harness.store.model_staging_root.iterdir())
        self.assertEqual(len(stages), 1)
        self.assertTrue((stages[0] / MODEL_CACHE_MARKER_FILE).is_file())
        journal = self.harness.store.read_attempt(
            self.harness.identity.manifest_digest
        )
        self.assertEqual(journal.status, "FAILED")
        self.assertEqual(journal.failure_code, "MODEL_STORE_TARGET_COLLISION")

    def test_probe_excludes_only_the_fixed_hidden_staging_directory(self):
        self.harness.store.initialize_model_layout()
        staging_model = self.harness.store.model_staging_root / "unfinished"
        staging_model.mkdir()
        (staging_model / "config.json").write_text("{}", encoding="utf-8")
        visible = self.harness.store.model_root / "visible"
        hidden_user_model = self.harness.store.model_root / ".custom-model"
        for candidate in (visible, hidden_user_model):
            candidate.mkdir()
            (candidate / "config.json").write_text("{}", encoding="utf-8")

        models = NodeProbe(
            FakeRunner(),
            model_roots=(self.harness.store.model_root,),
        )._probe_dure_models(self.harness.store.model_root)

        self.assertEqual(
            {Path(model.path).name for model in models},
            {"visible", ".custom-model"},
        )
        self.assertNotIn(
            DURE_MODEL_STAGING_DIRECTORY,
            {Path(model.path).name for model in models},
        )


if __name__ == "__main__":
    unittest.main()
