from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import event, func, select

from dure.control import service as service_module
from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import (
    ArtifactChunk,
    ArtifactFileChunk,
    ArtifactManifest,
    ArtifactManifestFile,
    AuditEvent,
    Deployment,
    ModelArtifact,
    Task,
)
from dure.control.service import (
    ArtifactManifestConflictError,
    ArtifactManifestNotFoundError,
    artifact_manifest_dict,
    canonical_artifact_manifest_digest,
    create_model_artifact,
    get_artifact_manifest,
    register_artifact_manifest,
)


COUNTED_MODELS = (
    ArtifactManifest,
    ArtifactManifestFile,
    ArtifactChunk,
    ArtifactFileChunk,
    AuditEvent,
    Deployment,
    Task,
)


def _digest(seed: str) -> str:
    return "sha256:" + hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _manifest() -> dict:
    shared = _digest("shared-four-byte-chunk")
    return {
        "schema_version": 1,
        "files": [
            {
                "path": "empty.txt",
                "kind": "REGULAR",
                "size_bytes": 0,
                "sha256": _digest("empty-file"),
                "chunks": [],
            },
            {
                "path": "weights/b.bin",
                "kind": "REGULAR",
                "size_bytes": 4,
                "sha256": _digest("file-b"),
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": 4,
                        "sha256": shared,
                    }
                ],
            },
            {
                "path": "weights/a.bin",
                "kind": "REGULAR",
                "size_bytes": 8,
                "sha256": _digest("file-a"),
                "chunks": [
                    {
                        "ordinal": 1,
                        "offset_bytes": 4,
                        "length_bytes": 4,
                        "sha256": _digest("file-a-tail"),
                    },
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": 4,
                        "sha256": shared,
                    },
                ],
            },
        ],
    }


def _reordered_manifest() -> dict:
    value = copy.deepcopy(_manifest())
    value["files"].reverse()
    for item in value["files"]:
        item["chunks"].reverse()
        item["sha256"] = str(item["sha256"])
    return value


def _row_counts(session) -> dict[str, int]:
    return {
        model.__tablename__: session.scalar(select(func.count()).select_from(model))
        for model in COUNTED_MODELS
    }


def _create_artifact(session, key: str, manifest: dict | None = None) -> ModelArtifact:
    source = manifest or _manifest()
    return create_model_artifact(
        session,
        model_id=f"artifact-{key}",
        repository=f"TestOrg/Artifact-{key}",
        revision=hashlib.sha256(f"revision-{key}".encode()).hexdigest()[:40],
        manifest_digest=canonical_artifact_manifest_digest(source),
        quantization="awq",
        size_mib=1,
        default_max_model_len=1024,
        layer_count=1,
        license_id="apache-2.0",
    )


class ArtifactManifestServiceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.engine = make_engine(
            f"sqlite:///{Path(self.temporary.name) / 'artifact-manifests.db'}"
        )
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)

    def tearDown(self):
        self.engine.dispose()
        self.temporary.cleanup()

    def test_registration_is_canonical_idempotent_and_read_only_for_hosts(self):
        original = _manifest()
        reordered = _reordered_manifest()
        self.assertEqual(
            canonical_artifact_manifest_digest(original),
            canonical_artifact_manifest_digest(reordered),
        )
        with self.factory() as session:
            artifact = _create_artifact(session, "canonical", original)
            before = _row_counts(session)

            record, created = register_artifact_manifest(
                session,
                artifact_id=artifact.id,
                manifest=reordered,
            )

            self.assertTrue(created)
            self.assertEqual(record.digest, artifact.manifest_digest)
            self.assertEqual(record.total_size_bytes, 12)
            self.assertEqual(record.file_count, 3)
            # chunk_count is the number of file-to-chunk links, not unique blobs.
            self.assertEqual(record.chunk_count, 3)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ArtifactChunk)),
                2,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ArtifactFileChunk)),
                3,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ArtifactManifestFile)),
                3,
            )
            result = artifact_manifest_dict(session, record)
            self.assertEqual(
                [item["path"] for item in result["files"]],
                ["empty.txt", "weights/a.bin", "weights/b.bin"],
            )
            self.assertEqual(
                [item["ordinal"] for item in result["files"][1]["chunks"]],
                [0, 1],
            )
            self.assertEqual(result["digest"], artifact.manifest_digest)
            self.assertEqual(result["model_artifact_id"], artifact.id)
            self.assertEqual(result["total_size_bytes"], 12)
            self.assertEqual(result["file_count"], 3)
            self.assertEqual(result["chunk_count"], 3)
            self.assertIsNotNone(result["created_at"])
            self.assertEqual(
                json.loads(record.canonical_json),
                {"schema_version": 1, "files": result["files"]},
            )

            counts_after_first = _row_counts(session)
            repeated, repeated_created = register_artifact_manifest(
                session,
                artifact_id=artifact.id,
                manifest=original,
            )
            self.assertFalse(repeated_created)
            self.assertEqual(repeated.digest, record.digest)
            self.assertEqual(_row_counts(session), counts_after_first)
            self.assertEqual(get_artifact_manifest(session, artifact.id).digest, record.digest)
            self.assertEqual(before[AuditEvent.__tablename__], counts_after_first[AuditEvent.__tablename__])
            self.assertEqual(before[Task.__tablename__], counts_after_first[Task.__tablename__])
            self.assertEqual(
                before[Deployment.__tablename__],
                counts_after_first[Deployment.__tablename__],
            )

    def test_legacy_artifact_remains_explicitly_unregistered(self):
        with self.factory() as session:
            artifact = _create_artifact(session, "legacy")

            self.assertIsNone(get_artifact_manifest(session, artifact.id))
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ArtifactManifest)),
                0,
            )
            with self.assertRaises(ArtifactManifestNotFoundError):
                get_artifact_manifest(session, str(uuid.uuid4()))

    def test_strict_fields_paths_types_and_digests_are_rejected_atomically(self):
        with self.factory() as session:
            artifact = _create_artifact(session, "strict")
            before = _row_counts(session)
            invalid: list[tuple[str, object]] = []

            value = _manifest()
            value["unknown"] = True
            invalid.append(("top-level unknown", value))
            value = _manifest()
            value[1] = "non-string-key"
            invalid.append(("non-string key", value))
            value = _manifest()
            value["files"][0]["unknown"] = True
            invalid.append(("file unknown", value))
            value = _manifest()
            del value["files"][1]["sha256"]
            invalid.append(("file missing", value))
            value = _manifest()
            value["files"][1]["chunks"][0]["unknown"] = True
            invalid.append(("chunk unknown", value))
            value = _manifest()
            value["schema_version"] = True
            invalid.append(("boolean schema version", value))
            value = _manifest()
            value["files"] = tuple(value["files"])
            invalid.append(("non-list files", value))
            value = _manifest()
            value["files"][1]["chunks"] = tuple(value["files"][1]["chunks"])
            invalid.append(("non-list chunks", value))
            value = _manifest()
            value["files"][1]["size_bytes"] = True
            invalid.append(("boolean size", value))
            value = _manifest()
            value["files"][1]["sha256"] = "sha256:" + "A" * 64
            invalid.append(("non-canonical digest", value))

            unsafe_paths = (
                "",
                "/absolute.bin",
                "C:/absolute.bin",
                "../parent.bin",
                "dir/../parent.bin",
                "./dot.bin",
                "dir/./dot.bin",
                "dir//empty.bin",
                "dir/",
                "dir\\windows.bin",
                "control\x01.bin",
                "surrogate\ud800.bin",
            )
            for path in unsafe_paths:
                value = _manifest()
                value["files"][1]["path"] = path
                invalid.append((f"unsafe path {path!r}", value))
            for kind in ("SYMLINK", "SPECIAL", "regular", 1):
                value = _manifest()
                value["files"][1]["kind"] = kind
                invalid.append((f"kind {kind!r}", value))

            duplicate = _manifest()
            duplicate["files"][1]["path"] = "caf\u00e9.bin"
            duplicate["files"][2]["path"] = "cafe\u0301.bin"
            invalid.append(("duplicate normalized unicode path", duplicate))

            for name, manifest in invalid:
                with self.subTest(name=name):
                    with self.assertRaises(ValueError):
                        register_artifact_manifest(
                            session,
                            artifact_id=artifact.id,
                            manifest=manifest,
                        )
                    self.assertEqual(_row_counts(session), before)

    def test_chunk_ranges_and_shared_digest_sizes_are_exact(self):
        with self.factory() as session:
            artifact = _create_artifact(session, "ranges")
            before = _row_counts(session)
            invalid: list[tuple[str, dict]] = []

            value = _manifest()
            value["files"][2]["chunks"][0]["ordinal"] = 2
            invalid.append(("ordinal gap", value))
            value = _manifest()
            value["files"][2]["chunks"][1]["offset_bytes"] = 1
            invalid.append(("first offset gap", value))
            value = _manifest()
            value["files"][2]["chunks"][0]["offset_bytes"] = 3
            invalid.append(("overlap", value))
            value = _manifest()
            value["files"][2]["chunks"][0]["length_bytes"] = 3
            invalid.append(("truncated coverage", value))
            value = _manifest()
            value["files"][2]["size_bytes"] = 9
            invalid.append(("file size mismatch", value))
            value = _manifest()
            value["files"][1]["chunks"] = []
            invalid.append(("non-empty file without chunks", value))
            value = _manifest()
            value["files"][0]["chunks"] = [
                {
                    "ordinal": 0,
                    "offset_bytes": 0,
                    "length_bytes": 1,
                    "sha256": _digest("empty-has-chunk"),
                }
            ]
            invalid.append(("empty file with chunk", value))
            value = _manifest()
            value["files"][1]["chunks"][0]["length_bytes"] = 3
            value["files"][1]["size_bytes"] = 3
            invalid.append(("shared digest length mismatch", value))

            for name, manifest in invalid:
                with self.subTest(name=name):
                    with self.assertRaises(ValueError):
                        register_artifact_manifest(
                            session,
                            artifact_id=artifact.id,
                            manifest=manifest,
                        )
                    self.assertEqual(_row_counts(session), before)

    def test_public_bounds_are_enforced_before_any_registry_write(self):
        cases = (
            ("MAX_ARTIFACT_MANIFEST_FILES", 2),
            ("MAX_ARTIFACT_MANIFEST_CHUNKS", 2),
            ("MAX_ARTIFACT_PATH_LENGTH", 3),
            ("MAX_ARTIFACT_FILE_BYTES", 7),
            ("MAX_ARTIFACT_TOTAL_BYTES", 11),
        )
        with self.factory() as session:
            artifact = _create_artifact(session, "bounds")
            before = _row_counts(session)
            for constant, limit in cases:
                with self.subTest(constant=constant), patch(
                    f"dure.control.service.{constant}", limit
                ):
                    with self.assertRaises(ValueError):
                        register_artifact_manifest(
                            session,
                            artifact_id=artifact.id,
                            manifest=_manifest(),
                        )
                    self.assertEqual(_row_counts(session), before)

    def test_digest_binding_and_stored_chunk_conflicts_leave_all_rows_unchanged(self):
        with self.factory() as session:
            artifact = _create_artifact(session, "digest-binding")
            before = _row_counts(session)
            changed = _manifest()
            changed["files"][1]["path"] = "weights/renamed.bin"
            with self.assertRaises(ArtifactManifestConflictError):
                register_artifact_manifest(
                    session,
                    artifact_id=artifact.id,
                    manifest=changed,
                )
            self.assertEqual(_row_counts(session), before)

            shared_digest = _manifest()["files"][1]["chunks"][0]["sha256"]
            session.add(ArtifactChunk(digest=shared_digest, size_bytes=5))
            session.commit()
            before_chunk_conflict = _row_counts(session)
            with self.assertRaises(ArtifactManifestConflictError):
                register_artifact_manifest(
                    session,
                    artifact_id=artifact.id,
                    manifest=_manifest(),
                )
            self.assertEqual(_row_counts(session), before_chunk_conflict)

    def test_same_digest_with_different_stored_content_is_an_immutable_collision(self):
        manifest = _manifest()
        with self.factory() as session:
            artifact = _create_artifact(session, "collision", manifest)
            session.add(
                ArtifactManifest(
                    digest=artifact.manifest_digest,
                    schema_version=1,
                    model_artifact_id=artifact.id,
                    total_size_bytes=1,
                    file_count=1,
                    chunk_count=1,
                    canonical_json='{"files":[],"schema_version":1}',
                )
            )
            session.commit()
            before = _row_counts(session)

            with self.assertRaises(ArtifactManifestConflictError):
                register_artifact_manifest(
                    session,
                    artifact_id=artifact.id,
                    manifest=manifest,
                )

            self.assertEqual(_row_counts(session), before)
            stored = session.get(ArtifactManifest, artifact.manifest_digest)
            self.assertEqual(stored.canonical_json, '{"files":[],"schema_version":1}')

    def test_lookup_and_reregistration_reject_relational_ordinal_corruption(self):
        with self.factory() as session:
            artifact = _create_artifact(session, "ordinal-corruption")
            record, _ = register_artifact_manifest(
                session,
                artifact_id=artifact.id,
                manifest=_manifest(),
            )
            first_file = session.scalar(
                select(ArtifactManifestFile).where(
                    ArtifactManifestFile.manifest_digest == record.digest,
                    ArtifactManifestFile.ordinal == 0,
                )
            )
            self.assertIsNotNone(first_file)
            first_file.ordinal = 10
            session.commit()
            before = _row_counts(session)

            with self.assertRaises(ArtifactManifestConflictError):
                artifact_manifest_dict(session, record)
            with self.assertRaises(ArtifactManifestConflictError):
                register_artifact_manifest(
                    session,
                    artifact_id=artifact.id,
                    manifest=_manifest(),
                )
            self.assertEqual(_row_counts(session), before)

    def test_commit_false_can_be_rolled_back_without_partial_rows(self):
        with self.factory() as session:
            artifact = _create_artifact(session, "rollback")
            record, created = register_artifact_manifest(
                session,
                artifact_id=artifact.id,
                manifest=_manifest(),
                commit=False,
            )
            self.assertTrue(created)
            self.assertIsNotNone(session.get(ArtifactManifest, record.digest))
            session.rollback()
            artifact_id = artifact.id

        with self.factory() as session:
            self.assertIsNotNone(session.get(ModelArtifact, artifact_id))
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ArtifactManifest)),
                0,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ArtifactManifestFile)),
                0,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ArtifactChunk)),
                0,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ArtifactFileChunk)),
                0,
            )

    def test_concurrent_shared_chunk_insert_is_reused_after_conflict(self):
        manifest = _manifest()
        shared_digest = manifest["files"][1]["chunks"][0]["sha256"]
        with self.factory() as session:
            artifact = _create_artifact(session, "shared-race", manifest)
            session.add(ArtifactChunk(digest=shared_digest, size_bytes=4))
            session.commit()

            original = service_module._artifact_chunks_by_digest
            lookups = 0

            def hide_the_concurrent_winner_once(current_session, digests):
                nonlocal lookups
                lookups += 1
                if lookups == 1:
                    return {}
                return original(current_session, digests)

            with patch(
                "dure.control.service._artifact_chunks_by_digest",
                side_effect=hide_the_concurrent_winner_once,
            ):
                record, created = register_artifact_manifest(
                    session,
                    artifact_id=artifact.id,
                    manifest=manifest,
                )

            self.assertTrue(created)
            self.assertEqual(record.digest, artifact.manifest_digest)
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(ArtifactChunk).where(
                        ArtifactChunk.digest == shared_digest
                    )
                ),
                1,
            )

    def test_late_shared_chunk_size_conflict_rolls_back_new_chunks(self):
        manifest = _manifest()
        shared_digest = manifest["files"][1]["chunks"][0]["sha256"]
        with self.factory() as session:
            artifact = _create_artifact(session, "shared-size-race", manifest)
            session.add(ArtifactChunk(digest=shared_digest, size_bytes=5))
            session.commit()
            before = _row_counts(session)

            original = service_module._artifact_chunks_by_digest
            lookups = 0

            def hide_the_conflicting_winner_once(current_session, digests):
                nonlocal lookups
                lookups += 1
                if lookups == 1:
                    return {}
                return original(current_session, digests)

            with patch(
                "dure.control.service._artifact_chunks_by_digest",
                side_effect=hide_the_conflicting_winner_once,
            ), self.assertRaises(ArtifactManifestConflictError):
                register_artifact_manifest(
                    session,
                    artifact_id=artifact.id,
                    manifest=manifest,
                )

            session.commit()
            self.assertEqual(_row_counts(session), before)

    def test_manifest_readback_uses_join_queries_without_expanding_id_lists(self):
        with self.factory() as session:
            artifact = _create_artifact(session, "bounded-readback")
            record, _ = register_artifact_manifest(
                session,
                artifact_id=artifact.id,
                manifest=_manifest(),
            )
            statements: list[str] = []

            def capture_statement(
                _connection,
                _cursor,
                statement,
                _parameters,
                _context,
                _executemany,
            ):
                statements.append(statement)

            event.listen(
                self.engine,
                "before_cursor_execute",
                capture_statement,
            )
            try:
                value = artifact_manifest_dict(session, record)
            finally:
                event.remove(
                    self.engine,
                    "before_cursor_execute",
                    capture_statement,
                )

            self.assertEqual(value["digest"], record.digest)
            self.assertFalse(
                any(" IN (" in statement.upper() for statement in statements),
                statements,
            )


if __name__ == "__main__":
    unittest.main()
