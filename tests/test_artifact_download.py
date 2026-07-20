from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import threading
import traceback
import unittest
from pathlib import Path
from typing import Mapping

from dure.artifact_download import (
    ArtifactChunkDownloader,
    TrustedHTTPSOrigin,
)
from dure.model_store import ContentAddressedModelStore, ModelStoreError


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


class FakeResponse:
    def __init__(
        self,
        *,
        status: int,
        headers: Mapping[str, tuple[str, ...] | str],
        body: bytes = b"",
        scripted_reads: list[bytes | BaseException] | None = None,
        header_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.status = status
        self._headers = {
            name.lower(): (value,) if isinstance(value, str) else tuple(value)
            for name, value in headers.items()
        }
        self._body = io.BytesIO(body)
        self._scripted_reads = list(scripted_reads or [])
        self._header_error = header_error
        self._close_error = close_error
        self.closed = False

    def header_values(self, name: str) -> tuple[str, ...]:
        if self._header_error is not None:
            raise self._header_error
        return self._headers.get(name.lower(), ())

    def read(self, size: int) -> bytes:
        if self._scripted_reads:
            value = self._scripted_reads.pop(0)
            if isinstance(value, BaseException):
                raise value
            return value
        return self._body.read(size)

    def close(self) -> None:
        self.closed = True
        if self._close_error is not None:
            raise self._close_error


class ScriptedTransport:
    def __init__(self, actions: list[FakeResponse | BaseException]) -> None:
        self.actions = list(actions)
        self.calls: list[dict[str, object]] = []

    def open(
        self,
        origin: TrustedHTTPSOrigin,
        object_url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> FakeResponse:
        self.calls.append(
            {
                "origin": origin,
                "object_url": object_url,
                "headers": dict(headers),
                "timeout_seconds": timeout_seconds,
            }
        )
        if not self.actions:
            raise AssertionError("unexpected extra transport request")
        action = self.actions.pop(0)
        if isinstance(action, BaseException):
            raise action
        return action


class NeverOpenTransport:
    def __init__(self) -> None:
        self.calls = 0

    def open(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError("transport must not be opened")


class ArtifactDownloadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.origin = TrustedHTTPSOrigin(
            "https://artifacts.example.test/v1",
            allowed_redirect_hosts=("cdn.example.test",),
        )
        self.manifest_digest = _digest(b"manifest")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _store(self, name: str) -> ContentAddressedModelStore:
        return ContentAddressedModelStore(
            store_root=self.root / name / "store",
            model_root=self.root / name / "models",
        )

    @staticmethod
    def _response(
        payload: bytes,
        *,
        status: int = 200,
        headers: Mapping[str, tuple[str, ...] | str] | None = None,
        body: bytes | None = None,
        **kwargs,
    ) -> FakeResponse:
        values: dict[str, tuple[str, ...] | str] = {
            "Content-Length": str(len(payload))
        }
        values.update(headers or {})
        return FakeResponse(
            status=status,
            headers=values,
            body=payload if body is None else body,
            **kwargs,
        )

    @staticmethod
    def _write_descriptor(descriptor: int, payload: bytes) -> None:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise AssertionError("test fixture could not write payload")
            view = view[written:]
        os.fsync(descriptor)

    def _seed_partial(
        self,
        store: ContentAddressedModelStore,
        digest: str,
        complete_payload: bytes,
        partial_payload: bytes,
    ) -> Path:
        with store.chunk_lock(digest):
            path, descriptor, offset = store.open_chunk_partial(
                digest, len(complete_payload)
            )
            self.assertEqual(offset, 0)
            try:
                self._write_descriptor(descriptor, partial_payload)
            finally:
                os.close(descriptor)
        return path

    def _seed_exact_chunk(
        self,
        store: ContentAddressedModelStore,
        payload: bytes,
    ) -> tuple[str, Path]:
        digest = _digest(payload)
        with store.chunk_lock(digest):
            _, descriptor, offset = store.open_chunk_partial(
                digest, len(payload)
            )
            self.assertEqual(offset, 0)
            try:
                self._write_descriptor(descriptor, payload)
            finally:
                os.close(descriptor)
            path = store.publish_chunk_partial(digest, len(payload))
        return digest, path

    def _download(
        self,
        store: ContentAddressedModelStore,
        transport,
        payload: bytes,
        *,
        chunk_digest: str | None = None,
        attempts: int = 1,
        manifest_digest: str | None = None,
    ) -> str:
        return ArtifactChunkDownloader(
            store,
            transport=transport,
            attempts=attempts,
            timeout_seconds=5,
            max_chunk_bytes=1024,
        ).download_chunk(
            origin=self.origin,
            manifest_digest=manifest_digest or self.manifest_digest,
            chunk_digest=chunk_digest or _digest(payload),
            expected_size=len(payload),
        )

    def assert_failure_journal(
        self,
        store: ContentAddressedModelStore,
        code: str,
        *,
        manifest_digest: str | None = None,
    ) -> None:
        journal = store.read_attempt(manifest_digest or self.manifest_digest)
        self.assertIsNotNone(journal)
        self.assertEqual(journal.status, "FAILED")
        self.assertEqual(journal.failure_code, code)

    def test_trusted_origin_rejects_non_https_userinfo_query_and_traversal(self):
        invalid = (
            "http://artifacts.example.test/v1",
            "https://user:password@artifacts.example.test/v1",
            "https://artifacts.example.test/v1?token=secret",
            "https://artifacts.example.test/v1/../private",
            "https://artifacts.example.test/v1/%2e%2e/private",
        )

        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                TrustedHTTPSOrigin(value)

    def test_redirect_rejects_untrusted_host_port_and_query(self):
        current = self.origin.object_url(_digest(b"redirect"))
        rejected = (
            "https://evil.example.test/chunks/sha256/abcd",
            "https://cdn.example.test:8443/chunks/sha256/abcd",
            "https://cdn.example.test/chunks/sha256/abcd?token=secret",
        )

        for location in rejected:
            with self.subTest(location=location) as context:
                with self.assertRaises(ModelStoreError) as raised:
                    self.origin.validated_redirect(current, location)
                self.assertEqual(
                    raised.exception.code, "MODEL_STORE_DOWNLOAD_REJECTED"
                )

    def test_exact_200_and_exact_206_resume_are_accepted(self):
        payload = b"abcdef"
        full_store = self._store("exact-200")
        full_transport = ScriptedTransport([self._response(payload)])

        full_path = Path(self._download(full_store, full_transport, payload))

        self.assertEqual(full_path.read_bytes(), payload)
        self.assertEqual(len(full_transport.calls), 1)
        self.assertNotIn("Range", full_transport.calls[0]["headers"])

        resumed_store = self._store("exact-206")
        chunk_digest = _digest(payload)
        partial = self._seed_partial(
            resumed_store, chunk_digest, payload, payload[:3]
        )
        resumed_transport = ScriptedTransport(
            [
                self._response(
                    payload[3:],
                    status=206,
                    headers={
                        "Content-Range": "bytes 3-5/6",
                        "Content-Length": "3",
                    },
                )
            ]
        )

        resumed_path = Path(
            self._download(resumed_store, resumed_transport, payload)
        )

        self.assertEqual(resumed_path.read_bytes(), payload)
        self.assertFalse(partial.exists())
        self.assertEqual(
            resumed_transport.calls[0]["headers"]["Range"], "bytes=3-"
        )

    def test_missing_and_duplicate_content_length_are_rejected(self):
        payload = b"content-length"
        cases = (
            ("missing", {}),
            ("duplicate", {"Content-Length": (str(len(payload)),) * 2}),
        )

        for name, headers in cases:
            with self.subTest(name=name):
                store = self._store(f"content-length-{name}")
                response = FakeResponse(
                    status=200,
                    headers=headers,
                    body=payload,
                )
                with self.assertRaises(ModelStoreError) as raised:
                    self._download(
                        store, ScriptedTransport([response]), payload
                    )
                self.assertEqual(
                    raised.exception.code, "MODEL_STORE_DOWNLOAD_REJECTED"
                )
                self.assert_failure_journal(
                    store, "MODEL_STORE_DOWNLOAD_REJECTED"
                )

    def test_wrong_content_range_is_rejected_and_partial_is_reset(self):
        payload = b"abcdef"
        wrong_ranges = (
            "bytes 2-5/6",
            "bytes 3-4/6",
            "bytes 3-5/7",
            "items 3-5/6",
        )

        for index, content_range in enumerate(wrong_ranges):
            with self.subTest(content_range=content_range):
                store = self._store(f"wrong-range-{index}")
                chunk_digest = _digest(payload)
                partial = self._seed_partial(
                    store, chunk_digest, payload, payload[:3]
                )
                response = self._response(
                    payload[3:],
                    status=206,
                    headers={
                        "Content-Range": content_range,
                        "Content-Length": "3",
                    },
                )

                with self.assertRaises(ModelStoreError) as raised:
                    self._download(
                        store, ScriptedTransport([response]), payload
                    )

                self.assertEqual(
                    raised.exception.code, "MODEL_STORE_DOWNLOAD_REJECTED"
                )
                self.assertEqual(partial.read_bytes(), b"")
                self.assert_failure_journal(
                    store, "MODEL_STORE_DOWNLOAD_REJECTED"
                )

    def test_transfer_and_content_encoding_are_rejected(self):
        payload = b"encoded"
        cases = (
            ("transfer", {"Transfer-Encoding": "chunked"}),
            ("content", {"Content-Encoding": "gzip"}),
        )

        for name, headers in cases:
            with self.subTest(name=name):
                store = self._store(f"encoding-{name}")
                with self.assertRaises(ModelStoreError) as raised:
                    self._download(
                        store,
                        ScriptedTransport(
                            [self._response(payload, headers=headers)]
                        ),
                        payload,
                    )
                self.assertEqual(
                    raised.exception.code, "MODEL_STORE_DOWNLOAD_REJECTED"
                )
                self.assert_failure_journal(
                    store, "MODEL_STORE_DOWNLOAD_REJECTED"
                )

    def test_truncated_and_overrun_bodies_fail_closed(self):
        payload = b"abcdef"
        truncated_store = self._store("truncated")
        truncated_response = self._response(payload, body=payload[:3])

        with self.assertRaises(ModelStoreError) as truncated:
            self._download(
                truncated_store,
                ScriptedTransport([truncated_response]),
                payload,
            )

        self.assertEqual(
            truncated.exception.code, "MODEL_STORE_DOWNLOAD_INTERRUPTED"
        )
        self.assertEqual(
            truncated_store.chunk_partial_path(_digest(payload)).read_bytes(),
            payload[:3],
        )

        overrun_store = self._store("overrun")
        overrun_response = self._response(
            payload,
            scripted_reads=[payload + b"x"],
        )
        with self.assertRaises(ModelStoreError) as overrun:
            self._download(
                overrun_store,
                ScriptedTransport([overrun_response]),
                payload,
            )

        self.assertEqual(
            overrun.exception.code, "MODEL_STORE_DOWNLOAD_REJECTED"
        )
        self.assertEqual(
            overrun_store.chunk_partial_path(_digest(payload)).read_bytes(), b""
        )

    def test_interruption_resumes_the_same_partial_from_range_offset(self):
        payload = b"abcdef"
        store = self._store("interrupted-resume")
        first = self._response(payload, body=payload[:3])
        second = self._response(
            payload[3:],
            status=206,
            headers={
                "Content-Range": "bytes 3-5/6",
                "Content-Length": "3",
            },
        )
        transport = ScriptedTransport([first, second])

        result = Path(
            self._download(store, transport, payload, attempts=2)
        )

        self.assertEqual(result.read_bytes(), payload)
        self.assertEqual(len(transport.calls), 2)
        self.assertNotIn("Range", transport.calls[0]["headers"])
        self.assertEqual(
            transport.calls[1]["headers"]["Range"], "bytes=3-"
        )
        journal = store.read_attempt(self.manifest_digest)
        self.assertIsNotNone(journal)
        self.assertEqual(journal.status, "SUCCEEDED")
        self.assertEqual(journal.bytes_complete, len(payload))

    def test_retry_count_is_bounded(self):
        payload = b"retry-bound"
        store = self._store("bounded-retry")
        transport = ScriptedTransport(
            [self._response(payload, body=b"") for _ in range(3)]
        )

        with self.assertRaises(ModelStoreError) as raised:
            self._download(store, transport, payload, attempts=3)

        self.assertEqual(
            raised.exception.code, "MODEL_STORE_DOWNLOAD_INTERRUPTED"
        )
        self.assertEqual(len(transport.calls), 3)
        self.assert_failure_journal(
            store, "MODEL_STORE_DOWNLOAD_INTERRUPTED"
        )

    def test_full_digest_mismatch_retries_then_preserves_empty_partial(self):
        payload = b"received-content"
        claimed_digest = _digest(b"different-content")
        store = self._store("digest-mismatch")
        transport = ScriptedTransport(
            [self._response(payload), self._response(payload)]
        )

        with self.assertRaises(ModelStoreError) as raised:
            self._download(
                store,
                transport,
                payload,
                chunk_digest=claimed_digest,
                attempts=2,
            )

        self.assertEqual(
            raised.exception.code, "MODEL_STORE_DIGEST_MISMATCH"
        )
        self.assertEqual(len(transport.calls), 2)
        self.assertFalse(store.chunk_path(claimed_digest).exists())
        self.assertEqual(
            store.chunk_partial_path(claimed_digest).read_bytes(), b""
        )
        self.assert_failure_journal(store, "MODEL_STORE_DIGEST_MISMATCH")

    def test_remote_failures_do_not_expose_url_token_cause_or_journal(self):
        payload = b"secret-safety"
        secret = "https://remote.example/object?token=TOP-SECRET-TOKEN"
        cases = (
            (
                "open",
                ScriptedTransport([RuntimeError(secret)]),
                "MODEL_STORE_DOWNLOAD_REJECTED",
            ),
            (
                "header",
                ScriptedTransport(
                    [
                        self._response(
                            payload,
                            header_error=RuntimeError(secret),
                        )
                    ]
                ),
                "MODEL_STORE_DOWNLOAD_REJECTED",
            ),
            (
                "read",
                ScriptedTransport(
                    [
                        self._response(
                            payload,
                            scripted_reads=[RuntimeError(secret)],
                        )
                    ]
                ),
                "MODEL_STORE_DOWNLOAD_INTERRUPTED",
            ),
            (
                "close",
                ScriptedTransport(
                    [
                        self._response(
                            payload,
                            close_error=RuntimeError(secret),
                        )
                    ]
                ),
                "MODEL_STORE_DOWNLOAD_INTERRUPTED",
            ),
        )

        for name, transport, expected_code in cases:
            with self.subTest(name=name):
                store = self._store(f"secret-{name}")
                try:
                    self._download(store, transport, payload)
                except ModelStoreError as error:
                    rendered = "".join(
                        traceback.format_exception(
                            type(error), error, error.__traceback__
                        )
                    )
                    self.assertEqual(error.code, expected_code)
                    self.assertIsNone(error.__cause__)
                    self.assertNotIn(secret, str(error))
                    self.assertNotIn(secret, repr(error))
                    self.assertNotIn(secret, rendered)
                else:
                    self.fail("remote failure unexpectedly succeeded")

                journal = store.read_attempt(self.manifest_digest)
                self.assertIsNotNone(journal)
                encoded = json.dumps(journal.to_dict(), sort_keys=True)
                self.assertNotIn("TOP-SECRET-TOKEN", encoded)
                self.assertNotIn("remote.example", encoded)
                self.assertEqual(journal.failure_code, expected_code)

    def test_close_failure_does_not_replace_an_earlier_rejection(self):
        payload = b"rejected-before-close"
        secret = "https://remote.example/object?token=TOP-SECRET-TOKEN"
        store = self._store("rejected-close")
        response = self._response(
            payload,
            status=500,
            close_error=RuntimeError(secret),
        )

        try:
            self._download(store, ScriptedTransport([response]), payload)
        except ModelStoreError as error:
            rendered = "".join(
                traceback.format_exception(
                    type(error), error, error.__traceback__
                )
            )
            self.assertEqual(error.code, "MODEL_STORE_DOWNLOAD_REJECTED")
            self.assertIsNone(error.__cause__)
            self.assertNotIn(secret, rendered)
        else:
            self.fail("invalid response unexpectedly succeeded")

        self.assertTrue(response.closed)
        self.assert_failure_journal(
            store,
            "MODEL_STORE_DOWNLOAD_REJECTED",
        )

    def test_existing_exact_chunk_returns_without_network(self):
        payload = b"already-present"
        store = self._store("existing")
        digest, expected_path = self._seed_exact_chunk(store, payload)
        transport = NeverOpenTransport()

        result = self._download(
            store, transport, payload, chunk_digest=digest
        )

        self.assertEqual(Path(result), expected_path)
        self.assertEqual(expected_path.read_bytes(), payload)
        self.assertEqual(transport.calls, 0)
        journal = store.read_attempt(self.manifest_digest)
        self.assertIsNotNone(journal)
        self.assertEqual(journal.status, "SUCCEEDED")

    def test_corrupt_cas_is_preserved_and_blocks_network(self):
        expected_payload = b"expected"
        corrupt_payload = b"corrupt!"
        digest = _digest(expected_payload)
        store = self._store("corrupt-cas")
        store.ensure_chunk_directory(digest)
        final = store.chunk_path(digest)
        final.write_bytes(corrupt_payload)
        final.chmod(0o600)
        transport = NeverOpenTransport()

        with self.assertRaises(ModelStoreError) as raised:
            self._download(
                store,
                transport,
                expected_payload,
                chunk_digest=digest,
            )

        self.assertEqual(raised.exception.code, "MODEL_STORE_CHUNK_CORRUPT")
        self.assertEqual(final.read_bytes(), corrupt_payload)
        self.assertEqual(transport.calls, 0)
        self.assert_failure_journal(store, "MODEL_STORE_CHUNK_CORRUPT")

    def test_partial_symlink_is_preserved_and_failure_is_journaled(self):
        payload = b"symlink"
        digest = _digest(payload)
        store = self._store("partial-symlink")
        store.ensure_chunk_directory(digest)
        target = self.root / "symlink-target"
        target.write_bytes(b"operator-evidence")
        partial = store.chunk_partial_path(digest)
        partial.symlink_to(target)
        transport = NeverOpenTransport()

        with self.assertRaises(ModelStoreError) as raised:
            self._download(
                store, transport, payload, chunk_digest=digest
            )

        self.assertEqual(
            raised.exception.code, "MODEL_STORE_CHUNK_COLLISION"
        )
        self.assertTrue(partial.is_symlink())
        self.assertEqual(target.read_bytes(), b"operator-evidence")
        self.assertEqual(transport.calls, 0)
        self.assert_failure_journal(store, "MODEL_STORE_CHUNK_COLLISION")

    def test_shared_chunk_contention_downloads_once(self):
        payload = b"shared-contention"
        digest = _digest(payload)
        store = self._store("shared-contention")
        first_opened = threading.Event()
        release_first = threading.Event()

        class BlockingTransport:
            def __init__(self) -> None:
                self.calls = 0
                self.lock = threading.Lock()

            def open(
                inner_self,
                origin: TrustedHTTPSOrigin,
                object_url: str,
                *,
                headers: Mapping[str, str],
                timeout_seconds: float,
            ) -> FakeResponse:
                with inner_self.lock:
                    inner_self.calls += 1
                    call_number = inner_self.calls
                if call_number == 1:
                    first_opened.set()
                    if not release_first.wait(timeout=5):
                        raise AssertionError("contention fixture timed out")
                return self._response(payload)

        transport = BlockingTransport()
        downloader = ArtifactChunkDownloader(
            store,
            transport=transport,
            attempts=1,
            timeout_seconds=5,
            max_chunk_bytes=1024,
        )
        results: list[str] = []
        failures: list[BaseException] = []
        second_started = threading.Event()

        def worker(manifest_digest: str, *, second: bool = False) -> None:
            if second:
                second_started.set()
            try:
                results.append(
                    downloader.download_chunk(
                        origin=self.origin,
                        manifest_digest=manifest_digest,
                        chunk_digest=digest,
                        expected_size=len(payload),
                    )
                )
            except BaseException as exc:  # captured and asserted in main thread
                failures.append(exc)

        first = threading.Thread(
            target=worker, args=(_digest(b"manifest-one"),)
        )
        second = threading.Thread(
            target=worker,
            args=(_digest(b"manifest-two"),),
            kwargs={"second": True},
        )
        first.start()
        self.assertTrue(first_opened.wait(timeout=5))
        second.start()
        self.assertTrue(second_started.wait(timeout=5))
        release_first.set()
        first.join(timeout=5)
        second.join(timeout=5)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], results[1])
        self.assertEqual(Path(results[0]).read_bytes(), payload)
        self.assertEqual(transport.calls, 1)


if __name__ == "__main__":
    unittest.main()
