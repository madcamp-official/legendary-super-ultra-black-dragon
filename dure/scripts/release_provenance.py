#!/usr/bin/env python3
"""Create and verify the signed provenance manifest for a Dure Debian release.

The manifest is deliberately small and dependency-free so a static APT mirror can
verify a signed manifest and its package digest before it publishes any bytes.
The manifest is not a replacement for the GitHub build attestation or APT's
InRelease signature: it connects those two independently verifiable records to
the exact source commit and Debian package.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


class ProvenanceError(ValueError):
    """Raised when release provenance is malformed or does not match an artifact."""


_COMMIT_PATTERN = re.compile(r"[0-9a-f]{40,64}")
_FINGERPRINT_PATTERN = re.compile(r"[0-9A-F]{40}(?:[0-9A-F]{24})?")
_VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9.+:~_-]*")
_ARTIFACT_PATTERN = re.compile(r"dure_[A-Za-z0-9.+:~_-]+_(?:all|amd64)\.deb")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as package:
            for block in iter(lambda: package.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as error:
        raise ProvenanceError(f"cannot read artifact {path}: {error}") from error
    return digest.hexdigest()


def _normalize_fingerprint(value: str) -> str:
    fingerprint = value.replace(" ", "").upper()
    if _FINGERPRINT_PATTERN.fullmatch(fingerprint) is None:
        raise ProvenanceError("signing key fingerprint must contain 40 or 64 hexadecimal characters")
    return fingerprint


def _validate_repository(repository: str) -> str:
    parsed = urlparse(repository)
    if parsed.scheme != "https" or parsed.netloc != "github.com" or not parsed.path.strip("/"):
        raise ProvenanceError("source repository must be an https://github.com/<owner>/<repository> URL")
    if parsed.params or parsed.query or parsed.fragment:
        raise ProvenanceError("source repository URL must not contain parameters, a query, or a fragment")
    return repository.rstrip("/")


def _validate_release_inputs(
    *,
    package: Path,
    version: str,
    tag: str,
    source_repository: str,
    source_commit: str,
    workflow_run_url: str,
    signing_key_fingerprint: str,
    suite: str,
    component: str,
    architecture: str,
) -> tuple[str, str]:
    if not package.is_file():
        raise ProvenanceError(f"release package does not exist: {package}")
    if _ARTIFACT_PATTERN.fullmatch(package.name) is None:
        raise ProvenanceError(f"unexpected Dure Debian package filename: {package.name}")
    if _VERSION_PATTERN.fullmatch(version) is None:
        raise ProvenanceError(f"invalid Debian version: {version}")
    if tag != f"v{version}":
        raise ProvenanceError(f"release tag {tag!r} must exactly match Debian version v{version}")
    repository = _validate_repository(source_repository)
    if _COMMIT_PATTERN.fullmatch(source_commit) is None:
        raise ProvenanceError("source commit must be a 40-64 character lowercase hexadecimal hash")
    if not workflow_run_url.startswith(f"{repository}/actions/runs/"):
        raise ProvenanceError("workflow run URL must belong to the source repository")
    if not suite or not component or architecture != "amd64":
        raise ProvenanceError("only the stable/main amd64 APT repository is supported")
    return repository, _normalize_fingerprint(signing_key_fingerprint)


def create_manifest(
    *,
    package: Path,
    version: str,
    tag: str,
    source_repository: str,
    source_commit: str,
    workflow_run_url: str,
    signing_key_fingerprint: str,
    suite: str = "stable",
    component: str = "main",
    architecture: str = "amd64",
) -> dict[str, Any]:
    """Create the canonical, JSON-serializable provenance document."""

    repository, fingerprint = _validate_release_inputs(
        package=package,
        version=version,
        tag=tag,
        source_repository=source_repository,
        source_commit=source_commit,
        workflow_run_url=workflow_run_url,
        signing_key_fingerprint=signing_key_fingerprint,
        suite=suite,
        component=component,
        architecture=architecture,
    )
    return {
        "schema_version": 1,
        "release": {
            "tag": tag,
            "version": version,
            "source": {"repository": repository, "commit": source_commit},
        },
        "build": {
            "workflow": ".github/workflows/publish-apt.yml",
            "run_url": workflow_run_url,
        },
        "artifact": {
            "name": package.name,
            "sha256": _sha256(package),
            "size": package.stat().st_size,
        },
        "apt": {
            "suite": suite,
            "component": component,
            "architecture": architecture,
            "signing_key_fingerprint": fingerprint,
        },
    }


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    try:
        path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError as error:
        raise ProvenanceError(f"cannot write manifest {path}: {error}") from error


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProvenanceError(f"cannot read manifest {path}: {error}") from error
    if not isinstance(value, dict):
        raise ProvenanceError("release provenance manifest must contain a JSON object")
    return value


def verify_manifest(
    *,
    manifest_path: Path,
    package: Path,
    version: str,
    tag: str,
    source_repository: str,
    source_commit: str,
    workflow_run_url: str,
    signing_key_fingerprint: str,
) -> dict[str, Any]:
    """Verify every manifest claim that connects a package to an official release."""

    expected = create_manifest(
        package=package,
        version=version,
        tag=tag,
        source_repository=source_repository,
        source_commit=source_commit,
        workflow_run_url=workflow_run_url,
        signing_key_fingerprint=signing_key_fingerprint,
    )
    actual = _read_manifest(manifest_path)
    if actual != expected:
        raise ProvenanceError("release provenance manifest does not match the expected release package")
    return actual


def _create_command(arguments: argparse.Namespace) -> int:
    manifest = create_manifest(
        package=arguments.package.resolve(),
        version=arguments.version,
        tag=arguments.tag,
        source_repository=arguments.source_repository,
        source_commit=arguments.source_commit,
        workflow_run_url=arguments.workflow_run_url,
        signing_key_fingerprint=arguments.signing_key_fingerprint,
    )
    write_manifest(arguments.output, manifest)
    print(f"Wrote release provenance: {arguments.output}")
    return 0


def _verify_command(arguments: argparse.Namespace) -> int:
    verify_manifest(
        manifest_path=arguments.manifest,
        package=arguments.package.resolve(),
        version=arguments.version,
        tag=arguments.tag,
        source_repository=arguments.source_repository,
        source_commit=arguments.source_commit,
        workflow_run_url=arguments.workflow_run_url,
        signing_key_fingerprint=arguments.signing_key_fingerprint,
    )
    print(f"Verified release provenance: {arguments.manifest}")
    return 0


def _add_release_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--package", type=Path, required=True, help="built dure .deb package")
    parser.add_argument("--version", required=True, help="Debian package version")
    parser.add_argument("--tag", required=True, help="Git tag, exactly v<version>")
    parser.add_argument("--source-repository", required=True, help="official GitHub repository URL")
    parser.add_argument("--source-commit", required=True, help="immutable source commit hash")
    parser.add_argument("--workflow-run-url", required=True, help="official GitHub Actions run URL")
    parser.add_argument(
        "--signing-key-fingerprint",
        required=True,
        help="official APT archive signing-key fingerprint",
    )


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="create or verify Dure Debian release provenance"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create", help="write canonical release provenance JSON")
    _add_release_arguments(create)
    create.add_argument("--output", type=Path, required=True, help="manifest JSON path")
    verify = commands.add_parser("verify", help="verify manifest claims against a package")
    _add_release_arguments(verify)
    verify.add_argument("--manifest", type=Path, required=True, help="manifest JSON path")
    options = parser.parse_args(arguments)

    try:
        if options.command == "create":
            return _create_command(options)
        return _verify_command(options)
    except ProvenanceError as error:
        print(f"Release provenance failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover - covered through main in subprocess tests.
    raise SystemExit(main())
