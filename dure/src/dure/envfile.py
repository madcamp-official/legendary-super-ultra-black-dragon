from __future__ import annotations

import os
import stat
from pathlib import Path


ENV_FILE_MAX_BYTES = 64 * 1024


def parse_secure_env_file(
    path: Path,
    *,
    keys: frozenset[str],
    required: bool,
    required_description: str,
) -> dict[str, str]:
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | os.O_CLOEXEC
            | os.O_NONBLOCK
            | getattr(os, "O_NOFOLLOW", 0),
        )
    except FileNotFoundError:
        if required:
            raise ValueError(f"env file does not exist: {path}") from None
        return {}
    except OSError as exc:
        raise ValueError(f"env file is not a safe readable file: {path}: {exc}") from exc

    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"env file must be a regular file: {path}")
        content = os.read(descriptor, ENV_FILE_MAX_BYTES + 1)
    except ValueError:
        raise
    except OSError as exc:
        raise ValueError(f"could not read env file: {path}: {exc}") from exc
    finally:
        os.close(descriptor)

    if len(content) > ENV_FILE_MAX_BYTES:
        raise ValueError(f"env file exceeds {ENV_FILE_MAX_BYTES} bytes: {path}")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"env file is not valid UTF-8: {path}") from exc

    values: dict[str, str] = {}
    for line_number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[7:].lstrip()
        if "=" not in stripped:
            if stripped.startswith("DURE_"):
                raise ValueError(f"invalid Dure setting in {path}:{line_number}")
            continue
        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        if key not in keys:
            continue
        if key in values:
            raise ValueError(f"duplicate {key} in env file: {path}")
        raw_value = raw_value.strip()
        if raw_value[:1] in {"'", '"'}:
            quote = raw_value[0]
            if len(raw_value) < 2 or raw_value[-1] != quote:
                raise ValueError(f"unterminated quoted {key} in {path}:{line_number}")
            raw_value = raw_value[1:-1]
        if not raw_value:
            raise ValueError(f"{key} must not be empty in env file: {path}")
        values[key] = raw_value

    if not values and not required:
        return {}
    missing = sorted(keys - values.keys())
    if missing:
        raise ValueError(f"env file must define {required_description} together: {path}")
    if metadata.st_uid != os.geteuid():
        raise ValueError(f"env file must be owned by the current user: {path}")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise ValueError(f"env file must not be accessible by group or others: {path}")
    return values
