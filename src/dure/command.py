from __future__ import annotations

import os
import selectors
import shutil
import subprocess
import time
from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence


@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str = ""
    stderr: str = ""

    @property
    def ok(self) -> bool:
        return self.returncode == 0


class Runner(Protocol):
    def exists(self, executable: str) -> bool: ...

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout: float = 15,
        env: Mapping[str, str] | None = None,
    ) -> CommandResult: ...


class SubprocessRunner:
    def exists(self, executable: str) -> bool:
        return shutil.which(executable) is not None

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout: float = 15,
        env: Mapping[str, str] | None = None,
    ) -> CommandResult:
        command = tuple(str(part) for part in argv)
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=dict(env) if env is not None else None,
            )
            return CommandResult(
                argv=command,
                returncode=completed.returncode,
                stdout=completed.stdout.strip(),
                stderr=completed.stderr.strip(),
            )
        except FileNotFoundError as exc:
            return CommandResult(command, 127, stderr=str(exc))
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else exc.stdout or ""
            stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else exc.stderr or ""
            return CommandResult(command, 124, stdout.strip(), stderr.strip() or "command timed out")

    def run_limited_output(
        self,
        argv: Sequence[str],
        *,
        timeout: float = 15,
        max_output_bytes: int,
        env: Mapping[str, str] | None = None,
    ) -> CommandResult:
        """Run a command while bounding combined stdout/stderr during execution.

        This is intentionally separate from ``run`` so existing callers keep their
        current behavior.  Security-sensitive commands can opt in without first
        materializing untrusted, arbitrarily large output in memory.
        """
        if type(max_output_bytes) is not int or max_output_bytes <= 0:
            raise ValueError("max_output_bytes must be a positive integer")

        command = tuple(str(part) for part in argv)
        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=dict(env) if env is not None else None,
            )
        except FileNotFoundError as exc:
            return CommandResult(command, 127, stderr=str(exc))

        selector = selectors.DefaultSelector()
        streams = {
            "stdout": process.stdout,
            "stderr": process.stderr,
        }
        for name, stream in streams.items():
            if stream is None:  # pragma: no cover - guaranteed by PIPE above
                continue
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)

        captured = {"stdout": bytearray(), "stderr": bytearray()}
        captured_bytes = 0
        deadline = time.monotonic() + timeout
        termination_reason: str | None = None

        try:
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    termination_reason = "timeout"
                    break
                for key, _ in selector.select(timeout=min(remaining, 0.1)):
                    try:
                        chunk = os.read(key.fileobj.fileno(), 64 * 1024)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                        continue
                    if captured_bytes + len(chunk) > max_output_bytes:
                        termination_reason = "output_limit"
                        break
                    captured[key.data].extend(chunk)
                    captured_bytes += len(chunk)
                if termination_reason is not None:
                    break

            if termination_reason is None:
                remaining = deadline - time.monotonic()
                try:
                    process.wait(timeout=max(0, remaining))
                except subprocess.TimeoutExpired:
                    termination_reason = "timeout"

            if termination_reason is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        finally:
            selector.close()
            for stream in streams.values():
                if stream is not None and not stream.closed:
                    stream.close()

        if termination_reason == "output_limit":
            return CommandResult(
                command, 125, stderr="command output limit exceeded"
            )
        if termination_reason == "timeout":
            return CommandResult(command, 124, stderr="command timed out")

        def decoded(value: bytearray) -> str:
            return bytes(value).decode("utf-8", errors="replace").strip()

        return CommandResult(
            command,
            process.returncode,
            stdout=decoded(captured["stdout"]),
            stderr=decoded(captured["stderr"]),
        )
