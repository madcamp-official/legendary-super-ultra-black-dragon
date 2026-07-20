from __future__ import annotations

import shutil
import subprocess
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

