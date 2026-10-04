"""Explicit synchronous adapters. Only record mode touches external resources."""

from __future__ import annotations

import random
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .core import Cassette


@dataclass(frozen=True)
class HTTPResponse:
    status: int
    headers: dict[str, str]
    body: bytes


class HTTP:
    """urllib HTTP requests; non-2xx statuses are ordinary HTTPResponse values."""

    def __init__(self, cassette: Cassette, *, name: str = "http") -> None:
        self.cassette = cassette
        self.name = name

    def request(
        self,
        url: str,
        *,
        method: str = "GET",
        body: bytes | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float = 10.0,
    ) -> HTTPResponse:
        normalized = {k.lower(): v for k, v in (headers or {}).items()}

        def send() -> tuple[int, dict[str, str], bytes]:
            request = urllib.request.Request(
                url, data=body, headers=normalized, method=method
            )
            try:
                response = urllib.request.urlopen(request, timeout=timeout)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                return (
                    response.code,
                    {k.lower(): v for k, v in response.headers.items()},
                    response.read(),
                )

        status, result_headers, data = self.cassette.call(
            self.name + ".request", (url, method, body, normalized, timeout), send
        )
        return HTTPResponse(status, result_headers, data)


class Cursor(Protocol):
    rowcount: int
    description: Any

    def execute(self, operation: str, parameters: Any) -> Any: ...
    def fetchall(self) -> Any: ...
    def close(self) -> None: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


@dataclass(frozen=True)
class DBResult:
    rows: list[tuple[Any, ...]]
    rowcount: int
    columns: tuple[str, ...]


class Database:
    """DB-API execute/fetchall and transaction boundaries; caller owns connection.

    Driver-specific row objects are normalized to tuples. Values must fit the
    cassette codec (convert Decimal/datetime in SQL or use a custom operation).
    """

    def __init__(
        self,
        cassette: Cassette,
        connection: Connection | None = None,
        *,
        name: str = "database",
    ) -> None:
        self.cassette = cassette
        self.connection = connection
        self.name = name

    def _connection(self) -> Connection:
        if self.connection is None:
            raise ValueError("Recording requires a DB-API connection")
        return self.connection

    def execute(
        self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ()
    ) -> DBResult:
        def run() -> tuple[list[tuple[Any, ...]], int, tuple[str, ...]]:
            cursor = self._connection().cursor()
            try:
                cursor.execute(sql, parameters)
                columns = tuple(column[0] for column in cursor.description or ())
                rows = [tuple(row) for row in cursor.fetchall()] if columns else []
                return rows, cursor.rowcount, columns
            finally:
                cursor.close()

        rows, count, columns = self.cassette.call(
            self.name + ".execute", (sql, parameters), run
        )
        return DBResult(rows, count, columns)

    def commit(self) -> None:
        self.cassette.call(
            self.name + ".commit", (), lambda: self._connection().commit()
        )

    def rollback(self) -> None:
        self.cassette.call(
            self.name + ".rollback", (), lambda: self._connection().rollback()
        )


class RedisClient(Protocol):
    def execute_command(self, *args: Any, **options: Any) -> Any: ...


class Redis:
    """Wrap synchronous redis-py execute_command; replay needs no client."""

    def __init__(
        self,
        cassette: Cassette,
        client: RedisClient | None = None,
        *,
        name: str = "redis",
    ) -> None:
        self.cassette = cassette
        self.client = client
        self.name = name

    def command(self, command: str, *arguments: str | int | float | bytes) -> Any:
        def run() -> Any:
            if self.client is None:
                raise ValueError("Recording requires a Redis client")
            return self.client.execute_command(command, *arguments)

        return self.cassette.call(
            self.name + ".command", (command.upper(), arguments), run
        )


class Filesystem:
    """Byte reads, writes, and unlinks. Replayed writes/unlinks have no disk effects."""

    def __init__(self, cassette: Cassette, *, name: str = "filesystem") -> None:
        self.cassette = cassette
        self.name = name

    def read(self, path: str | Path) -> bytes:
        return self.cassette.call(
            self.name + ".read", str(path), lambda: Path(path).read_bytes()
        )

    def write(self, path: str | Path, data: bytes) -> int:
        return self.cassette.call(
            self.name + ".write",
            (str(path), data),
            lambda: Path(path).write_bytes(data),
        )

    def unlink(self, path: str | Path) -> None:
        self.cassette.call(
            self.name + ".unlink", str(path), lambda: Path(path).unlink()
        )


@dataclass(frozen=True)
class ProcessResult:
    returncode: int
    stdout: bytes
    stderr: bytes


class Processes:
    """Shell-free subprocess execution, capturing output bytes and exit code."""

    def __init__(self, cassette: Cassette, *, name: str = "process") -> None:
        self.cassette = cassette
        self.name = name

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        input: bytes | None = None,
        timeout: float = 10.0,
    ) -> ProcessResult:
        def run() -> tuple[int, bytes, bytes]:
            result = subprocess.run(
                list(args),
                cwd=cwd,
                env=env,
                input=input,
                timeout=timeout,
                capture_output=True,
                check=False,
            )
            return result.returncode, result.stdout, result.stderr

        code, out, err = self.cassette.call(
            self.name + ".run",
            (list(args), cwd, dict(env) if env is not None else None, input, timeout),
            run,
        )
        return ProcessResult(code, out, err)


class Clock:
    """Wall and monotonic time plus sleep; replay sleep returns immediately."""

    def __init__(self, cassette: Cassette, *, name: str = "clock") -> None:
        self.cassette = cassette
        self.name = name

    def time(self) -> float:
        return self.cassette.call(self.name + ".time", (), time.time)

    def monotonic(self) -> float:
        return self.cassette.call(self.name + ".monotonic", (), time.monotonic)

    def sleep(self, seconds: float) -> None:
        self.cassette.call(self.name + ".sleep", seconds, lambda: time.sleep(seconds))


class Randomness:
    """An owned random generator; does not change the process-wide random state."""

    def __init__(
        self,
        cassette: Cassette,
        generator: random.Random | None = None,
        *,
        name: str = "random",
    ) -> None:
        self.cassette = cassette
        self.generator = generator if generator is not None else random.Random()
        self.name = name

    def random(self) -> float:
        return self.cassette.call(self.name + ".random", (), self.generator.random)

    def randint(self, a: int, b: int) -> int:
        return self.cassette.call(
            self.name + ".randint", (a, b), lambda: self.generator.randint(a, b)
        )

    def uniform(self, a: float, b: float) -> float:
        return self.cassette.call(
            self.name + ".uniform", (a, b), lambda: self.generator.uniform(a, b)
        )
