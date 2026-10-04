"""Strict, safe JSON cassettes. No pickle, imports, or code execution on replay."""

from __future__ import annotations

import base64
import json
import math
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, Self, TypeVar

T = TypeVar("T")
Redactor = Callable[[Any], Any]


class CassetteError(Exception):
    """A malformed cassette, mismatched call, or unconsumed interaction."""


class RecordedError(Exception):
    """An external failure, normalized identically in record and replay modes."""

    def __init__(self, kind: str, message: str) -> None:
        self.kind = kind
        self.message = message
        super().__init__(f"{kind}: {message}")


def redact_secrets(value: Any) -> Any:
    """Recursively replace common credential dictionary values before persistence."""
    keys = {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "password",
        "token",
        "api_key",
        "x-api-key",
        "secret",
    }
    if isinstance(value, dict):
        return {
            k: "<redacted>" if str(k).lower() in keys else redact_secrets(v)
            for k, v in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact_secrets(v) for v in value)
    if isinstance(value, list):
        return [redact_secrets(v) for v in value]
    return value


def _encode(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, bytes):
        return {"$bytes": base64.b64encode(value).decode("ascii")}
    if isinstance(value, tuple):
        return {"$tuple": [_encode(v) for v in value]}
    if isinstance(value, list):
        return [_encode(v) for v in value]
    if isinstance(value, dict) and all(isinstance(k, str) for k in value):
        return {"$dict": {k: _encode(v) for k, v in value.items()}}
    raise CassetteError(f"Unsupported value type: {type(value).__name__}")


def _decode(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, list):
        return [_decode(v) for v in value]
    if isinstance(value, dict) and len(value) == 1:
        if "$bytes" in value and isinstance(value["$bytes"], str):
            try:
                return base64.b64decode(value["$bytes"], validate=True)
            except ValueError as e:
                raise CassetteError("Invalid bytes encoding") from e
        if "$tuple" in value and isinstance(value["$tuple"], list):
            return tuple(_decode(v) for v in value["$tuple"])
        if "$dict" in value and isinstance(value["$dict"], dict):
            return {k: _decode(v) for k, v in value["$dict"].items()}
    raise CassetteError("Invalid encoded value")


def _canonical(value: Any) -> str:
    # JSON spellings preserve bool/int/float distinctions; Python equality does not.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class Cassette:
    """Sequential recorder/replayer. One instance belongs to one execution thread.

    Record overwrites only on successful context exit. Replay requires every
    recorded call to be consumed. Redaction affects matching and returned values
    in both modes, so a recording never returns data absent from its cassette.
    """

    def __init__(
        self,
        path: str | Path,
        mode: Literal["record", "replay"],
        *,
        redactor: Redactor = redact_secrets,
    ) -> None:
        if mode not in ("record", "replay"):
            raise ValueError("mode must be record or replay")
        self.path = Path(path)
        self.mode = mode
        self.redactor = redactor
        self._events: list[dict[str, Any]] = []
        self._position = 0
        self._closed = False
        if mode == "replay":
            self._load()

    def _load(self) -> None:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            raise CassetteError("Cannot read cassette") from e
        if not isinstance(data, dict) or set(data) != {"version", "events"}:
            raise CassetteError("Invalid cassette schema")
        if type(data["version"]) is not int or data["version"] != 1:
            raise CassetteError("Unsupported cassette version")
        if not isinstance(data["events"], list):
            raise CassetteError("events must be a list")
        for event in data["events"]:
            if not isinstance(event, dict) or set(event) != {
                "name",
                "args",
                "result",
                "error",
            }:
                raise CassetteError("Invalid event schema")
            if not isinstance(event["name"], str) or not event["name"]:
                raise CassetteError("Invalid event name")
            _decode(event["args"])
            _decode(event["result"])
            error = event["error"]
            if error is not None:
                if (
                    not isinstance(error, dict)
                    or set(error) != {"kind", "message"}
                    or not all(isinstance(v, str) for v in error.values())
                    or event["result"] is not None
                ):
                    raise CassetteError("Invalid recorded error")
        self._events = data["events"]

    def call(self, name: str, args: Any, operation: Callable[[], T]) -> T:
        """Capture a typed operation; replay never calls operation.

        External exceptions become RecordedError in both modes. Programmer and
        serialization errors propagate. Custom operations must return JSON-safe
        scalars, bytes, tuples, lists, or string-keyed dictionaries.
        """
        if self._closed:
            raise CassetteError("Cassette is closed")
        if not isinstance(name, str) or not name:
            raise ValueError("operation name must not be empty")
        encoded = _encode(self.redactor(args))
        if self.mode == "record":
            try:
                raw = operation()
            except Exception as e:
                kind = f"{type(e).__module__}.{type(e).__qualname__}"
                message = self.redactor(str(e))
                if not isinstance(message, str):
                    raise CassetteError("Redactor must preserve error strings") from e
                self._events.append(
                    {
                        "name": name,
                        "args": encoded,
                        "result": None,
                        "error": {"kind": kind, "message": message},
                    }
                )
                raise RecordedError(kind, message) from e
            result = _encode(self.redactor(raw))
            self._events.append(
                {"name": name, "args": encoded, "result": result, "error": None}
            )
            return _decode(result)  # type: ignore[no-any-return]
        if self._position >= len(self._events):
            raise CassetteError(f"Unexpected call #{self._position + 1}: {name}")
        event = self._events[self._position]
        if event["name"] != name or _canonical(event["args"]) != _canonical(encoded):
            raise CassetteError(f"Call #{self._position + 1} does not match: {name}")
        self._position += 1
        if event["error"] is not None:
            raise RecordedError(**event["error"])
        return _decode(event["result"])  # type: ignore[no-any-return]

    def close(self) -> None:
        """Atomically persist a recording or assert replay completion."""
        if self._closed:
            return
        if self.mode == "replay":
            if self._position != len(self._events):
                raise CassetteError(
                    f"{len(self._events) - self._position} unconsumed calls"
                )
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(
                {"version": 1, "events": self._events}, indent=2, allow_nan=False
            )
            fd, temporary = tempfile.mkstemp(dir=self.path.parent, prefix=".cassette-")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as file:
                    file.write(payload)
                    file.flush()
                    os.fsync(file.fileno())
                os.replace(temporary, self.path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        self._closed = True

    def __enter__(self) -> Self:
        if self._closed:
            raise CassetteError("Cassette is closed")
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.close()
        else:
            self._closed = True
