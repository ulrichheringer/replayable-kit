import json
import math

import pytest

from replayable_kit import Cassette, CassetteError, RecordedError, redact_secrets


def forbidden():
    raise AssertionError("Replay executed an external operation")


def test_roundtrip_types(tmp_path):
    path = tmp_path / "tape.json"
    value = {
        "bytes": b"\x00\xff",
        "tuple": (None, True, 42, 0.25),
        "list": ["hello"],
        "tag": {"$bytes": "ordinary dictionary"},
    }
    with Cassette(path, "record") as tape:
        assert tape.call("value", ("arg",), lambda: value) == value
    with Cassette(path, "replay") as tape:
        assert tape.call("value", ("arg",), forbidden) == value


def test_errors_identical_and_safe(tmp_path):
    path = tmp_path / "tape.json"

    def fail():
        raise ValueError("external failure")

    with Cassette(path, "record") as tape:
        with pytest.raises(RecordedError) as recorded:
            tape.call("fail", {}, fail)
    with Cassette(path, "replay") as tape:
        with pytest.raises(RecordedError) as replayed:
            tape.call("fail", {}, forbidden)
    assert str(recorded.value) == str(replayed.value)
    assert replayed.value.kind == "builtins.ValueError"


def test_order_arguments_and_extra_call(tmp_path):
    path = tmp_path / "tape.json"
    with Cassette(path, "record") as tape:
        tape.call("first", 1, lambda: 2)
    with Cassette(path, "replay") as tape:
        with pytest.raises(CassetteError, match="does not match"):
            tape.call("second", 1, forbidden)
        with pytest.raises(CassetteError, match="does not match"):
            tape.call("first", 2, forbidden)
        assert tape.call("first", 1, forbidden) == 2
        with pytest.raises(CassetteError, match="Unexpected"):
            tape.call("extra", 1, forbidden)


def test_unconsumed_call(tmp_path):
    path = tmp_path / "tape.json"
    with Cassette(path, "record") as tape:
        tape.call("first", 1, lambda: 2)
    with pytest.raises(CassetteError, match="unconsumed"):
        with Cassette(path, "replay"):
            pass


def test_aborted_record_preserves_file(tmp_path):
    path = tmp_path / "tape.json"
    with Cassette(path, "record") as tape:
        tape.call("first", 1, lambda: 2)
    original = path.read_bytes()
    with pytest.raises(RuntimeError):
        with Cassette(path, "record") as tape:
            tape.call("other", 0, lambda: 4)
            raise RuntimeError("abort")
    assert path.read_bytes() == original


@pytest.mark.parametrize("value", [object(), math.nan, math.inf, {1: "bad key"}])
def test_unsupported_values(tmp_path, value):
    with Cassette(tmp_path / "tape.json", "record") as tape:
        with pytest.raises(CassetteError, match="Unsupported"):
            tape.call("bad", (), lambda: value)


@pytest.mark.parametrize(
    "data",
    [
        None,
        {},
        {"version": True, "events": []},
        {"version": 2, "events": []},
        {"version": 1, "events": {}},
        {
            "version": 1,
            "events": [
                {"name": "x", "args": {"$bytes": "!"}, "result": None, "error": None}
            ],
        },
        {
            "version": 1,
            "events": [
                {
                    "name": "x",
                    "args": None,
                    "result": None,
                    "error": {"kind": 1, "message": "x"},
                }
            ],
        },
        {
            "version": 1,
            "events": [
                {
                    "name": "x",
                    "args": None,
                    "result": {"__import__": "os"},
                    "error": None,
                }
            ],
        },
    ],
)
def test_invalid_schema(tmp_path, data):
    path = tmp_path / "tape.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(CassetteError):
        Cassette(path, "replay")


def test_redaction_matches_rotated_credentials_and_returns_redacted(tmp_path):
    path = tmp_path / "tape.json"
    with Cassette(path, "record") as tape:
        result = tape.call(
            "secret",
            {"Authorization": "secret-one"},
            lambda: {"token": "secret-two", "data": b"safe"},
        )
    assert "secret-one" not in path.read_text()
    assert "secret-two" not in path.read_text()
    assert result["token"] == "<redacted>"
    with Cassette(path, "replay") as tape:
        assert tape.call("secret", {"Authorization": "rotated"}, forbidden) == result


def test_custom_redactor_covers_error_strings(tmp_path):
    path = tmp_path / "tape.json"

    def redact(value):
        if isinstance(value, str):
            return value.replace("sensitive", "redacted")
        return redact_secrets(value)

    def fail():
        raise RuntimeError("sensitive")

    with Cassette(path, "record", redactor=redact) as tape:
        with pytest.raises(RecordedError, match="redacted"):
            tape.call("operation", (), fail)
    assert "sensitive" not in path.read_text()


def test_closed_and_invalid_mode(tmp_path):
    with pytest.raises(ValueError):
        Cassette(tmp_path / "tape.json", "typo")
    tape = Cassette(tmp_path / "tape.json", "record")
    tape.close()
    tape.close()
    with pytest.raises(CassetteError, match="closed"):
        tape.call("value", (), lambda: 1)


def test_missing_and_malformed_file(tmp_path):
    path = tmp_path / "tape.json"
    with pytest.raises(CassetteError, match="Cannot read"):
        Cassette(path, "replay")


@pytest.mark.parametrize("different", [True, 1.0])
def test_argument_matching_preserves_scalar_types(tmp_path, different):
    path = tmp_path / "tape.json"
    with Cassette(path, "record") as tape:
        tape.call("typed", 1, lambda: "ok")
    with Cassette(path, "replay") as tape:
        with pytest.raises(CassetteError, match="does not match"):
            tape.call("typed", different, forbidden)
        assert tape.call("typed", 1, forbidden) == "ok"
    path.write_text("{broken", encoding="utf-8")
    with pytest.raises(CassetteError, match="Cannot read"):
        Cassette(path, "replay")
