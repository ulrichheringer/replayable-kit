import random
import sqlite3
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from replayable_kit import (
    HTTP,
    Cassette,
    Clock,
    Database,
    Filesystem,
    Processes,
    Randomness,
    RecordedError,
)


def forbidden(*args, **kwargs):
    raise AssertionError("Replay executed external operation")


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.send_response(201)
        self.send_header("X-Test", "recorded")
        self.send_header("Set-Cookie", "private")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"missing")

    def log_message(self, *args):
        pass


def test_http_real_record_offline_replay(tmp_path, monkeypatch):
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/"
    path = tmp_path / "http.json"
    try:
        with Cassette(path, "record") as tape:
            client = HTTP(tape)
            result = client.request(
                url, method="POST", body=b"hello", headers={"Authorization": "private"}
            )
            missing = client.request(url)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert result.status == 201
    assert result.body == b"hello"
    assert result.headers["set-cookie"] == "<redacted>"
    assert missing.status == 404 and missing.body == b"missing"
    assert "private" not in path.read_text()
    monkeypatch.setattr("urllib.request.urlopen", forbidden)
    with Cassette(path, "replay") as tape:
        client = HTTP(tape)
        assert (
            client.request(
                url, method="POST", body=b"hello", headers={"Authorization": "rotated"}
            )
            == result
        )
        assert client.request(url) == missing


def database_workflow(tape, connection=None):
    db = Database(tape, connection)
    db.execute("CREATE TABLE data (id INTEGER, payload BLOB)")
    db.execute("INSERT INTO data VALUES (?, ?)", (1, b"hello"))
    db.commit()
    db.execute("INSERT INTO data VALUES (?, ?)", (2, b"discard"))
    db.rollback()
    return db.execute("SELECT id, payload FROM data")


def test_sqlite_transactions_without_connection_on_replay(tmp_path):
    path = tmp_path / "db.json"
    connection = sqlite3.connect(":memory:")
    with Cassette(path, "record") as tape:
        result = database_workflow(tape, connection)
    connection.close()
    assert result.rows == [(1, b"hello")]
    assert result.columns == ("id", "payload")
    with Cassette(path, "replay") as tape:
        assert database_workflow(tape) == result


def test_filesystem_replay_never_mutates(tmp_path):
    path = tmp_path / "fs.json"
    data = tmp_path / "data"
    with Cassette(path, "record") as tape:
        files = Filesystem(tape)
        assert files.write(data, b"hello") == 5
        assert files.read(data) == b"hello"
        files.unlink(data)
    data.write_bytes(b"keep me")
    with Cassette(path, "replay") as tape:
        files = Filesystem(tape)
        assert files.write(data, b"hello") == 5
        assert files.read(data) == b"hello"
        files.unlink(data)
    assert data.read_bytes() == b"keep me"


def test_filesystem_error_replay(tmp_path):
    path = tmp_path / "fs.json"
    for mode in ("record", "replay"):
        with Cassette(path, mode) as tape:
            with pytest.raises(RecordedError, match="FileNotFoundError"):
                Filesystem(tape).read(tmp_path / "missing")


def test_subprocess_and_timeout(tmp_path, monkeypatch):
    path = tmp_path / "process.json"
    args = [
        sys.executable,
        "-c",
        "import sys; print('out'); print('err', file=sys.stderr); sys.exit(3)",
    ]
    slow = [sys.executable, "-c", "import time; time.sleep(2)"]
    with Cassette(path, "record") as tape:
        processes = Processes(tape)
        result = processes.run(args)
        with pytest.raises(RecordedError, match="TimeoutExpired"):
            processes.run(slow, timeout=0.02)
    assert result.returncode == 3
    assert result.stdout.strip() == b"out" and result.stderr.strip() == b"err"
    monkeypatch.setattr("subprocess.run", forbidden)
    with Cassette(path, "replay") as tape:
        processes = Processes(tape)
        assert processes.run(args) == result
        with pytest.raises(RecordedError, match="TimeoutExpired"):
            processes.run(slow, timeout=0.02)


def clock_random_workflow(tape):
    clock = Clock(tape)
    clock.sleep(0.001)
    rng = Randomness(tape, random.Random(123))
    return (
        clock.time(),
        clock.monotonic(),
        rng.random(),
        rng.randint(1, 10),
        rng.uniform(1, 2),
    )


def test_clock_random_replay(tmp_path, monkeypatch):
    path = tmp_path / "time.json"
    with Cassette(path, "record") as tape:
        result = clock_random_workflow(tape)
    monkeypatch.setattr("time.sleep", forbidden)
    monkeypatch.setattr("time.time", forbidden)
    monkeypatch.setattr("time.monotonic", forbidden)
    monkeypatch.setattr(random.Random, "random", forbidden)
    monkeypatch.setattr(random.Random, "randint", forbidden)
    monkeypatch.setattr(random.Random, "uniform", forbidden)
    with Cassette(path, "replay") as tape:
        assert clock_random_workflow(tape) == result
