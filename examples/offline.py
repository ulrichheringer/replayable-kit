"""Record real dependencies, then replay without executing them."""

import random
import sqlite3
import sys
import tempfile
from pathlib import Path

from replayable_kit import (
    Cassette,
    Clock,
    Database,
    Filesystem,
    Processes,
    Randomness,
)


def workflow(tape, path, connection=None):
    files = Filesystem(tape)
    files.write(path, b"hello")
    content = files.read(path)
    db = Database(tape, connection)
    db.execute("CREATE TABLE items (value TEXT)")
    db.execute("INSERT INTO items VALUES (?)", ("hello",))
    db.commit()
    rows = db.execute("SELECT value FROM items").rows
    process = Processes(tape).run([sys.executable, "-c", "print('hello')"])
    return (
        content,
        rows,
        process,
        Clock(tape).time(),
        Randomness(tape, random.Random(42)).randint(1, 100),
    )


with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    connection = sqlite3.connect(":memory:")
    with Cassette(root / "demo.json", "record") as tape:
        expected = workflow(tape, root / "data", connection)
    connection.close()
    (root / "data").unlink()
    with Cassette(root / "demo.json", "replay") as tape:
        assert workflow(tape, root / "data") == expected
    assert not (root / "data").exists()
    print("Offline replay matched database, filesystem, process, clock and random")
