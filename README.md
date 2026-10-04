# replayable-kit

Record external dependencies once, then replay the same call sequence offline.
Explicit synchronous adapters make dependency boundaries visible without global
patches. Python 3.11+; no runtime dependencies for the core library.

```sh
pip install replayable-kit
# Optional, to supply a synchronous redis-py client:
pip install 'replayable-kit[redis]'
```

## Quick start

```python
from replayable_kit import Cassette, HTTP, Clock, Randomness


def workflow(tape):
    response = HTTP(tape).request("https://example.com/")
    return response.status, Clock(tape).time(), Randomness(tape).randint(1, 6)


with Cassette("workflow.cassette.json", "record") as tape:
    expected = workflow(tape)

with Cassette("workflow.cassette.json", "replay") as tape:
    assert workflow(tape) == expected
```

Replay checks operation names, arguments, and call order. An extra call or changed
argument raises `CassetteError`; successful context exit checks for missing calls.
Replay never invokes an adapter's external operation. Give adapters distinct
`name=` values when your application uses several databases, clients, or clocks.
Each cassette belongs to one thread and one sequential workflow; concurrent and
async workloads need their own independent cassettes.

## Adapters

| Adapter | Supported operations | Recording behavior |
| --- | --- | --- |
| `HTTP` | `request(url, method="GET", body=None, headers=None, timeout=10)` | urllib request, response status/headers/body; non-2xx responses remain ordinary responses |
| `Database` | `execute(sql, parameters=())`, `commit()`, `rollback()` | DB-API cursor with eager fetchall, row tuples, rowcount, column names |
| `Redis` | `command(command, *arguments)` | redis-py synchronous `execute_command` |
| `Filesystem` | `read(path)`, `write(path, bytes)`, `unlink(path)` | Actual byte IO in record; no file IO in replay |
| `Processes` | `run(args, cwd=None, env=None, input=None, timeout=10)` | Shell-free subprocess, bytes stdout/stderr, return code |
| `Clock` | `time()`, `monotonic()`, `sleep(seconds)` | Actual clocks/sleep in record; immediate replay |
| `Randomness` | `random()`, `randint(a,b)`, `uniform(a,b)` | Owned random generator; accepts a supplied `random.Random` |

### Database and Redis

```python
import sqlite3
from replayable_kit import Cassette, Database

connection = sqlite3.connect(":memory:")
with Cassette("database.cassette.json", "record") as tape:
    db = Database(tape, connection)
    db.execute("CREATE TABLE items (name TEXT)")
    db.execute("INSERT INTO items VALUES (?)", ("example",))
    db.commit()
    assert db.execute("SELECT name FROM items").rows == [("example",)]
connection.close()

# Replay the same operations with Database(tape), without any connection.
```

```python
import redis
from replayable_kit import Cassette, Redis

client = redis.Redis(host="localhost", port=6379)
with Cassette("redis.cassette.json", "record") as tape:
    cache = Redis(tape, client)
    cache.command("SET", "example", b"hello", "EX", 60)
    assert cache.command("GET", "example") == b"hello"
# Replay with Redis(tape), without a client or Redis server.
client.close()
```

Connections remain owned by the caller. Transactions are recorded explicitly;
replayed commits, rollbacks, writes, and commands produce no external effects.
Database values support null, bool, int, finite float, str, bytes, lists, tuples,
and dictionaries with string keys. Convert Decimal/datetime/driver objects to
these types before recording (for example, cast in SQL). The Redis adapter handles
commands whose replies fit this codec; pub/sub streams, pipelines and async
clients are outside its scope.

### Custom dependencies and errors

`tape.call("unique.operation", args, operation)` records any synchronous callable
with codec-compatible results and preserves its return type in static typing.
Pass the same inputs in replay; the callable will never run. Serialization errors
raise `CassetteError`. Exceptions raised by external operations become
`RecordedError` in **both** modes, with `kind` (original qualified exception class)
and `message`. Catch that exception inside the context to persist a failure:

```python
from replayable_kit import Cassette, Filesystem, RecordedError

with Cassette("missing.cassette.json", "record") as tape:
    try:
        Filesystem(tape).read("missing-file")
    except RecordedError as error:
        print(error.kind, error.message)
```

The original exception class is not reconstructed or imported. KeyboardInterrupt
and SystemExit propagate. A context exiting with an exception does not write its
recording. Successful records atomically replace the destination file; existing
cassettes are not merged. `close()` supports use without a context manager.

## Secrets and cassette safety

Cassettes are versioned JSON, never pickle. Loading validates the entire schema
and encoded values before replay. Default redaction replaces dictionary values
under authorization, proxy-authorization, cookie, set-cookie, password, token,
api_key, x-api-key, and secret keys (case-insensitive). HTTP headers are normalized
to lowercase. Redaction applies to arguments, responses, and error messages
before storage; recorded returns are also redacted to keep replay identical.
Requests still receive their original inputs during recording.

This is deliberately limited: arbitrary URLs, SQL, Redis arguments, subprocess
arguments, response bodies and error strings may contain secrets. Supply
`redactor=` to remove application-specific secrets, including exception strings,
and inspect recordings before committing them. A custom redactor must preserve
the structure/type required by the adapter and should compose with
`redact_secrets`. Redacted credentials do not participate in matching; rotate
them freely, but this cannot test differences between credential values. Cassettes
can contain private application data and should be treated as test fixtures, not
untrusted size-unbounded input. Replay determinism covers calls through these
adapters, not your application's unwrapped dependencies or CPU/thread scheduling.

## Development

```sh
python -m venv .venv
# Activate .venv using your platform's standard activation command.
python -m pip install -e '.[dev,redis]'
python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m build
python examples/offline.py
```

The default suite needs no services. Set `REPLAYABLE_REDIS_URL` to run the real
Redis integration test (CI runs it against a Redis service). Tests run application
code natively; containers are only used for infrastructure. CI covers Linux and
Windows, Python 3.11 and 3.14, with typing, lint, format, wheel/sdist, and an offline
example. See [examples/offline.py](examples/offline.py) for a runnable recording
and replay workflow. Contributions should include a focused test and use the
checks above. Licensed under the [MIT license](LICENSE).
