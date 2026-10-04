import os
import uuid

import pytest

from replayable_kit import Cassette, Redis


def test_real_redis_record_and_client_free_replay(tmp_path):
    url = os.environ.get("REPLAYABLE_REDIS_URL")
    if not url:
        pytest.skip("Set REPLAYABLE_REDIS_URL for real Redis integration")
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(url)
    key = "replayable-kit:test:" + uuid.uuid4().hex
    path = tmp_path / "redis.json"
    try:
        with Cassette(path, "record") as tape:
            cache = Redis(tape, client)
            assert cache.command("SET", key, b"hello", "EX", 30) is True
            assert cache.command("GET", key) == b"hello"
            assert cache.command("DEL", key) == 1
        with Cassette(path, "replay") as tape:
            cache = Redis(tape)
            assert cache.command("SET", key, b"hello", "EX", 30) is True
            assert cache.command("GET", key) == b"hello"
            assert cache.command("DEL", key) == 1
        assert client.get(key) is None
    finally:
        client.delete(key)
        client.close()
