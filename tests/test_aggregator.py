import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict
from fastapi.testclient import TestClient

from src.config import Settings
from src.dedup import DedupStore
from src.main import create_app
from src.queue_backends import QueueBackend, QueueFullError


def _settings(tmp_path, **overrides) -> Settings:
    base = Settings(
        database_url=f"sqlite:///{tmp_path}/events.db",
        broker_url="memory://tests",
        broker_queue_key="in-memory",
        worker_count=2,
        queue_maxsize=200,
        queue_poll_interval=1,
    )
    return base.model_copy(update=overrides)


def _wait_for(predicate, timeout: float = 2.0) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def _sample_event(idx: int, topic: str = "t-main") -> Dict[str, object]:
    return {
        "topic": topic,
        "event_id": f"evt-{idx}",
        "timestamp": "2025-10-24T00:00:00Z",
        "source": "test-suite",
        "payload": {"idx": idx},
    }


def test_publish_single_event_processed(tmp_path):
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    with client:
        response = client.post("/publish", json=_sample_event(1))
        assert response.status_code == 202
        assert response.json()["accepted"] == 1

        assert _wait_for(lambda: client.get("/stats").json()["unique_processed"] == 1)
        events = client.get("/events").json()
        assert any(evt["event_id"] == "evt-1" for evt in events)


def test_publish_batch_deduplicates(tmp_path):
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    batch = [_sample_event(i) for i in range(5)] + [_sample_event(2), _sample_event(4)]
    with client:
        response = client.post("/publish", json=batch)
        assert response.status_code == 202
        assert response.json()["accepted"] == len(batch)
        assert _wait_for(lambda: client.get("/stats").json()["duplicate_dropped"] == 2)


def test_empty_batch_rejected(tmp_path):
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    with client:
        response = client.post("/publish", json=[])
        assert response.status_code == 400


def test_invalid_schema_rejected(tmp_path):
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    invalid_event = {"topic": "bad", "timestamp": "2025-10-24T00:00:00Z", "source": "x", "payload": {}}
    with client:
        response = client.post("/publish", json=invalid_event)
        assert response.status_code == 422


def test_get_events_filtered_by_topic(tmp_path):
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    with client:
        client.post("/publish", json=_sample_event(1, topic="alpha"))
        client.post("/publish", json=_sample_event(2, topic="beta"))
        assert _wait_for(lambda: client.get("/stats").json()["unique_processed"] >= 2)
        alpha_events = client.get("/events", params={"topic": "alpha"}).json()
        assert len(alpha_events) == 1 and alpha_events[0]["topic"] == "alpha"


def test_stats_include_topics_and_counts(tmp_path):
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    with client:
        for idx in range(3):
            client.post("/publish", json=_sample_event(idx, topic="metrics"))
        assert _wait_for(lambda: client.get("/stats").json()["unique_processed"] == 3)
        stats = client.get("/stats").json()
        topic_entry = next((t for t in stats["topics"] if t["topic"] == "metrics"), None)
        assert topic_entry is not None and topic_entry["count"] == 3
        assert stats["worker_count"] == 2


def test_store_persistence_across_instances(tmp_path):
    db_url = f"sqlite:///{tmp_path}/persist.db"
    store = DedupStore(db_url)
    store.init_db()
    store.record_event(_sample_event(1))
    store.close()

    store_second = DedupStore(db_url)
    store_second.init_db()
    assert store_second.record_event(_sample_event(1)) is False


def test_concurrent_recording_is_idempotent(tmp_path):
    db_url = f"sqlite:///{tmp_path}/race.db"
    store = DedupStore(db_url)
    store.init_db()

    def _write():
        return store.record_event(_sample_event(42))

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(lambda _: _write(), range(20)))
    assert results.count(True) == 1
    stats = store.get_stats()
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] == 19


def test_queue_full_returns_503(tmp_path):
    settings = _settings(tmp_path, queue_maxsize=1)
    app = create_app(settings)

    class AlwaysFullQueue(QueueBackend):
        async def enqueue_many(self, events):  # type: ignore[override]
            raise QueueFullError("full")

        async def dequeue(self, timeout: int):  # type: ignore[override]
            await asyncio.sleep(timeout)
            return None

        async def close(self):  # type: ignore[override]
            return None

    client = TestClient(app)
    with client:
        client.app.state.queue_backend = AlwaysFullQueue()
        response = client.post("/publish", json=_sample_event(9))
        assert response.status_code == 503


def test_healthz_endpoint(tmp_path):
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    with client:
        resp = client.get("/healthz")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


def test_store_topic_listing(tmp_path):
    db_url = f"sqlite:///{tmp_path}/topics.db"
    store = DedupStore(db_url)
    store.init_db()
    store.record_event(_sample_event(1, topic="alpha"))
    store.record_event(_sample_event(2, topic="beta"))
    store.record_event(_sample_event(3, topic="alpha"))
    topics = store.list_topics()
    alpha = next(t for t in topics if t["topic"] == "alpha")
    assert alpha["count"] == 2


def test_publish_requires_json(tmp_path):
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    with client:
        response = client.post("/publish", data="not-json", headers={"Content-Type": "text/plain"})
        assert response.status_code == 400
