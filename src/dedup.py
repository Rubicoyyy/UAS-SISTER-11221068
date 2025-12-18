import json
import os
import threading
import time
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url


class DedupStore:
    """Transactional event store backed by a SQL database."""

    def __init__(self, database_url: str = "sqlite:///./data/dedup.db"):
        self.database_url = database_url
        self.engine = self._create_engine(database_url)
        self._initialized = False
        self._lock = threading.Lock()

    def _create_engine(self, url: str) -> Engine:
        sa_url = make_url(url)
        connect_args = {}
        if sa_url.get_backend_name() == "sqlite":
            db_path = sa_url.database
            if db_path and db_path != ":memory":
                directory = os.path.dirname(db_path)
                if directory:
                    os.makedirs(directory, exist_ok=True)
            connect_args["check_same_thread"] = False
        return create_engine(url, future=True, pool_pre_ping=True, connect_args=connect_args)

    def init_db(self) -> None:
        with self._lock:
            if self._initialized:
                return
            attempts = 0
            last_error: Optional[Exception] = None
            while attempts < 5:
                try:
                    with self.engine.begin() as conn:
                        conn.execute(
                            text(
                                """
                                CREATE TABLE IF NOT EXISTS events (
                                    topic TEXT NOT NULL,
                                    event_id TEXT NOT NULL,
                                    timestamp TEXT NOT NULL,
                                    source TEXT NOT NULL,
                                    payload TEXT NOT NULL,
                                    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    PRIMARY KEY (topic, event_id)
                                )
                                """
                            )
                        )
                        conn.execute(
                            text(
                                """
                                CREATE TABLE IF NOT EXISTS system_metrics (
                                    id INTEGER PRIMARY KEY CHECK (id = 1),
                                    received BIGINT NOT NULL DEFAULT 0,
                                    unique_processed BIGINT NOT NULL DEFAULT 0,
                                    duplicate_dropped BIGINT NOT NULL DEFAULT 0
                                )
                                """
                            )
                        )
                        conn.execute(
                            text("INSERT INTO system_metrics (id) VALUES (1) ON CONFLICT(id) DO NOTHING")
                        )
                    self._initialized = True
                    return
                except Exception as exc:  # noqa: BLE001
                    attempts += 1
                    last_error = exc
                    time.sleep(min(2 * attempts, 5))
            if last_error:
                raise last_error

    def increment_received(self, amount: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE system_metrics SET received = received + :amount WHERE id = 1"),
                {"amount": amount},
            )

    def record_event(self, event: Dict[str, Any]) -> bool:
        payload_json = json.dumps(event.get("payload", {}), separators=(",", ":"))
        params = {
            "topic": event["topic"],
            "event_id": event["event_id"],
            "timestamp": event["timestamp"],
            "source": event.get("source", ""),
            "payload": payload_json,
        }
        with self.engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO events (topic, event_id, timestamp, source, payload)
                    VALUES (:topic, :event_id, :timestamp, :source, :payload)
                    ON CONFLICT(topic, event_id) DO NOTHING
                    """
                ),
                params,
            )
            inserted = result.rowcount == 1
            column = "unique_processed" if inserted else "duplicate_dropped"
            conn.execute(
                text(f"UPDATE system_metrics SET {column} = {column} + 1 WHERE id = 1")
            )
            return inserted

    def list_events(self, topic: Optional[str] = None) -> List[Dict[str, Any]]:
        query = "SELECT topic, event_id, timestamp, source, payload, ingested_at FROM events"
        params: Dict[str, Any] = {}
        if topic:
            query += " WHERE topic = :topic"
            params["topic"] = topic
        query += " ORDER BY ingested_at"
        with self.engine.begin() as conn:
            rows = conn.execute(text(query), params).all()
        events: List[Dict[str, Any]] = []
        for row in rows:
            payload = row.payload
            try:
                payload_dict = json.loads(payload)
            except json.JSONDecodeError:
                payload_dict = {"raw": payload}
            ingested_at = row.ingested_at
            if hasattr(ingested_at, "isoformat"):
                ingested_value = ingested_at.isoformat()
            else:
                ingested_value = str(ingested_at) if ingested_at else None
            events.append(
                {
                    "topic": row.topic,
                    "event_id": row.event_id,
                    "timestamp": row.timestamp,
                    "source": row.source,
                    "payload": payload_dict,
                    "ingested_at": ingested_value,
                }
            )
        return events

    def list_topics(self) -> List[Dict[str, Any]]:
        with self.engine.begin() as conn:
            rows = conn.execute(
                text("SELECT topic, COUNT(*) AS count FROM events GROUP BY topic ORDER BY topic")
            ).all()
        return [{"topic": row.topic, "count": row.count} for row in rows]

    def get_stats(self) -> Dict[str, int]:
        with self.engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT received, unique_processed, duplicate_dropped FROM system_metrics WHERE id = 1"
                )
            ).first()
        if row is None:
            return {"received": 0, "unique_processed": 0, "duplicate_dropped": 0}
        return {
            "received": row.received,
            "unique_processed": row.unique_processed,
            "duplicate_dropped": row.duplicate_dropped,
        }

    def health_check(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("SELECT 1"))

    def close(self) -> None:
        self.engine.dispose()
