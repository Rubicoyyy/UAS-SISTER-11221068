import os
from pydantic import BaseModel, Field


class Settings(BaseModel):
    """Application configuration resolved from environment variables."""

    database_url: str = Field(default="sqlite:///./data/dedup.db")
    broker_url: str = Field(default="memory://local")
    broker_queue_key: str = Field(default="aggregator:events")
    worker_count: int = Field(default=4, ge=1, le=64)
    queue_maxsize: int = Field(default=5000, ge=100)
    queue_poll_interval: int = Field(default=2, ge=1, le=30)

    @classmethod
    def from_env(cls) -> "Settings":
        """Build a Settings instance by reading environment variables."""

        def _int_value(name: str, default: int) -> int:
            raw = os.getenv(name)
            if raw is None:
                return default
            try:
                return int(raw)
            except ValueError:
                return default

        return cls(
            database_url=os.getenv("DATABASE_URL", cls.model_fields["database_url"].default),
            broker_url=os.getenv("BROKER_URL", cls.model_fields["broker_url"].default),
            broker_queue_key=os.getenv("BROKER_QUEUE_KEY", cls.model_fields["broker_queue_key"].default),
            worker_count=_int_value("WORKER_COUNT", cls.model_fields["worker_count"].default),
            queue_maxsize=_int_value("QUEUE_MAXSIZE", cls.model_fields["queue_maxsize"].default),
            queue_poll_interval=_int_value("QUEUE_POLL_INTERVAL", cls.model_fields["queue_poll_interval"].default),
        )
