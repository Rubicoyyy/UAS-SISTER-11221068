from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncio
import logging
import time

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError

from .config import Settings
from .dedup import DedupStore
from .queue_backends import (
    MemoryQueueBackend,
    QueueBackend,
    QueueFullError,
    RedisQueueBackend,
)

logger = logging.getLogger("aggregator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class EventModel(BaseModel):
    topic: str = Field(min_length=1, max_length=128)
    event_id: str = Field(min_length=1, max_length=128)
    timestamp: datetime
    source: str = Field(min_length=1, max_length=128)
    payload: Dict[str, Any] = Field(default_factory=dict)


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    cfg = settings or Settings.from_env()
    app = FastAPI(title="UTS Log Aggregator", version="2.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.start_time = time.time()
    app.state.settings = cfg
    app.state.store = DedupStore(cfg.database_url)
    app.state.queue_backend: Optional[QueueBackend] = None
    app.state.consumer_tasks: List[asyncio.Task] = []

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        logger.info("Starting aggregator service")
        await run_in_threadpool(app.state.store.init_db)
        app.state.queue_backend = _build_queue_backend(cfg)
        for worker_id in range(cfg.worker_count):
            task = asyncio.create_task(consumer_loop(app, worker_id))
            app.state.consumer_tasks.append(task)
        try:
            yield
        finally:
            logger.info("Shutting down aggregator service")
            for task in app.state.consumer_tasks:
                task.cancel()
            if app.state.consumer_tasks:
                await asyncio.gather(*app.state.consumer_tasks, return_exceptions=True)
            if app.state.queue_backend:
                await app.state.queue_backend.close()
            await run_in_threadpool(app.state.store.close)

    app.router.lifespan_context = lifespan

    @app.post("/publish", status_code=202)
    async def publish(request: Request):
        try:
            body = await request.json()
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail="Invalid JSON body") from exc

        if isinstance(body, dict):
            raw_events = [body]
        elif isinstance(body, list):
            if not body:
                raise HTTPException(status_code=400, detail="Batch payload cannot be empty")
            raw_events = body
        else:
            raise HTTPException(status_code=400, detail="JSON payload must be an object or array")

        parsed_events: List[EventModel] = []
        for idx, candidate in enumerate(raw_events):
            try:
                parsed_events.append(EventModel(**candidate))
            except ValidationError as exc:  # noqa: BLE001
                raise HTTPException(status_code=422, detail={"index": idx, "errors": exc.errors()}) from exc

        serialized = [event.model_dump(mode="json") for event in parsed_events]
        queue_backend = app.state.queue_backend
        if queue_backend is None:
            raise HTTPException(status_code=503, detail="Queue backend is not ready")
        try:
            await queue_backend.enqueue_many(serialized)
        except QueueFullError:
            raise HTTPException(status_code=503, detail="Queue is full, please retry") from None

        await run_in_threadpool(app.state.store.increment_received, len(serialized))
        return {"accepted": len(serialized), "worker_count": cfg.worker_count}

    @app.get("/events")
    async def get_events(topic: Optional[str] = None):
        return await run_in_threadpool(app.state.store.list_events, topic)

    @app.get("/stats")
    async def get_stats():
        stats = await run_in_threadpool(app.state.store.get_stats)
        topics = await run_in_threadpool(app.state.store.list_topics)
        stats.update(
            {
                "topics": topics,
                "uptime_seconds": round(time.time() - app.state.start_time, 2),
                "worker_count": cfg.worker_count,
            }
        )
        return stats

    @app.get("/healthz")
    async def healthz():
        await run_in_threadpool(app.state.store.health_check)
        return {"status": "ok"}

    return app


def _build_queue_backend(cfg: Settings) -> QueueBackend:
    if cfg.broker_url.startswith("redis://"):
        return RedisQueueBackend(cfg.broker_url, cfg.broker_queue_key)
    return MemoryQueueBackend(cfg.queue_maxsize)


async def consumer_loop(app: FastAPI, worker_id: int) -> None:
    logger.info("Worker %s started", worker_id)
    poll_timeout = max(1, app.state.settings.queue_poll_interval)
    while True:
        try:
            backend = app.state.queue_backend
            if backend is None:
                await asyncio.sleep(0.1)
                continue
            event_data = await backend.dequeue(timeout=poll_timeout)
            if event_data is None:
                continue
            inserted = await run_in_threadpool(app.state.store.record_event, event_data)
            if inserted:
                logger.info("Worker %s processed %s:%s", worker_id, event_data["topic"], event_data["event_id"])
            else:
                logger.info(
                    "Worker %s skipped duplicate %s:%s", worker_id, event_data["topic"], event_data["event_id"]
                )
        except asyncio.CancelledError:
            logger.info("Worker %s cancelled", worker_id)
            break
        except Exception as exc:  # noqa: BLE001
            logger.exception("Worker %s failure: %s", worker_id, exc)
            await asyncio.sleep(0.5)


app = create_app()


if __name__ == "__main__":
    uvicorn.run("src.main:app", host="0.0.0.0", port=8080)