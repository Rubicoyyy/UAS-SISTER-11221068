import asyncio
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import redis.asyncio as redis


class QueueError(RuntimeError):
    """Base exception for queue related issues."""


class QueueFullError(QueueError):
    """Raised when the queue is full and cannot accept more events."""


class QueueBackend(ABC):
    """Abstraction for publish/consume backends."""

    @abstractmethod
    async def enqueue_many(self, events: List[Dict[str, Any]]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def dequeue(self, timeout: int) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class MemoryQueueBackend(QueueBackend):
    """In-memory asyncio queue, useful for tests and local runs."""

    def __init__(self, maxsize: int):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)

    async def enqueue_many(self, events: List[Dict[str, Any]]) -> None:
        for event in events:
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull as exc:
                raise QueueFullError("Memory queue is at capacity") from exc

    async def dequeue(self, timeout: int) -> Optional[Dict[str, Any]]:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def close(self) -> None:
        # Nothing to clean up for in-memory queue.
        return None


class RedisQueueBackend(QueueBackend):
    """Redis list-backed queue using BLPOP/RPUSH operations."""

    def __init__(self, url: str, queue_key: str):
        self._client = redis.from_url(url, encoding="utf-8", decode_responses=True)
        self._queue_key = queue_key

    async def enqueue_many(self, events: List[Dict[str, Any]]) -> None:
        if not events:
            return
        payloads = [json.dumps(evt, separators=(",", ":")) for evt in events]
        await self._client.rpush(self._queue_key, *payloads)

    async def dequeue(self, timeout: int) -> Optional[Dict[str, Any]]:
        timeout = max(1, int(timeout))
        result = await self._client.blpop(self._queue_key, timeout=timeout)
        if result is None:
            return None
        _, payload = result
        return json.loads(payload)

    async def close(self) -> None:
        await self._client.aclose()
