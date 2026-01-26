from contextlib import contextmanager
from datetime import timedelta
import time
from typing import Any, Callable, Awaitable, Generator

import statsd
from statsd.client.timer import Timer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from app.config import get_config


class Stats:
    def timing(self, key: str, value: int) -> None:
        raise NotImplementedError

    def inc(self, key: str, count: int = 1, rate: int = 1) -> None:
        raise NotImplementedError

    def dec(self, key: str, count: int = 1, rate: int = 1) -> None:
        raise NotImplementedError

    def gauge(self, key: str, value: int, delta: bool = False) -> None:
        raise NotImplementedError

    def timer(self, key: str) -> Timer:
        raise NotImplementedError


class NoopStats(Stats):
    def timing(self, key: str, value: int) -> None:
        """Empty method due to NoopStats implementation"""
        pass

    def inc(self, key: str, count: int = 1, rate: int = 1) -> None:
        """Empty method due to NoopStats implementation"""
        pass

    def dec(self, key: str, count: int = 1, rate: int = 1) -> None:
        """Empty method due to NoopStats implementation"""
        pass

    def gauge(self, key: str, value: int, delta: bool = False) -> None:
        """Empty method due to NoopStats implementation"""
        pass

    def timer(self, key: str) -> Timer:
        @contextmanager
        def noop_context_manager() -> Generator[Any, Any, Any]:
            yield

        return noop_context_manager()


class MemoryClient:
    def __init__(self) -> None:
        self.memory: dict[str, Any] = {}

    def timer(self, stat: str, rate: int = 1) -> Timer:
        return Timer(self, stat, rate)

    def timing(self, stat: str, delta: timedelta | float, rate: int = 1) -> None:
        """Record a timing stat. | Warning: Own implementation, not from statsd. | rate unused"""
        if isinstance(delta, timedelta):
            # Convert timedelta to number of milliseconds.
            delta = delta.total_seconds() * 1000.0
        if stat not in self.memory:
            self.memory[stat] = []
        self.memory[stat].append(delta)

    def incr(self, stat: str, count: int = 1, rate: int = 1) -> None:
        """Increment a stat by `count`. | Warning: Own implementation, not from statsd. | rate unused"""
        if stat not in self.memory:
            self.memory[stat] = 0
        self.memory[stat] += count

    def decr(self, stat: str, count: int = 1, rate: int = 1) -> None:
        """Decrement a stat by `count`. | Warning: Own implementation, not from statsd."""
        self.incr(stat, -count, rate)

    def gauge(self, stat: str, value: int, rate: int = 1, delta: bool = False) -> None:
        """Set a gauge value. | Warning: Own implementation, not from statsd. | rate and delta unused"""
        if stat not in self.memory:
            self.memory[stat] = []
        snapshot = {"value": value, "timestamp": time.time()}
        self.memory[stat].append(snapshot)

    def get_memory(self) -> dict[str, Any]:
        return self.memory


class Statsd(Stats):
    def __init__(self, client: statsd.StatsClient | MemoryClient):
        self.client = client

    def timing(self, key: str, value: int) -> None:
        self.client.timing(key, value)

    def inc(self, key: str, count: int = 1, rate: int = 1) -> None:
        self.client.incr(key, count, rate)

    def dec(self, key: str, count: int = 1, rate: int = 1) -> None:
        self.client.decr(key, count, rate)

    def gauge(self, key: str, value: int, delta: bool = False) -> None:
        self.client.gauge(key, value, delta)

    def timer(self, key: str) -> Timer:
        return self.client.timer(key)


_STATS: Stats = NoopStats()


def setup_stats() -> None:
    config = get_config()

    if config.stats.enabled is False:
        return
    in_memory = (config.stats.host is None or config.stats.host == "")
    client = (
        MemoryClient()
        if in_memory
        else statsd.StatsClient(config.stats.host, config.stats.port)
    )
    global _STATS
    _STATS = Statsd(client)


def get_stats() -> Stats:
    global _STATS
    return _STATS


class StatsdMiddleware(BaseHTTPMiddleware):
    """
    Middleware to record request info and response time for each request
    """

    def __init__(self, app: ASGIApp, module_name: str):
        super().__init__(app)
        self.module_name = module_name

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        key = f"{self.module_name}.http.request.{request.method.lower()}.{request.url.path}"
        get_stats().inc(key)

        start_time = time.monotonic()
        response = await call_next(request)
        end_time = time.monotonic()

        response_time = int((end_time - start_time) * 1000)
        get_stats().timing(f"{self.module_name}.http.response_time", response_time)

        return response
