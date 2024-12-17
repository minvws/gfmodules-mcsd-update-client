import time
from typing import Callable, Awaitable

import statsd
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


class NoopStats(Stats):
    def timing(self, key: str, value: int) -> None:
        pass

    def inc(self, key: str, count: int = 1, rate: int = 1) -> None:
        pass

    def dec(self, key: str, count: int = 1, rate: int = 1) -> None:
        pass

    def gauge(self, key: str, value: int, delta: bool = False) -> None:
        pass


class Statsd(Stats):
    def __init__(self, host: str, port: int):
        self.client = statsd.StatsClient(host, port)

    def timing(self, key: str, value: int) -> None:
        self.client.timing(key, value)

    def inc(self, key: str, count: int = 1, rate: int = 1) -> None:
        self.client.incr(key, count, rate)

    def dec(self, key: str, count: int = 1, rate: int = 1) -> None:
        self.client.decr(key, count, rate)

    def gauge(self, key: str, value: int, delta: bool = False) -> None:
        self.client.gauge(key, value, delta)


_STATS: Stats = NoopStats()


def setup_stats() -> None:
    config = get_config()

    if config.stats.enabled is False:
        return

    config.stats.host = config.stats.host or "localhost"
    config.stats.port = config.stats.port or 8125

    global _STATS
    _STATS = Statsd(config.stats.host, config.stats.port)


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

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        normalized_path = self.normalize_path(request)

        key = f"{self.module_name}.http.request.{request.method.lower()}.{normalized_path}"
        get_stats().inc(key)

        start_time = time.monotonic()
        response = await call_next(request)
        end_time = time.monotonic()

        response_time = int((end_time - start_time) * 1000)
        get_stats().timing(f"{self.module_name}.http.response_time", response_time)

        return response

    @staticmethod
    def normalize_path(request: Request) -> str:
        """
        Normalize the path to remove resource IDs. This makes it easier to group similar requests together in the stats
        """
        if not request.url.path.startswith("/resource/"):
            return request.url.path

        parts = request.url.path.split("/")

        if len(parts) >= 4 and parts[3] != "_search":
            parts[3] = "%resource_id%"  # Remove Resource ID
        if len(parts) >= 5 and parts[4] == "_history":
            parts[5] = "%version_id%"  # Remove Version ID
        return '/'.join(parts)




