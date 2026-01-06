from __future__ import annotations

from abc import ABC
import logging
import random
import threading
import time
from typing import Any, Callable, Dict

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import RequestException, Timeout
from yarl import URL

from app.services.api.authenticators.authenticator import Authenticator

logger = logging.getLogger(__name__)

# Transient HTTP statuses we can safely retry (esp. for idempotent semantics).
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


def request(  # noqa: A001 - Keep name for backwards-compatible test patching.
    method: str,
    url: str,
    *,
    session: Session | None = None,
    **kwargs: Any,
) -> Response:
    """
    Wrapper around requests that allows easy patching in tests.

    Tests in this repository historically patched `app.services.api.api_service.request`.
    By keeping a function with this name we maintain compatibility while still using
    a pooled `requests.Session` under the hood.
    """
    if session is None:
        return requests.request(method=method, url=url, **kwargs)
    return session.request(method=method, url=url, **kwargs)


class HttpService(ABC):
    """
    Base class for making HTTP requests with retry logic.

    Improvements over the reference implementation:
    - Uses a per-thread `requests.Session` for connection pooling (performance).
    - Retries on retryable HTTP status codes (reliability).
    - Correct handling of the `verify` parameter (security & correctness).
    - Inject-able `sleep` and `session_factory` for testability.
    """

    def __init__(
        self,
        base_url: str,
        timeout: int,
        retries: int,
        backoff: float,
        authenticator: Authenticator | None = None,
        mtls_cert: str | None = None,
        mtls_key: str | None = None,
        verify_ca: str | bool = True,
        # Optional tuning knobs.
        pool_connections: int = 10,
        pool_maxsize: int = 10,
        # Test hooks.
        sleep: Callable[[float], None] = time.sleep,
        session_factory: Callable[[], Session] | None = None,
    ) -> None:
        self.base_url = base_url
        self.authenticator = authenticator
        self.__mtls_cert = mtls_cert
        self.__mtls_key = mtls_key
        self.__verify_ca = verify_ca
        self.__timeout = timeout
        self.__retries = max(1, retries)
        self.__backoff = backoff
        self.__sleep = sleep

        self.__pool_connections = pool_connections
        self.__pool_maxsize = pool_maxsize

        self.__local = threading.local()
        self.__session_factory = session_factory or self.__default_session_factory

    def __default_session_factory(self) -> Session:
        session = Session()
        adapter = HTTPAdapter(
            pool_connections=self.__pool_connections,
            pool_maxsize=self.__pool_maxsize,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get_session(self) -> Session:
        session = getattr(self.__local, "session", None)
        if session is None:
            session = self.__session_factory()
            self.__local.session = session
        return session

    def do_request(
        self,
        method: str,
        sub_route: str | None = None,
        json: Dict[str, Any] | None = None,
        params: Dict[str, Any] | None = None,
    ) -> Response:
        """
        Perform an HTTP request with retry logic.
        """
        headers = self.make_headers()
        url = self.make_target_url(sub_route, params)

        # Correct `verify` handling:
        # - bool: enable/disable verification
        # - str: path to CA bundle
        verify = self.__verify_ca if self.__verify_ca is not None else True

        # Use pooled session per-thread (thread-safety).
        session = self._get_session()

        last_exc: Exception | None = None

        for attempt in range(self.__retries):
            try:
                logger.debug("HTTP request. method=%s url=%s attempt=%s/%s", method, url, attempt + 1, self.__retries)

                response = request(
                    method=method,
                    url=str(url),
                    session=session,
                    headers=headers,
                    timeout=self.__timeout,
                    json=json,
                    params=None,  # already applied by yarl URL
                    cert=(self.__mtls_cert, self.__mtls_key)
                    if self.__mtls_cert and self.__mtls_key
                    else None,
                    verify=verify,
                    auth=self.authenticator.get_auth() if self.authenticator else None,
                )

                # Retry on transient HTTP statuses.
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.__retries - 1:
                    delay = self.__compute_backoff(attempt)
                    logger.warning(
                        "Retryable HTTP status. status=%s method=%s url=%s retry_in=%.2fs attempt=%s/%s",
                        response.status_code,
                        method,
                        url,
                        delay,
                        attempt + 1,
                        self.__retries,
                    )
                    self.__sleep(delay)
                    continue

                return response

            except (RequestsConnectionError, Timeout) as e:
                last_exc = e
                if attempt < self.__retries - 1:
                    delay = self.__compute_backoff(attempt)
                    logger.warning(
                        "Transient network error. error=%s method=%s url=%s retry_in=%.2fs attempt=%s/%s",
                        type(e).__name__,
                        method,
                        url,
                        delay,
                        attempt + 1,
                        self.__retries,
                    )
                    self.__sleep(delay)
                    continue
                break
            except RequestException as e:
                # Catch-all for other request-level errors (e.g. invalid URL).
                last_exc = e
                logger.exception("Request failed. method=%s url=%s", method, url)
                break

        if last_exc is not None:
            logger.error(
                "Failed to make request after %s attempts. error=%s detail=%s method=%s url=%s",
                self.__retries,
                type(last_exc).__name__,
                last_exc,
                method,
                url,
            )
            raise last_exc

        logger.error("Failed to make request after %s attempts. method=%s url=%s", self.__retries, method, url)
        raise RequestsConnectionError("Failed to make request after too many retries")

    def __compute_backoff(self, attempt: int) -> float:
        # Exponential backoff with small jitter to avoid thundering herd.
        base = self.__backoff * (2**attempt)
        jitter = random.uniform(0, max(0.0, self.__backoff))
        return base + jitter

    def make_headers(self) -> Dict[str, Any]:
        headers: Dict[str, Any] = {
            "Content-Type": "application/json",
            "Accept": "application/fhir+json",
        }
        if self.authenticator:
            headers["Authorization"] = self.authenticator.get_authentication_header()

        return headers

    def make_target_url(
        self, sub_route: str | None = None, params: Dict[str, Any] | None = None
    ) -> URL:
        url = self.base_url.rstrip("/")
        if sub_route:
            url = f"{url}/{sub_route.lstrip('/')}"

        target = URL(url)
        if params:
            return target.with_query(params)

        return target
