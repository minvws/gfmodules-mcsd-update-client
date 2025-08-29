from abc import ABC
import logging
import time
from typing import Dict, Any
from requests import request, Response
from requests.exceptions import Timeout, ConnectionError
from yarl import URL

from app.services.api.authenticators.authenticator import Authenticator

logger = logging.getLogger(__name__)


class HttpService(ABC):
    """
    Base class for making HTTP requests with retry logic
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
        mtls_ca: str | None = None,
    ) -> None:
        self.base_url = base_url
        self.authenticator = authenticator
        self.__mtls_cert = mtls_cert
        self.__mtls_key = mtls_key
        self.__mtls_ca = mtls_ca
        self.__timeout = timeout
        self.__retries = retries
        self.__backoff = backoff

    def do_request(
        self,
        method: str,
        sub_route: str | None = None,
        json: Dict[str, Any] | None = None,
        params: Dict[str, Any] | None = None,
    ) -> Response:
        """
        Perform an HTTP request.
        """
        headers = self.make_headers()
        url = self.make_target_url(sub_route, params)

        for attempt in range(self.__retries):
            try:
                logger.info(f"Making HTTP {method} request to {url}")
                response = request(
                    method=method,
                    url=str(url),
                    headers=headers,
                    timeout=self.__timeout,
                    json=json,
                    cert=(
                        (self.__mtls_cert, self.__mtls_key)
                        if self.__mtls_cert and self.__mtls_key
                        else None
                    ),
                    verify=self.__mtls_ca or True,
                    auth=self.authenticator.get_auth() if self.authenticator else None,
                )
                return response
            except (
                ConnectionError,
                Timeout,
            ):
                logger.warning(f"Failed to make request to {url} on attempt {attempt}")

                # Connection error or timeout, we can retry with an exponential backoff until
                # we reach the max retries
                if attempt < self.__retries - 1:
                    logger.info(f"Retrying in {self.__backoff * (2**attempt)} seconds")
                    time.sleep(self.__backoff * (2**attempt))

        logger.error(f"Failed to make request to {url} after {self.__retries} attempts")
        raise ConnectionError("Failed to make request after too many retries")

    def make_headers(self) -> Dict[str, Any]:
        # We always assume application/json as the content type
        headers = {"Content-Type": "application/json"}
        if self.authenticator:
            headers["Authorization"] = self.authenticator.get_authentication_header()

        return headers

    def make_target_url(
        self, sub_route: str | None = None, params: Dict[str, Any] | None = None
    ) -> URL:
        url = self.base_url
        if sub_route:
            url = f"{url}/{sub_route}"

        target = URL(url)
        if params:
            return target.with_query(params)

        return target
