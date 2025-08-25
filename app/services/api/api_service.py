from abc import ABC, abstractmethod
import logging
import time
from typing import Dict, Any
import requests
from requests.exceptions import Timeout, ConnectionError
from yarl import URL

from app.services.api.authenticators.authenticator import Authenticator

logger = logging.getLogger(__name__)


class RequestService(ABC):
    """
    Abstract base class for making HTTP requests with retry logic.
    """
    def __init__(self, timeout: int, backoff: float, retries: int):
        self.__timeout = timeout
        self.__backoff = backoff
        self.__retries = retries

    @property
    def retries(self) -> int:
        """
        Number of retries for failed requests.
        """
        return self.__retries

    @retries.setter
    def retries(self, count: int) -> None:
        """
        Set the number of retries for failed requests.
        """
        self.__retries = count

    @property
    def backoff(self) -> float:
        return self.__backoff

    @backoff.setter
    def backoff(self, backoff: float) -> None:
        self.__backoff = backoff

    @property
    def timeout(self) -> int:
        return self.__timeout

    @timeout.setter
    def timeout(self, time: int) -> None:
        self.__timeout = time

    @abstractmethod
    def do_request(
        self, method: str, url: URL, json: Dict[str, Any] | None = None
    ) -> requests.Response:
        """
        Perform an HTTP request.
        """
        raise NotImplementedError


class ApiService(RequestService, ABC):
    def __init__(self, timeout: int, backoff: float, retries: int):
        super().__init__(timeout, backoff, retries)

    def do_request(
        self, method: str, url: URL, json: Dict[str, Any] | None = None
    ) -> requests.Response:
        # We always assume application/json as the content type
        headers = {"Content-Type": "application/json"}

        for attempt in range(self.retries):
            try:
                logger.info(f"Making HTTP {method} request to {url}")
                response = requests.request(
                    method=method,
                    url=str(url.with_query(None)),
                    params=url.query,  # type: ignore
                    headers=headers,
                    timeout=self.timeout,
                    json=json,
                )
                return response
            except (
                ConnectionError,
                Timeout,
            ):
                logger.warning(f"Failed to make request to {url} on attempt {attempt}")

                # Connection error or timeout, we can retry with an exponential backoff until
                # we reach the max retries
                if attempt < self.retries - 1:
                    logger.info(f"Retrying in {self.backoff * (2**attempt)} seconds")
                    time.sleep(self.backoff * (2**attempt))

        logger.error(f"Failed to make request to {url} after {self.retries} attempts")
        raise Exception("Failed to make request after too many retries")


class AuthenticationBasedApiService(RequestService, ABC):
    """
    API service that uses an Authenticator for authenticated requests.
    """
    def __init__(self, auth: Authenticator, timeout: int, backoff: float, retries: int):
        super().__init__(timeout, backoff, retries)
        self.__auth = auth

    @property
    def auth(self) -> Authenticator:
        return self.__auth

    @auth.setter
    def auth(self, auth: Authenticator) -> None:
        self.__auth = auth

    def do_request(
        self, method: str, url: URL, json: Dict[str, Any] | None = None
    ) -> requests.Response:
        """
        Perform an HTTP request with authentication. Note that authentication can either be done
        by a simple Authentication header, or by passing an auth object to the requests library.
        This should cover most of the common authentication mechanisms.
        """
        headers = {"Content-Type": "application/json"}

        # Add authentication header if available
        auth_header = self.auth.get_authentication_header()
        if auth_header:
            headers["Authorization"] = auth_header

        for attempt in range(self.retries):
            try:
                logger.info(f"Making HTTP {method} request to {url}")
                response = requests.request(
                    method=method,
                    url=str(url.with_query(None)),
                    params=url.query,  # type: ignore
                    headers=headers,
                    json=json,
                    timeout=self.timeout,
                    # Pass the auth object from the authenticator if available
                    auth=self.auth.get_auth(),
                )
                return response
            except (
                ConnectionError,
                Timeout,
            ):
                logger.warning(f"Failed to make request to {url} on attempt {attempt}")
                if attempt < self.retries - 1:
                    logger.info(f"Retrying in {self.backoff * (2 ** attempt)} seconds")
                    time.sleep(self.backoff * (2**attempt))

        logger.error(f"Failed to make request to {url} after {self.retries} attempts")
        raise Exception("Failed to make request after too many retries")
