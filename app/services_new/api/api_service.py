from abc import ABC, abstractmethod
import logging
import time
from typing import Dict, Any
import requests
from yarl import URL

from app.services_new.api.authenticators import Authenticator

logger = logging.getLogger(__name__)


class RequestService(ABC):
    def __init__(self, timeout: int, backoff: float, retries: int):
        self.__timeout = timeout
        self.__backoff = backoff
        self.__retries = retries

    @property
    def retries(self) -> int:
        return self.__retries

    @retries.setter
    def retries(self, count: int) -> None:
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
        raise NotImplementedError


class ApiService(RequestService, ABC):
    def __init__(self, timeout: int, backoff: float, retries: int):
        super().__init__(timeout, backoff, retries)

    def do_request(
        self, method: str, url: URL, json: Dict[str, Any] | None = None
    ) -> requests.Response:

        headers = {"Content-Type": "application/json"}

        for attempt in range(self.retries):
            try:
                logger.info(f"Making HTTP {method} request to {url}")
                response = requests.request(
                    method=method,
                    url=str(url.with_query(None)),
                    params=url.query,
                    headers=headers,
                    timeout=self.timeout,
                    json=json,
                )
                return response
            except (
                ConnectionError,
                TimeoutError,
                requests.exceptions.RequestException,
            ):
                logger.warning(f"Failed to make request to {url} on attempt {attempt}")
                if attempt < self.retries - 1:
                    logger.info(f"Retrying in {self.backoff * (2**attempt)} seconds")
                    time.sleep(self.backoff * (2**attempt))

        logger.error(f"Failed to make request to {url} after {self.retries} attempts")
        raise Exception("Failed to make request after too many retries")


class AuthenticationBasedApi(RequestService, ABC):
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
        headers = {"Content-Type": "application/json"}
        auth_header = self.auth.get_authentication_header()
        if auth_header:
            headers["Authorization"] = auth_header

        for attempt in range(self.retries):
            try:
                logger.info(f"Making HTTP {method} request to {url}")
                response = requests.request(
                    method=method,
                    url=str(url.with_query(None)),
                    params=url.query,
                    headers=headers,
                    json=json,
                    timeout=self.timeout,
                    auth=self.auth.get_auth(),
                )
                return response
            except (
                ConnectionError,
                TimeoutError,
                requests.exceptions.RequestException,
            ):
                logger.warning(f"Failed to make request to {url} on attempt {attempt}")
                if attempt < self.retries - 1:
                    logger.info(f"Retrying in {self.backoff * (2 ** attempt)} seconds")
                    time.sleep(self.backoff * (2**attempt))

        logger.error(f"Failed to make request to {url} after {self.retries} attempts")
        raise Exception("Failed to make request after too many retries")
