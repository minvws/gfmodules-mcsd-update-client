import logging
import time
from collections.abc import Sequence
from typing import Dict, Any

import requests
from yarl import URL

from app.services.entity_services.supplier_service import SupplierApi
from app.models.supplier.dto import SupplierDto

logger = logging.getLogger(__name__)


class HttpApi(SupplierApi):
    def __init__(self, base_url: str, timeout: int, backoff: float) -> None:
        self.base_url = base_url
        self.timeout = timeout
        self.backoff = backoff
        self.retry_count = 5

    def get_one(self, supplier_id: str) -> SupplierDto:
        url = URL(f"{self.base_url}/api/supplier/{supplier_id}")
        response = self._do_request("GET", url)
        if response.status_code > 300:
            raise Exception(response.json())

        results: Dict[str, Any] = response.json()
        return SupplierDto(
            id=results["id"],
            name=results["name"],
            endpoint=results["endpoint"],
        )

    def get_all(self) -> Sequence[SupplierDto]:
        suppliers = []

        url = URL(f"{self.base_url}/api/suppliers")
        while True:
            response = self._do_request("GET", url)
            if response.status_code > 300:
                raise Exception(response.json())

            results: Dict[str, Any] = response.json()

            for result in results["data"]:
                suppliers.append(
                    SupplierDto(
                        id=result["id"],
                        name=result["name"],
                        endpoint=result["endpoint"],
                    )
                )

            if results["next_page_url"] is None:
                break

            url = URL(results["next_page_url"])

        return suppliers

    def _do_request(self, method: str, url: URL) -> requests.Response:
        headers = {"Content-Type": "application/json"}

        for attempt in range(self.retry_count):
            try:
                logger.info(f"Making HTTP {method} request to {url}")
                response = requests.request(
                    method=method,
                    url=str(url.with_query(None)),
                    params=url.query,
                    headers=headers,
                    timeout=self.timeout,
                )
                return response
            except (
                ConnectionError,
                TimeoutError,
                requests.exceptions.RequestException,
            ):
                logger.warning(f"Failed to make request to {url} on attempt {attempt}")
                if attempt < self.retry_count - 1:
                    logger.info(f"Retrying in {self.backoff * (2 ** attempt)} seconds")
                    time.sleep(self.backoff * (2**attempt))

        logger.error(
            f"Failed to make request to {url} after {self.retry_count} attempts"
        )
        raise Exception("Failed to make request after too many retries")
