import logging
import time
from typing import Any, Dict
from fastapi.encoders import jsonable_encoder
import requests
from yarl import URL

from app.models.fhir.r4.types import Bundle
from app.services.request_services.Authenticators import Authenticator

logger = logging.getLogger(__name__)


class FhirRequestService:
    def __init__(self, timeout: int, backoff: float, auth: Authenticator) -> None:
        # Maximum timeout for a complete request in seconds
        self.timeout = timeout
        # Backoff base time in seconds when a request fails
        self.backoff = backoff
        # Authentication system if required
        self.auth = auth
        # Number of retries for a request
        self.retry_count = 5

    def _do_request(
        self, method: str, url: URL, json: Dict[str, Any] | None = None
    ) -> requests.Response:
        headers = {"Content-Type": "application/json"}
        auth_header = self.auth.get_authentication_header()
        if auth_header:
            headers["Authorization"] = auth_header

        for attempt in range(self.retry_count):
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
                if attempt < self.retry_count - 1:
                    logger.info(f"Retrying in {self.backoff * (2 ** attempt)} seconds")
                    time.sleep(self.backoff * (2**attempt))

        logger.error(
            f"Failed to make request to {url} after {self.retry_count} attempts"
        )
        raise Exception("Failed to make request after too many retries")

    def get_resource_history(
        self,
        base_url: str,
        resource_type: str,
        resource_id: str | None = None,
        params: dict[str, str] | None = None,
    ) -> Bundle:
        history_bundle = Bundle(type="history", entry=[])

        next_url: URL | None = (
            URL(f"{base_url}/{resource_type}/{resource_id}/_history").with_query(params)
            if resource_id is not None
            else URL(f"{base_url}/{resource_type}/_history").with_query(params)
        )

        # Repeat until we have all the history
        while next_url is not None:
            response = self._do_request("GET", next_url)
            if response.status_code > 300:
                logger.warning(
                    f"Failed to get resource history: {response.status_code}"
                )
                break

            # Convert the page result into a page-bundle
            page_bundle: Bundle = Bundle(**jsonable_encoder(response.json()))

            # Add all the entries to the main bundle
            if page_bundle.entry is not None:
                history_bundle.entry.extend(page_bundle.entry)

            # Try and extract the next page URL
            next_url = None
            if page_bundle.link is not None and len(page_bundle.link) > 0:
                for link in page_bundle.link:
                    if link.relation == "next":
                        next_url = URL(link.url)  # type: ignore

        return history_bundle

    def post_bundle(self, base_url: str, bundle: Dict[str, Any]) -> Dict[str, Any]:
        url = URL(f"{base_url}")
        response = self._do_request("POST", url, bundle)
        data: Dict[str, Any] = response.json()

        return data
