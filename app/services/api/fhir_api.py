from datetime import datetime
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from typing import List
import logging
from fhir.resources.R4B.bundle import BundleEntry
from yarl import URL
from app.services.fhir.bundle.bundle_utils import filter_history_entries
from app.services.api.api_service import AuthenticationBasedApiService
from app.services.api.authenticators.authenticator import Authenticator
from app.services.fhir.fhir_service import FhirService

from fhir.resources.R4B.bundle import Bundle

logger = logging.getLogger(__name__)


class FhirApi(AuthenticationBasedApiService):
    def __init__(
        self,
        timeout: int,
        backoff: float,
        auth: Authenticator,
        url: str,
        request_count: int,
        strict_validation: bool,
    ):
        super().__init__(timeout=timeout, backoff=backoff, auth=auth, retries=5)
        self.__base_url = url
        self.__request_count = request_count
        self.__fhir_service = FhirService(strict_validation=strict_validation)

    def post_bundle(self, bundle: Bundle) -> Bundle:
        try:
            url = URL(f"{self.__base_url}")
            response = self.do_request(
                "POST", url, jsonable_encoder(bundle.model_dump())
            )
            if response.status_code > 300:
                logger.error(response.json())
                raise HTTPException(status_code=500, detail=response.json())
            data = response.json()
            return self.__fhir_service.create_bundle(data)
        except Exception as e:
            logging.error(e)
            raise e

    def build_base_history_url(
        self, resource_type: str, since: datetime | None = None
    ) -> URL:
        params: dict[str, int | str] = {"_count": self.__request_count}
        if since is not None:
            params["_since"] = since.isoformat()

        url = URL(f"{self.__base_url}/{resource_type}/_history").with_query(params)
        return url

    def get_history_batch(
        self,
        url: URL,
    ) -> tuple[URL | None, List[BundleEntry]]:
        response = self.do_request("GET", url)
        if response.status_code > 300:
            logger.error(
                f"An error with status code {response.status_code} has occurred from server. See response:\n{response.json()}"
            )
            raise HTTPException(status_code=500, detail=response.json())

        page_bundle = self.__fhir_service.create_bundle(response.json())
        next_url = None
        entries = filter_history_entries(page_bundle.entry) if page_bundle.entry else []
        if page_bundle.link is not None and len(page_bundle.link) > 0:
            for link in page_bundle.link:
                if link.relation == "next":
                    next_url = URL(link.url)

        return next_url, entries
