from datetime import datetime
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from typing import Any, Dict, List
import logging
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.domainresource import DomainResource
from requests import JSONDecodeError
from yarl import URL

from app.models.fhir.types import BundleError
from app.services.fhir.bundle.utils import filter_history_entries
from app.services.api.api_service import HttpService
from app.services.api.authenticators.authenticator import Authenticator
from app.services.fhir.capability_statement_validator import is_capability_statement_valid
from app.services.fhir.fhir_service import FhirService

from fhir.resources.R4B.bundle import Bundle

from app.services.fhir.utils import collect_errors

ERR_MSG_FORMAT = "FHIR API error: %s"
HTTP_ERR_MSG = "An error occurred while processing a FHIR request."
logger = logging.getLogger(__name__)


class FhirApi(HttpService):
    def __init__(
        self,
        base_url: str,
        timeout: int,
        backoff: float,
        auth: Authenticator,
        request_count: int,
        fill_required_fields: bool,
        retries: int,
        mtls_cert: str | None = None,
        mtls_key: str | None = None,
        mtls_ca: str | None = None,
    ):
        super().__init__(
            base_url=base_url,
            timeout=timeout,
            backoff=backoff,
            retries=retries,
            authenticator=auth,
            mtls_ca=mtls_ca,
            mtls_cert=mtls_cert,
            mtls_key=mtls_key,
        )
        self.request_count = request_count
        self.__fhir_service = FhirService(fill_required_fields)

    def post_bundle(self, bundle: Bundle) -> tuple[Bundle, list[BundleError]]:
        """
        Post a FHIR bundle to the server and return the response as a Bundle object.
        Will return a tuple containing a parsed Bundle and BundleErrors if present
        """
        try:
            response = self.do_request(
                "POST", json=jsonable_encoder(bundle.model_dump())
            )
        except Exception as e:
            logger.error(ERR_MSG_FORMAT.format(e))
            raise HTTPException(status_code=500, detail=str(e))

        # Make sure we have a valid HTTP response status
        if response.status_code >= 400:
            # See if we can get an Operation Outcome from the error response
            try:
                data = response.json()
            except JSONDecodeError:
                # No json data found, just return generic error
                logger.error(ERR_MSG_FORMAT.format(response.text))
                raise HTTPException(status_code=500, detail=HTTP_ERR_MSG)

            # Return a global error
            logger.error(ERR_MSG_FORMAT.format(data))
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        # Successful HTTP status. Check if we have a JSON body
        try:
            data = response.json()
        except JSONDecodeError:
            logger.error("Failed to decode JSON response: %s", response.text)
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        bundle = self.__fhir_service.create_bundle(data)
        bundle_errors = collect_errors(bundle)

        return bundle, bundle_errors

    def search_resource(
        self, resource_type: str, params: dict[str, Any]
    ) -> tuple[URL | None, List[BundleEntry]]:
        """
        Search for resources of a given type with specified parameters. Will return a tuple containing the next URL (if available)
        and a list of BundleEntry objects. If the next url is empty, there are no more pages to fetch
        """
        response = self.do_request(
            method="GET", sub_route=f"{resource_type}/_search", params=params
        )
        if response.status_code > 300:
            logger.error(
                f"An error with status code {response.status_code} has occurred from server. See response:\n{response.json()}"
            )
            raise HTTPException(status_code=500, detail=response.json())

        page_bundle = self.__fhir_service.create_bundle(response.json())
        next_url = None
        entries = page_bundle.entry if page_bundle.entry else []
        if page_bundle.link is not None and len(page_bundle.link) > 0:
            for link in page_bundle.link:
                if link.relation == "next":  # type: ignore[attr-defined]
                    next_url = URL(link.url)  # type: ignore[attr-defined]

        return next_url, entries

    def get_resource_by_id(
        self, resource_type: str, resource_id: str
    ) -> DomainResource:
        response = self.do_request(
            method="GET", sub_route=f"{resource_type}/{resource_id}"
        )
        if response.status_code == 410:
            logger.warning(
                f"Resource {resource_type}/{resource_id} has been deleted from the server."
            )
            raise HTTPException(status_code=410, detail="Resource has been deleted.")

        if response.status_code > 300:
            logger.error(
                f"An error with status code {response.status_code} has occurred from server. See response:\n{response.json()}"
            )
            raise HTTPException(status_code=500, detail=response.json())

        data = response.json()
        return self.__fhir_service.create_resource(data)

    def build_history_params(self, since: datetime | None = None) -> Dict[str, Any]:
        """
        Builds the params based on request_count and _since parameter.
        """
        params: Dict[str, int | str] = {"_count": str(self.request_count)}
        if since:
            params["_since"] = since.isoformat()

        return params

    def validate_capability_statement(self) -> bool:
        """
        Fetch the FHIR CapabilityStatement from the server.
        """
        response = self.do_request("GET", sub_route="metadata")
        if response.status_code > 300:
            logger.error(
                f"An error with status code {response.status_code} has occurred from server. See response:\n{response.json()}"
            )
            raise HTTPException(status_code=500, detail=response.json())

        try:
            data = response.json()
        except JSONDecodeError:
            logger.error("Failed to decode JSON response: %s", response.text)
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)
        return is_capability_statement_valid(data)

    @staticmethod
    def get_next_params(url: URL) -> Dict[str, Any]:
        """
        Helper function to extract query parameter from URL.
        """
        query = url.query
        return dict(query)

    def get_history_batch(
        self,
        resource_type: str,
        next_params: Dict[str, Any] | None = None,
    ) -> tuple[Dict[str, Any] | None, List[BundleEntry]]:
        """
        Fetch a batch of resource history entries from the given URL. Will return a tuple containing the next parameter (if available)
        and a list of BundleEntry objects. If the next params is empty, there are no more pages to fetch
        """
        response = self.do_request(
            "GET", sub_route=f"{resource_type}/_history", params=next_params
        )
        if response.status_code > 300:
            logger.error(
                f"An error with status code {response.status_code} has occurred from server. See response:\n{response.json()}"
            )
            raise HTTPException(status_code=500, detail=response.json())

        page_bundle = self.__fhir_service.create_bundle(response.json())
        next_params = None
        entries = filter_history_entries(page_bundle.entry) if page_bundle.entry else []
        if page_bundle.link is not None and len(page_bundle.link) > 0:
            for link in page_bundle.link:
                if link.relation == "next":  # type: ignore[attr-defined]
                    next_params = self.get_next_params(URL(link.url))  # type: ignore[attr-defined]

        return next_params, entries
