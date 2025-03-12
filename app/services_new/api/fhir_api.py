from datetime import datetime
from enum import Enum
from typing import Any, Dict, List
import logging
from fhir.resources.R4B.bundle import BundleEntry
from yarl import URL
from fhir.resources.R4B.domainresource import DomainResource

from app.services.fhir.bundle.bunlde_factory import create_bundle
from app.services_new.api.api_service import AuthenticationBasedApi
from app.services_new.api.authenticators import Authenticator
from app.services.fhir.fhir_service import FhirService

# from app.models.fhir.r4.types import Bundle

from fhir.resources.R4B.bundle import Bundle

logger = logging.getLogger(__name__)


class McsdResources(Enum):
    ORGANIZATION_AFFILIATION = "OrganizationAffiliation"
    PRACTITIONER_ROLE = "PractitionerRole"
    HEALTH_CARE_SERVICE = "HealthcareService"
    LOCATION = "Location"
    PRACTITIONER = "Practitioner"
    ORGANIZATION = "Organization"
    ENDPOINT = "Endpoint"


class FhirApi(AuthenticationBasedApi):
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

    def aggregate_all_resources_history(
        self, _since: datetime | None = None
    ) -> List[BundleEntry]:
        aggregate: List[BundleEntry] = []
        for res_type in McsdResources:
            print(f"\nfetching data for resource type {res_type.value}\n")
            aggregate.extend(
                self.aggregate_resource_history(
                    resource_type=res_type.value, _since=_since
                )
            )

        return aggregate

    def aggregate_resource_history(
        self,
        resource_type: str,
        resource_id: str | None = None,
        _since: datetime | None = None,
    ) -> List[BundleEntry]:

        params = {"_count": str(self.__request_count)}
        if _since is not None:
            params.update({"_since": _since.isoformat()})

        return self._aggregate_resource_history(
            resource_type=resource_type, resource_id=resource_id, params=params
        )

    def post_bundle(self, bundle: Bundle) -> Dict[str, Any]:
        url = URL(f"{self.__base_url}")
        response = self.do_request("POST", url, bundle.model_dump())
        data: Dict[str, Any] = response.json()

        return data

    def get_resource(
        self, resource_type: str, resource_id: str
    ) -> DomainResource | None:
        try:
            url = URL(f"{self.__base_url}/{resource_type}/{resource_id}")
            response = self.do_request("GET", url)
            data: Dict[str, Any] = response.json()

            return self.__fhir_service.create_resource(data)
        except Exception:
            return None

    def _aggregate_resource_history(
        self,
        resource_type: str,
        resource_id: str | None = None,
        params: dict[str, str] | None = None,
    ) -> List[BundleEntry]:
        history_entries: List[BundleEntry] = []
        next_url: URL | None = (
            URL(f"{self.__base_url}/{resource_type}/{resource_id}/_history").with_query(
                params
            )
            if resource_id is not None
            else URL(f"{self.__base_url}/{resource_type}/_history").with_query(params)
        )

        # Repeat until we have all the history
        while next_url is not None:
            response = self.do_request("GET", next_url)
            if response.status_code > 300:
                logger.warning(
                    f"Failed to get resource history: {response.status_code}"
                )
                break

            # Convert the page result into a page-bundle
            # page_bundle: Bundle = Bundle(**jsonable_encoder(response.json()))
            page_bundle = create_bundle(response.json())

            # Add all the entries to the main bundle
            if page_bundle.entry is not None:
                # history_bundle.entry.extend(page_bundle.entry)
                history_entries.extend(page_bundle.entry)

            # Try and extract the next page URL
            next_url = None
            if page_bundle.link is not None and len(page_bundle.link) > 0:
                for link in page_bundle.link:
                    if link.relation == "next":
                        next_url = URL(link.url)  # type: ignore
        # return history_bundle
        return history_entries
