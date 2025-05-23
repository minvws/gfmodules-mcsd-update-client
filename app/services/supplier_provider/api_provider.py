import logging
from typing import Any, Dict, List
from yarl import URL

from app.models.supplier.dto import SupplierDto
from app.services.api.api_service import ApiService
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.fhir.fhir_service import FhirService
from app.services.supplier_provider.supplier_provider import SupplierProvider
from fhir.resources.R4B.bundle import Bundle
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.endpoint import Endpoint

logger = logging.getLogger(__name__)


class SupplierApiProvider(SupplierProvider):
    def __init__(self, supplier_provider_url: str, api_service: ApiService, supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
        self.__supplier_provider_url = supplier_provider_url
        self.__api_service = api_service
        self.__fhir_service = FhirService(strict_validation=False)

    def get_all_suppliers(self, include_ignored: bool = False) -> List[SupplierDto]:
        return self.__fetch_suppliers(include_ignored=include_ignored, include_ignored_ids=None)


    def get_all_suppliers_include_ignored(self, include_ignored_ids: List[str]) -> List[SupplierDto]:
        return self.__fetch_suppliers(include_ignored=False, include_ignored_ids=include_ignored_ids)


    def __fetch_suppliers(
        self,
        include_ignored: bool,
        include_ignored_ids: List[str] | None = None
    ) -> List[SupplierDto]:
        suppliers: List[SupplierDto] = []
        url: URL | None = URL(
            f"{self.__supplier_provider_url}/fhir/Organization"
        ).with_query({"_include": "Organization:endpoint"})

        while url:
            try:
                results = self.__fetch_bundle(url)
                orgs, endpoint_map = self.__parse_bundle(results)

                for org in orgs:
                    supplier = self.__create_supplier_dto(org, endpoint_map)
                    if supplier:
                        suppliers.append(supplier)

                url = self.__get_next_page_url(results)

            except Exception as e:
                logger.exception(f"Failed to retrieve suppliers: {e}")
                raise e

        return suppliers


    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        url = URL(
            f"{self.__supplier_provider_url}/fhir/Organization/_search"
        ).with_query({"_id": supplier_id, "_include": "Organization:endpoint"})

        try:
            results = self.__fetch_bundle(url)
            orgs, endpoint_map = self.__parse_bundle(results)

            org = next(iter(orgs), None)
            if not org:
                raise ValueError(f"No organization found with supplier_id {supplier_id}")

            supplier = self.__create_supplier_dto(org, endpoint_map, supplier_id)
            if not supplier:
                raise ValueError(f"Invalid organization data for supplier_id {supplier_id}")

            return supplier

        except Exception as e:
            logger.warning(f"Failed to retrieve supplier {supplier_id}: {e}")
            raise e

    def check_if_supplier_is_deleted(self, supplier_id: str) -> bool:
        try:
            org_url = URL(f"{self.__supplier_provider_url}/fhir/Organization/{supplier_id}")
            response = self.__api_service.do_request("GET", org_url)
            if response.status_code == 410:
                return True
            response.raise_for_status()

            bundle = self.__fhir_service.create_bundle(response.json())
            org = next((e.resource for e in bundle.entry if e.resource and isinstance(e.resource, Organization)), None)

            if org:
                return False  # Exists

            history_url = URL(f"{self.__supplier_provider_url}/fhir/Organization/{supplier_id}/_history")
            response = self.__api_service.do_request("GET", history_url)
            response.raise_for_status()
            history = self.__fhir_service.create_bundle(response.json())

            for entry in history.entry:
                if entry.resource is not None and entry.request and entry.request.method == "DELETE":
                    return True

            return False

        except Exception as e:
            logger.warning(f"Check for deleted supplier {supplier_id} failed: {e}")
            return False


    def __fetch_bundle(self, url: URL) -> Bundle:
        response = self.__api_service.do_request("GET", url)
        response.raise_for_status()
        return self.__fhir_service.create_bundle(response.json())

    def __parse_bundle(self, bundle: Bundle) -> tuple[List[Organization], Dict[str, Endpoint]]:
        orgs: List[Organization] = []
        endpoint_map: Dict[str, Any] = {}

        for entry in bundle.entry if bundle.entry else []:
            resource = entry.resource
            if isinstance(resource, Organization):
                orgs.append(resource)
            elif isinstance(resource, Endpoint):
                endpoint_map[resource.id] = resource

        return orgs, endpoint_map

    def __create_supplier_dto(
        self, org: Organization, endpoint_map: Dict[str, Endpoint], supplier_id: str | None = None
    ) -> SupplierDto | None:
        name = org.name
        id = supplier_id or org.id
        endpoint_address = self.__get_endpoint_address(org, endpoint_map)

        if not all(isinstance(val, str) and val for val in [name, id, endpoint_address]):
            logger.warning(f"Invalid supplier data for ID {id}. Skipping.")
            return None

        return SupplierDto(
            id=id,
            name=name,
            endpoint=endpoint_address, # type:ignore
            is_deleted=False,
        )

    def __get_endpoint_address(self, org: Organization, endpoint_map: Dict[str, Endpoint]) -> str | None:
        if org.endpoint is None:
            return None
        ref = next((r.get("reference") for r in org.endpoint), None)
        if ref:
            endpoint = endpoint_map.get(ref.split("/")[-1])
            return endpoint.address if endpoint else None
        return None

    def __get_next_page_url(self, bundle: Bundle) -> URL | None:
        if bundle.link is None:
            return None
        for link in bundle.link:
            if link.relation == "next":
                return URL(link.url)
        return None
