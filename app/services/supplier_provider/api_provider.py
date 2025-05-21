import logging
from typing import Any, Dict, List
from yarl import URL

from app.models.supplier.dto import SupplierDto
from app.services.api.api_service import ApiService
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.supplier_provider.supplier_provider import SupplierProvider

logger = logging.getLogger(__name__)


class SupplierApiProvider(SupplierProvider):
    def __init__(self, supplier_provider_url: str, api_service: ApiService, supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
        self.__supplier_provider_url = supplier_provider_url
        self.__api_service = api_service
        self.__supplier_ignored_directory_service = supplier_ignored_directory_service

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
                org_map, endpoint_map = self.__parse_bundle(results)

                for org in org_map.values():
                    supplier = self.__create_supplier_dto(org, endpoint_map)
                    if supplier:
                        suppliers.append(supplier)

                url = self.__get_next_page_url(results)

            except Exception as e:
                logger.warning(f"Failed to retrieve suppliers: {e}")
                raise e

        return suppliers


    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        url = URL(
            f"{self.__supplier_provider_url}/fhir/Organization/_search"
        ).with_query({"_id": supplier_id, "_include": "Organization:endpoint"})

        try:
            results = self.__fetch_bundle(url)
            org_map, endpoint_map = self.__parse_bundle(results)

            org = next(iter(org_map.values()), None)
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
            response.raise_for_status()

            results: Dict[str, Any] = response.json()
            entries = results.get("entry", [])
            org = next((e.get("resource") for e in entries if e.get("resource", {}).get("resourceType") == "Organization"), None)

            if org:
                return False  # Exists

            history_url = URL(f"{self.__supplier_provider_url}/fhir/Organization/{supplier_id}/_history")
            history = self.__api_service.do_request("GET", history_url).json()

            for entry in history.get("entry", []):
                if "resource" not in entry and entry.get("request", {}).get("method") == "DELETE":
                    return True

            return False

        except Exception as e:
            logger.warning(f"Check for deleted supplier {supplier_id} failed: {e}")
            return False


    def __fetch_bundle(self, url: URL) -> Any:
        response = self.__api_service.do_request("GET", url)
        response.raise_for_status()
        return response.json()

    def __parse_bundle(self, bundle: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
        org_map: Dict[str, Any] = {}
        endpoint_map: Dict[str, Any] = {}

        for entry in bundle.get("entry", []):
            resource = entry.get("resource", {})
            rtype = resource.get("resourceType")
            if rtype == "Organization":
                org_map[resource["id"]] = resource
            elif rtype == "Endpoint":
                endpoint_map[resource["id"]] = resource

        return org_map, endpoint_map

    def __create_supplier_dto(
        self, org: Dict[str, Any], endpoint_map: Dict[str, Any], supplier_id: str | None = None
    ) -> SupplierDto | None:
        name = org.get("name")
        id = supplier_id or org.get("id")
        ura_number = self.__extract_ura_number(org.get("identifier", []))
        endpoint_address = self.__get_endpoint_address(org, endpoint_map)

        if not all(isinstance(val, str) and val for val in [name, id, ura_number, endpoint_address]):
            logger.warning(f"Invalid supplier data for ID {id}. Skipping.")
            return None

        return SupplierDto(
            id=id, # type:ignore
            ura_number=ura_number, # type:ignore
            name=name, # type:ignore
            endpoint=endpoint_address, # type:ignore
            is_deleted=False,
        )

    def __get_endpoint_address(self, org: Dict[str, Any], endpoint_map: Dict[str, Any]) -> str | None:
        ref = next((r.get("reference") for r in org.get("endpoint", []) if "reference" in r), None)
        if ref:
            endpoint = endpoint_map.get(ref.split("/")[-1])
            return endpoint.get("address") if endpoint else None
        return None

    def __extract_ura_number(self, identifiers: List[Dict[str, Any]]) -> str | None:
        return next((i.get("value") for i in identifiers if i.get("system") == "http://fhir.nl/fhir/NamingSystem/ura"), None)

    def __get_next_page_url(self, bundle: Dict[str, Any]) -> URL | None:
        for link in bundle.get("link", []):
            if link.get("relation") == "next":
                return URL(link["url"])
        return None
