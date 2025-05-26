import logging
from typing import Any, Dict, List
from fastapi import HTTPException
from yarl import URL

from app.models.supplier.dto import SupplierDto
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.api.fhir_api import FhirApi
from app.services.fhir.fhir_service import FhirService
from app.services.supplier_provider.supplier_provider import SupplierProvider
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.endpoint import Endpoint

logger = logging.getLogger(__name__)


class SupplierApiProvider(SupplierProvider):
    def __init__(self, fhir_api: FhirApi, supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
        self.__fhir_api = fhir_api
        self.__fhir_service = FhirService(strict_validation=False)
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
        params = {
            "_include": "Organization:endpoint",
        }
        next_url: URL | None = URL("")
        while next_url is not None:
            next_url, entries = self.__fhir_api.search_resource(
                resource_type="Organization",
                params=params,
            )
            try:
                orgs, endpoint_map = self.__parse_bundle(entries)

                for org in orgs:
                    supplier = self.__create_supplier_dto(org, endpoint_map)
                    if supplier:
                        suppliers.append(supplier)
            except Exception as e:
                logger.exception(f"Failed to retrieve suppliers: {e}")
                raise e

        return suppliers


    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        params = {
            "_id": supplier_id,
            "_include": "Organization:endpoint",
        }
        try:
            next_url, entries = self.__fhir_api.search_resource(
                resource_type="Organization",
                params=params,
            )
            orgs, endpoint_map = self.__parse_bundle(entries)

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
            _org = self.__fhir_api.get_resource_by_id(
                resource_type="Organization",
                resource_id=supplier_id,
            )
            return False # Organization resource exists, so supplier is not deleted
        except HTTPException as e:
            if e.status_code == 410:
                return True
            logger.warning(f"Check for deleted supplier {supplier_id} failed: {e}")
            return False
        except Exception as e:
            logger.warning(f"Check for deleted supplier {supplier_id} failed: {e}")
            return False

    def __parse_bundle(self, entries: BundleEntry) -> tuple[List[Organization], Dict[str, Endpoint]]:
        orgs: List[Organization] = []
        endpoint_map: Dict[str, Any] = {}

        for entry in entries:
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
        ref = self.__fhir_service.get_references(org) if org.endpoint else None
        if ref:
            first_ref = ref[0]
            if not first_ref.reference.startswith("Endpoint/"):
                logger.warning(f"Unexpected reference format: {first_ref.reference}")
                return None
            endpoint = endpoint_map.get(first_ref.reference.split("/")[-1])
            return str(endpoint.address) if endpoint else None
        return None