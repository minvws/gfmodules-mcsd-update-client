import logging
from typing import Any, Dict, List
from fastapi import HTTPException
from yarl import URL

from app.models.directory.dto import DirectoryDto
from app.services.api.fhir_api import FhirApi
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.endpoint import Endpoint
from app.services.fhir.references.reference_misc import build_node_reference

logger = logging.getLogger(__name__)


class DirectoryApiService:
    def __init__(
        self,
        fhir_api: FhirApi,
        provider_url: str
    ) -> None:
        """
        Service to manage directories from a FHIR-based API.
        """
        self.__fhir_api = fhir_api
        self.__provider_url = provider_url

    def fetch_directories(self) -> List[DirectoryDto]:
        directories: List[DirectoryDto] = []
        next_url: URL | None = URL("")
        while next_url is not None:
            params = {"_include": "Organization:endpoint"}
            next_url, entries = self.__fhir_api.search_resource(
                resource_type="Organization",
                params=params,
            )
            page_directories = self.__process_organizations_page(
                entries
            )
            directories.extend(page_directories)

        return directories



    def fetch_one_directory(self, directory_id: str) -> DirectoryDto:
        params = {
            "_id": directory_id,
            "_include": "Organization:endpoint",
        }
        try:
            _, entries = self.__fhir_api.search_resource(
                resource_type="Organization",
                params=params,
            )

            orgs, endpoint_map = self.__parse_bundle(entries)

            org = next(iter(orgs), None)
            if not org:
                    raise HTTPException(
                        status_code=404,
                    detail=f"No organization found with directory_id {directory_id}"
                )

            directory = self.__create_directory_dto(org, endpoint_map, directory_id)
            if not directory:
                raise HTTPException(
                    status_code=404,
                    detail=f"Invalid organization data for directory_id {directory_id}"
                )

            return directory

        except Exception as e:
            logger.warning(f"Failed to retrieve directory {directory_id}: {e}")
            raise HTTPException(
                status_code=404,
                detail=f"Failed to retrieve directory with id {directory_id}: {e}"
            )

    def check_if_directory_is_deleted(self, directory_id: str) -> bool:
        try:
            _org = self.__fhir_api.get_resource_by_id(
                resource_type="Organization",
                resource_id=directory_id,
            )
            return False  # Organization resource exists, so directory is not deleted
        except HTTPException as e:
            if e.status_code == 410:  # 410 Gone indicates the resource has been deleted
                return True
            logger.warning(f"Check for deleted directory {directory_id} failed: {e}")
            return False
        except Exception as e:
            logger.warning(f"Check for deleted directory {directory_id} failed: {e}")
            return False

    def __process_organizations_page(
        self,
        entries: List[BundleEntry],
    ) -> List[DirectoryDto]:
        directories: List[DirectoryDto] = []
        try:
            orgs, endpoint_map = self.__parse_bundle(entries)
            for org in orgs:
                directory = self.__create_directory_dto(org, endpoint_map)
                if not directory:
                    continue
                directories.append(directory)
            return directories
        except Exception as e:
            logger.exception(f"Failed to retrieve directories: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to retrieve directories: {e}"
            )

    def __parse_bundle(
        self, entries: List[BundleEntry]
    ) -> tuple[List[Organization], Dict[str, Endpoint]]:
        orgs: List[Organization] = []
        endpoint_map: Dict[str, Any] = {}

        for entry in entries:
            resource = entry.resource
            if isinstance(resource, Organization):
                orgs.append(resource)
            elif isinstance(resource, Endpoint):
                endpoint_map[resource.id] = resource

        return orgs, endpoint_map

    def __create_directory_dto(
        self,
        org: Organization,
        endpoint_map: Dict[str, Endpoint],
        directory_id: str | None = None,
    ) -> DirectoryDto | None:
        _id = directory_id or org.id
        if _id is None:
            logger.error("Organization resource missing id")
            raise ValueError("Organization resource missing id")
        endpoint_address = self.__get_endpoint_address(org, endpoint_map)

        return DirectoryDto(
            id=_id,
            endpoint_address=endpoint_address,
        )

    def __get_endpoint_address(self, org: Organization, endpoint_map: Dict[str, Endpoint]) -> str:
        if org.endpoint is None:
            logger.error(f"Organization {org.id} has no endpoints.")
            raise ValueError(f"Organization {org.id} has no endpoints.")

        if len(org.endpoint) > 1:
            logger.error(f"Organization {org.id} has multiple endpoints")
            raise ValueError(f"Organization {org.id} has multiple endpoints: {org.endpoint}")

        ref = org.endpoint[0]

        node_ref = build_node_reference(ref, self.__provider_url)
        if node_ref is None:
            logger.warning(f"Unexpected reference format: {ref.reference}")
            raise ValueError(f"Unexpected reference format: {ref.reference}")

        if node_ref.resource_type != "Endpoint":
            logger.warning(f"Unexpected reference format: {node_ref.resource_type}")
            raise ValueError(f"Unexpected reference format: {node_ref.resource_type}")

        endpoint = endpoint_map.get(node_ref.id)

        if endpoint is None:
            logger.error(f"Endpoint {node_ref.id} not found in included resources")
            raise ValueError(f"Endpoint {node_ref.id} not found in included resources")
        
        if endpoint.address is None:
            logger.error(f"No endpoint address for Endpoint {node_ref.id}")
            raise ValueError(f"No endpoint address for Endpoint {node_ref.id}")
        return str(endpoint.address)
