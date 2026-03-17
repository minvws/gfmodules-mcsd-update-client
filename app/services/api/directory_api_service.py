import logging
from typing import List
from fastapi import HTTPException
from yarl import URL

from app.models.directory.dto import DirectoryDto
from app.services.api.fhir_api import FhirApi
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.endpoint import Endpoint
from fhir.resources.R4B.reference import Reference
from app.services.fhir.references.reference_misc import build_node_reference
from app.services.fhir.utils import get_ura_from_organization

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
            page_directories = self.__parse_bundle(
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

            orgs = self.__parse_bundle(entries)
            if len(orgs) == 0 or orgs[0].id != directory_id:
                    raise HTTPException(
                        status_code=404,
                    detail=f"No organization found with directory_id {directory_id}"
                )
            return orgs[0]

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

    def __parse_bundle(
        self, entries: List[BundleEntry]
    ) -> List[DirectoryDto]:
        orgs: List[DirectoryDto] = []
        try:
            for entry in entries:
                resource = entry.resource
                if isinstance(resource, Organization) and resource.id is not None:
                    try:
                        endpoint_address = self.__get_endpoint_address(
                            resource.id, resource.endpoint , entries
                        )
                    except ValueError as e:
                        logger.error(f"Failed to get endpoint address for Organization {resource.id}: {e}")
                        continue
                    orgs.append(
                        DirectoryDto(
                            id=resource.id,
                            endpoint_address=endpoint_address,
                            ura=get_ura_from_organization(resource),
                        )
                    )
            return orgs
        except Exception as e:
            logger.exception(f"Failed to retrieve directories: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to retrieve directories: {e}"
            )

    def __get_endpoint_address(
        self, org_id: str, endpoint_refs: List[Reference] | None, entries: List[BundleEntry]
    ) -> str:
        if not endpoint_refs or len(endpoint_refs) == 0 or len(endpoint_refs) > 1:
            logger.error(f"Organization {org_id} has no endpoints, skipping")
            raise ValueError("No endpoints found")
        endpoint_ref = endpoint_refs[0]
        if not endpoint_ref.reference:
            logger.error(f"Organization {org_id} has empty endpoint reference, skipping")
            raise ValueError("Empty endpoint reference")
        endpoint_ref_node = build_node_reference(endpoint_ref, self.__provider_url)
        if not endpoint_ref_node or endpoint_ref_node.resource_type != "Endpoint":
            logger.error(f"Organization {org_id} has invalid endpoint reference {endpoint_ref_node}, skipping")
            raise ValueError("Invalid endpoint reference")
        endpoint_res = next((e.resource for e in entries if isinstance(e.resource, Endpoint) and e.resource.id == endpoint_ref_node.id), None)
        if not endpoint_res:
            logger.error(f"Endpoint {endpoint_ref_node.id} not found for Organization {org_id}, skipping")
            raise ValueError("Endpoint not found")
        if not endpoint_res.address:
            logger.error(f"Endpoint {endpoint_ref_node.id} has no address, skipping")
            raise ValueError("Endpoint has no address")
        return str(endpoint_res.address)
