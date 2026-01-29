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
from app.services.fhir.utils import ID_SYSTEM_URA, get_ura_from_organization

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
        params = {"_include": "Organization:endpoint"}
        saw_org = False
        saw_org_with_endpoint_ref = False
        try:
            next_url, entries = self.__fhir_api.search_resource(
                resource_type="Organization",
                params=params,
            )
        except HTTPException as exc:
            logger.warning(
                "Organization search failed. Falling back to Endpoint search. status=%s",
                exc.status_code,
            )
            return self.__fetch_directories_from_endpoints()

        saw_org, saw_org_with_endpoint_ref = self.__update_org_flags(
            entries, saw_org, saw_org_with_endpoint_ref
        )
        directories.extend(self.__parse_bundle(entries))
        while next_url is not None:
            try:
                next_url, entries = self.__fhir_api.search_resource_page(next_url)
            except HTTPException as exc:
                if self.__is_pagination_not_found(exc, next_url):
                    logger.warning(
                        "Pagination link not found or expired; stopping pagination. url=%s",
                        next_url,
                    )
                    break
                raise
            saw_org, saw_org_with_endpoint_ref = self.__update_org_flags(
                entries, saw_org, saw_org_with_endpoint_ref
            )
            directories.extend(self.__parse_bundle(entries))

        if directories:
            return directories

        if not saw_org or not saw_org_with_endpoint_ref:
            fallback = self.__fetch_directories_from_endpoints()
            if fallback:
                logger.info("Fetched directories from Endpoint search fallback.")
            return fallback

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
                if isinstance(resource, Organization):
                    endpoint_refs = resource.endpoint
                    try:
                        endpoint_address = self.__get_endpoint_address(
                            resource.id, endpoint_refs, entries
                        )
                    except ValueError as e:
                        logger.error(f"Failed to get endpoint address for Organization {resource.id}: {e}")
                        continue
                    orgs.append(
                        DirectoryDto(
                            id=resource.id,
                            endpoint_address=endpoint_address,
                            ura=self.__safe_get_ura(resource),
                        )
                    )
            return orgs
        except Exception as e:
            logger.exception(f"Failed to retrieve directories: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to retrieve directories: {e}"
            )

    def __fetch_directories_from_endpoints(self) -> List[DirectoryDto]:
        directories: List[DirectoryDto] = []
        seen_addresses: set[str] = set()
        next_url, entries = self.__fhir_api.search_resource(
            resource_type="Endpoint",
            params={},
        )
        directories.extend(self.__parse_endpoints(entries, seen_addresses))
        while next_url is not None:
            try:
                next_url, entries = self.__fhir_api.search_resource_page(next_url)
            except HTTPException as exc:
                if self.__is_pagination_not_found(exc, next_url):
                    logger.warning(
                        "Endpoint pagination link not found or expired; stopping pagination. url=%s",
                        next_url,
                    )
                    break
                raise
            directories.extend(self.__parse_endpoints(entries, seen_addresses))
        return directories

    @staticmethod
    def __update_org_flags(
        entries: List[BundleEntry],
        saw_org: bool,
        saw_org_with_endpoint_ref: bool,
    ) -> tuple[bool, bool]:
        for entry in entries:
            resource = entry.resource
            if isinstance(resource, Organization):
                saw_org = True
                if resource.endpoint and len(resource.endpoint) > 0:
                    saw_org_with_endpoint_ref = True
        return saw_org, saw_org_with_endpoint_ref

    @staticmethod
    def __is_pagination_not_found(exc: HTTPException, url: URL) -> bool:
        return exc.status_code in (404, 410) and "_getpages" in url.query

    def __parse_endpoints(
        self,
        entries: List[BundleEntry],
        seen_addresses: set[str],
    ) -> List[DirectoryDto]:
        directories: List[DirectoryDto] = []
        for entry in entries:
            resource = entry.resource
            if not isinstance(resource, Endpoint):
                continue
            if not resource.address:
                logger.error("Endpoint %s has no address, skipping", resource.id)
                continue
            endpoint_address = str(resource.address)
            if endpoint_address in seen_addresses:
                continue
            seen_addresses.add(endpoint_address)
            directory_id = self.__get_directory_id_from_endpoint(resource)
            ura = self.__get_ura_from_endpoint(resource)
            directories.append(
                DirectoryDto(
                    id=directory_id,
                    endpoint_address=endpoint_address,
                    ura=ura,
                )
            )
        return directories

    @staticmethod
    def __safe_get_ura(organization: Organization) -> str:
        try:
            return get_ura_from_organization(organization)
        except ValueError as exc:
            logger.error(
                "Organization %s has no URA identifier; defaulting to empty. error=%s",
                organization.id,
                exc,
            )
            return ""

    @staticmethod
    def __get_directory_id_from_endpoint(endpoint: Endpoint) -> str:
        ref = endpoint.managingOrganization
        if ref:
            if ref.reference:
                ref_id = ref.reference.split("/")[-1]
                if ref_id:
                    return ref_id
                return ref.reference
            if ref.display:
                return ref.display
            if ref.identifier and ref.identifier.value:
                return str(ref.identifier.value)
        if endpoint.id:
            return str(endpoint.id)
        return str(endpoint.address)

    @staticmethod
    def __get_ura_from_endpoint(endpoint: Endpoint) -> str:
        ref = endpoint.managingOrganization
        if not ref or not ref.identifier:
            return ""
        if not ref.identifier.system or not ref.identifier.value:
            return ""
        if ref.identifier.system.lower() != ID_SYSTEM_URA:
            return ""
        return str(ref.identifier.value)
    
    def __get_endpoint_address(
        self, org_id: str, endpoint_refs: List[Reference], entries: List[BundleEntry]
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
