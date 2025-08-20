import logging
from typing import Any, Dict, List, Sequence
from fastapi import HTTPException
from yarl import URL

from app.db.entities.ignored_directory import IgnoredDirectory
from app.models.directory.dto import DirectoryDto
from app.services.entity.ignored_directory_service import IgnoredDirectoryService
from app.services.api.fhir_api import FhirApi
from app.services.fhir.fhir_service import FhirService
from app.services.directory_provider.directory_provider import DirectoryProvider
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.endpoint import Endpoint

from app.services.fhir.references.reference_misc import ReferenceType, get_reference_type, is_same_origin

logger = logging.getLogger(__name__)


class DirectoryApiProvider(DirectoryProvider):
    def __init__(self, fhir_api: FhirApi, ignored_directory_service: IgnoredDirectoryService, provider_url: str) -> None:
        self.__fhir_api = fhir_api
        self.__fhir_service = FhirService(strict_validation=False)
        self.__ignored_directory_service = ignored_directory_service
        self.__provider_url = provider_url

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        return self.__fetch_directories(include_ignored=include_ignored, include_ignored_ids=None)


    def get_all_directories_include_ignored(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        return self.__fetch_directories(include_ignored=False, include_ignored_ids=include_ignored_ids)


    def __fetch_directories(
        self,
        include_ignored: bool,
        include_ignored_ids: List[str] | None = None
    ) -> List[DirectoryDto]:
        directories: List[DirectoryDto] = []
        ignored_directories: Sequence[IgnoredDirectory] = []
        if not include_ignored or include_ignored_ids:
            ignored_directories = self.__ignored_directory_service.get_all_ignored_directories()

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
                    directory = self.__create_directory_dto(org, endpoint_map)
                    if directory:
                        is_ignored = any(dir.directory_id == directory.id for dir in ignored_directories)
                        is_included = include_ignored_ids and directory.id in include_ignored_ids

                        if is_ignored and not include_ignored and not is_included:
                            continue
                        directories.append(directory)
            except Exception as e:
                logger.exception(f"Failed to retrieve directories: {e}")
                raise e

        return directories


    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        params = {
            "_id": directory_id,
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
                raise ValueError(f"No organization found with directory_id {directory_id}")

            directory = self.__create_directory_dto(org, endpoint_map, directory_id)
            if not directory:
                raise ValueError(f"Invalid organization data for directory_id {directory_id}")

            return directory

        except Exception as e:
            logger.warning(f"Failed to retrieve directory {directory_id}: {e}")
            raise e

    def check_if_directory_is_deleted(self, directory_id: str) -> bool:
        try:
            _org = self.__fhir_api.get_resource_by_id(
                resource_type="Organization",
                resource_id=directory_id,
            )
            return False # Organization resource exists, so directory is not deleted
        except HTTPException as e:
            if e.status_code == 410:
                return True
            logger.warning(f"Check for deleted directory {directory_id} failed: {e}")
            return False
        except Exception as e:
            logger.warning(f"Check for deleted directory {directory_id} failed: {e}")
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

    def __create_directory_dto(
        self, org: Organization, endpoint_map: Dict[str, Endpoint], directory_id: str | None = None
    ) -> DirectoryDto | None:
        name = org.name
        id = directory_id or org.id
        endpoint_address = self.__get_endpoint_address(org, endpoint_map)
        if endpoint_address is None:
            logger.warning(f"Organization {org.id} has no valid endpoint address.")
            return None

        if not all(isinstance(val, str) and val for val in [name, id, endpoint_address]):
            logger.warning(f"Invalid directory data for ID {id}. Skipping.")
            return None

        return DirectoryDto(
            id=id,
            name=name,
            endpoint=endpoint_address,
            is_deleted=False,
        )

    def __get_endpoint_address(self, org: Organization, endpoint_map: Dict[str, Endpoint]) -> str | None:
        if org.endpoint is None:
            return None

        if len(org.endpoint) > 1:
            logger.warning(f"Organization {org.id} has multiple endpoints, using the first one.")

        ref = org.endpoint[0]

        # Relative references are expected to be in the format "Endpoint/{id}"
        if get_reference_type(ref) == ReferenceType.RELATIVE:
            if not ref.reference.startswith("Endpoint/"):
                logger.warning(f"Unexpected reference format: {ref.reference}")
                return None
            endpoint = endpoint_map.get(ref.reference.split("/")[-1])
            return str(endpoint.address) if endpoint else None

        # Absolute references should be checked against the directory API's origin
        elif get_reference_type(ref) == ReferenceType.ABSOLUTE:
            if not is_same_origin(ref.reference, self.__provider_url):
                logger.warning(f"Endpoint reference {ref.reference} is not from the same origin as the directory API.")
                return None
            return str(ref.reference)

        return None
