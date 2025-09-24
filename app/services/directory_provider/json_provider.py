from typing import Any, List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.services.fhir.fhir_service import FhirService
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.endpoint import Endpoint



class DirectoryJsonProvider(DirectoryProvider):
    """
    Service to manage directories from a JSON data source.
    """
    def __init__(self, lrza_output: Any, directory_info_service: DirectoryInfoService) -> None:
        self.__lrza_output = lrza_output
        self.__directory_info_service = directory_info_service
        self.__fhir_service = FhirService(fill_required_fields=False)
        self._parsed_lrza_output = self._parse_lrza_output()


    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        ignored_ids = {dir.id for dir in self.__directory_info_service.get_all_ignored()}
        dirs = []
        for directory in self._parsed_lrza_output[0]:
            if directory.id in ignored_ids and not include_ignored:   
                continue
            dirs.append(
                self._update_directory(directory, self._parsed_lrza_output[1])
            ) 
        return dirs

    def get_all_directories_include_ignored_ids(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        ignored_ids = {dir.id for dir in self.__directory_info_service.get_all_ignored()}
        dirs = []
        for directory in self._parsed_lrza_output[0]:
            if directory.id in ignored_ids and directory.id not in include_ignored_ids:   
                continue
            dirs.append(
                self._update_directory(directory, self._parsed_lrza_output[1])
            ) 
        return dirs

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        for directory in self._parsed_lrza_output[0]:
            if directory.id == directory_id:
                return self._update_directory(directory, self._parsed_lrza_output[1])
        raise HTTPException(
            status_code=404,
            detail=f"Directory with ID '{directory_id}' not found."
        )
    

    def _parse_lrza_output(self) -> tuple[List[Organization], dict[str, Endpoint]]:
        bundle = self.__fhir_service.create_bundle(self.__lrza_output)
        bundle.entry = bundle.entry or []
        orgs = []
        endpoints_map: dict[str, Endpoint] = {}
        for entry in bundle.entry:
            if entry.resource is not None: # type: ignore[attr-defined]
                res = entry.resource # type: ignore[attr-defined]
                if not res.id:
                    raise ValueError("Resource is missing an ID.")
                if isinstance(res, Organization):
                    orgs.append(res)
                elif isinstance(res, Endpoint):
                    endpoints_map[res.id] = res
        return orgs, endpoints_map 
    
    def _update_directory(self, org: Organization, endpoints_map: dict[str, Endpoint]) -> DirectoryDto:
        if not org.id:
            raise ValueError("Organization resource is missing an ID.")
        if not org.endpoint or len(org.endpoint) == 0 or len(org.endpoint) > 1:
            raise ValueError(f"Organization with ID '{org.id}' must have exactly one Endpoint reference.")
        node_ref = self.__fhir_service.make_reference_node(org.endpoint[0], "https://example.com/fhir")
        if node_ref.id is None or node_ref.resource_type != "Endpoint":
            raise ValueError(f"Organization with ID '{org.id}' has an invalid Endpoint reference.")
        if node_ref.id not in endpoints_map:
            raise ValueError(f"Endpoint with ID '{node_ref.id}' referenced by Organization '{org.id}' not found.")
        endpoint = endpoints_map[node_ref.id]
        if endpoint.address is None:
            raise ValueError(f"Endpoint with ID '{node_ref.id}' must have an address.")
        return self.__directory_info_service.update(
            directory_id=org.id,
            endpoint_address=str(endpoint.address)
        )