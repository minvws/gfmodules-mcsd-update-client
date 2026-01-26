import copy
from typing import Dict, List

from fhir.resources.R4B.organization import Organization

from app.models.directory.dto import DirectoryDto

ID_SYSTEM_URA = "http://fhir.nl/fhir/namingsystem/ura"

UraWhitelist = Dict[str, List[str]]

def filter_ura(org: Organization, uras_allowed: list[str]) -> Organization:
    """
    Filters out URA identifiers from the organization that are not in the provided list.
    """
    filtered_identifiers = []

    for identifier in org.identifier or []:
        if identifier.system.lower() == ID_SYSTEM_URA and identifier.value not in uras_allowed:  # type: ignore
            # Skip this URA identifier, it is not in the list
            continue
        filtered_identifiers.append(identifier)

    # Make sure we do not modify the original organization
    new_org = copy.deepcopy(org)
    new_org.identifier = filtered_identifiers
    return new_org


def create_ura_whitelist(directories: list[DirectoryDto]) -> UraWhitelist:
    """
    Creates a URA whitelist dictionary from the provided directories.
    """
    ura_whitelist: UraWhitelist = {}

    for directory in directories:
        if directory.endpoint_address not in ura_whitelist:
            ura_whitelist[directory.endpoint_address] = []
        if directory.ura:
            ura_whitelist[directory.endpoint_address].append(directory.ura)

    return ura_whitelist
