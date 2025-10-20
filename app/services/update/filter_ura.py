import copy

from fhir.resources.R4B.organization import Organization

ID_SYSTEM_URA = "http://fhir.nl/fhir/namingsystem/ura"

def filter_ura(org: Organization, ura_whitelist: list[str]) -> Organization:
    """
    Filters out URA identifiers from the organization that are not in the provided whitelist.
    """
    filtered_identifiers = []

    for identifier in org.identifier or []:
        if identifier.system.lower() == ID_SYSTEM_URA and identifier.value not in ura_whitelist:  # type: ignore
            # Skip this URA identifier, it is not in the whitelist
            continue
        filtered_identifiers.append(identifier)

    # Make sure we do not modify the original organization
    new_org = copy.deepcopy(org)
    new_org.identifier = filtered_identifiers
    return new_org
