from typing import Any, Dict
from fhir.resources.R4B.capabilitystatement import CapabilityStatement, CapabilityStatementRest

from app.models.fhir.types import McsdResources


# Constants
REQUIRED_INTERACTIONS = {"read", "search-type", "history-type"}
REQUIRED_MCSD_PROFILE_SUFFIXES = {
    "Organization": {
        "IHE.mCSD.Organization",
        "IHE.mCSD.FacilityOrganization",
        "IHE.mCSD.JurisdictionOrganization",
    },
    "OrganizationAffiliation": {
        "IHE.mCSD.OrganizationAffiliation",
        "IHE.mCSD.OrganizationAffiliation.DocShare",
    },
    "Location": {
        "IHE.mCSD.Location",
        "IHE.mCSD.FacilityLocation",
        "IHE.mCSD.JurisdictionLocation",
    },
    "Practitioner": {"IHE.mCSD.Practitioner"},
    "PractitionerRole": {"IHE.mCSD.PractitionerRole"},
    "HealthcareService": {"IHE.mCSD.HealthcareService"},
    "Endpoint": {"IHE.mCSD.Endpoint", "IHE.mCSD.Endpoint.DocShare"},
}
NL_GF_PROFILE_URLS = {
    "Organization": {"http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-organization"},
    "OrganizationAffiliation": {"http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-organizationaffiliation"},
    "Location": {"http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-location"},
    "Practitioner": {"http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-practitioner"},
    "PractitionerRole": {"http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-practitionerrole"},
    "HealthcareService": {"http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-healthcareservice"},
    "Endpoint": {"http://nuts-foundation.github.io/nl-generic-functions-ig/StructureDefinition/nl-gf-endpoint"},
}
MCSD_IG_CANONICAL = "https://profiles.ihe.net/ITI/mCSD"
MCSD_CAPABILITY_STATEMENT_URL = (
    "https://profiles.ihe.net/ITI/mCSD/CapabilityStatement/IHE.mCSD.Directory"
)
def is_capability_statement_valid(data: Dict[str, Any], require_mcsd_profiles: bool = True) -> bool:
    """
    Check if CapabilityStatement supports mCSD requirements.
    
    Args:
        data: Dictionary FHIR CapabilityStatement
        
    Returns:
        True if supports required operations, False otherwise
    """
    try:
        capability_statement = CapabilityStatement.model_validate(data)
    except Exception:
        return False

    server_rest = _find_server_rest(capability_statement)
    if not server_rest:
        return False

    return _validate_mcsd_resources(server_rest, capability_statement, require_mcsd_profiles)


def _find_server_rest(capability_statement: CapabilityStatement) -> CapabilityStatementRest | None:
    """Find the server mode REST configuration."""
    if not capability_statement.rest:
        return None
        
    for rest in capability_statement.rest:
        if rest and getattr(rest, 'mode', None) == "server":
            return rest
    return None


def _validate_mcsd_resources(
    server_rest: CapabilityStatementRest,
    capability_statement: CapabilityStatement,
    require_mcsd_profiles: bool,
) -> bool:
    """Check if all required mCSD resources are supported with correct interactions."""
    if not getattr(server_rest, 'resource', None):
        return False

    supported_resources = _create_supported_resources_map(server_rest)

    # Check that all required mCSD resources are present
    for required_resource in {resource.value for resource in McsdResources}:
        if required_resource not in supported_resources:
            return False
        
        # Check if all required interactions are supported
        resource_interactions = supported_resources[required_resource]
        if not REQUIRED_INTERACTIONS.issubset(resource_interactions):
            return False

    if require_mcsd_profiles:
        if _validate_mcsd_profiles(server_rest):
            return True
        return _declares_mcsd_ig(capability_statement)

    return True


def _validate_mcsd_profiles(server_rest: CapabilityStatementRest) -> bool:
    """Check if required mCSD profiles are declared for each supported resource."""
    if not getattr(server_rest, 'resource', None):
        return False

    resource_by_type: Dict[str, Any] = {
        resource.type: resource
        for resource in server_rest.resource  # type: ignore
        if resource and hasattr(resource, "type")
    }

    for resource_type, required_profiles in REQUIRED_MCSD_PROFILE_SUFFIXES.items():
        resource = resource_by_type.get(resource_type)
        if resource is None:
            return False
        declared_profiles = set()
        profile = getattr(resource, "profile", None)
        if profile:
            declared_profiles.add(str(profile))
        supported_profiles = getattr(resource, "supportedProfile", None)
        if supported_profiles:
            declared_profiles.update(str(p) for p in supported_profiles)
        if not declared_profiles:
            return False
        if _matches_required_profile(declared_profiles, required_profiles):
            continue
        nl_profiles = NL_GF_PROFILE_URLS.get(resource_type, set())
        if nl_profiles and declared_profiles.intersection(nl_profiles):
            continue
        return False

    return True


def _matches_required_profile(
    declared_profiles: set[str], required_suffixes: set[str]
) -> bool:
    for profile in declared_profiles:
        if any(profile.endswith(suffix) for suffix in required_suffixes):
            return True
    return False


def _declares_mcsd_ig(capability_statement: CapabilityStatement) -> bool:
    implementation_guides = getattr(capability_statement, "implementationGuide", None)
    if implementation_guides:
        if any(str(ig).startswith(MCSD_IG_CANONICAL) for ig in implementation_guides):
            return True

    instantiates = getattr(capability_statement, "instantiates", None)
    if instantiates:
        if any(str(url) == MCSD_CAPABILITY_STATEMENT_URL for url in instantiates):
            return True

    return False


def _create_supported_resources_map(server_rest: CapabilityStatementRest) -> Dict[str, set[Any | None]]:
    """Create a map of supported resources with their interactions."""
    supported_resources = {}
    for resource in server_rest.resource: # type: ignore
        if resource and hasattr(resource, 'type'):
            resource_type = resource.type
            interactions = set()
            if hasattr(resource, 'interaction') and resource.interaction:
                interactions = {
                    getattr(interaction, 'code', None) 
                    for interaction in resource.interaction 
                    if interaction and hasattr(interaction, 'code')
                }
            supported_resources[resource_type] = interactions
    return supported_resources
