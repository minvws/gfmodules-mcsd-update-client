from typing import Any, Dict
from fhir.resources.R4B.capabilitystatement import CapabilityStatement, CapabilityStatementRest

from app.models.fhir.types import McsdResources


# Constants
REQUIRED_INTERACTIONS = {"read", "search-type", "history-type"}
def is_capability_statement_valid(data: Dict[str, Any]) -> bool:
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

    return _validate_mcsd_resources(server_rest)


def _find_server_rest(capability_statement: CapabilityStatement) -> CapabilityStatementRest | None:
    """Find the server mode REST configuration."""
    if not capability_statement.rest:
        return None
        
    for rest in capability_statement.rest:
        if rest and getattr(rest, 'mode', None) == "server":
            return rest
    return None


def _validate_mcsd_resources(server_rest: CapabilityStatementRest) -> bool:
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

    return True


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