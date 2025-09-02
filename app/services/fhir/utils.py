from typing import Any, Dict
from app.models.fhir.types import McsdResources, McsdResourcesWithRequiredFields


def validate_resource_type(resource_type: str) -> bool:
    return any(resource_type in r.value for r in McsdResources)


def check_for_required_fields(resource_type: str) -> bool:
    return any(resource_type in r.value for r in McsdResourcesWithRequiredFields)


def get_resource_type(resource: Dict[str, Any]) -> str:
    res_type_key = "resource_type" if "resource_type" in resource else "resourceType"
    resource_type: str = resource[res_type_key]

    return resource_type
