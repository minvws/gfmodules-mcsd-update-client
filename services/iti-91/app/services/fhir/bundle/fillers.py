from typing import Any, Dict


import uuid

from app.models.fhir.types import McsdResourcesWithRequiredFields
from app.services.fhir.resources.fillers import fill_resource
from app.services.fhir.utils import (
    check_for_required_fields,
    get_resource_type,
    validate_resource_type,
)


def _fill_resource_type(data: Dict[str, Any]) -> Dict[str, Any]:
    if "resourceType" not in data:
        data["resourceType"] = "Bundle"
    return data


def _fill_id(data: Dict[str, Any]) -> Dict[str, Any]:
    if "id" not in data:
        data["id"] = str(uuid.uuid4())
    return data


def _fill_type(data: Dict[str, Any]) -> Dict[str, Any]:
    if "type" not in data:
        data["type"] = "collection"
    return data


def _fill_domain_resource(data: Dict[str, Any]) -> Dict[str, Any]:
    resource_type = get_resource_type(data)
    valid_mcsd_resource_type = validate_resource_type(resource_type)
    if not valid_mcsd_resource_type:
        raise ValueError(f"{resource_type} is not a valid mCSD Resource")

    has_required_fields = check_for_required_fields(resource_type)
    if has_required_fields is False:
        return data

    mcsd_resource_type = McsdResourcesWithRequiredFields(resource_type)
    return fill_resource(mcsd_resource_type, data)


def _fill_entry_resource(data: Dict[str, Any]) -> Dict[str, Any]:
    if data["resourceType"] == "Bundle":
        return fill_bundle(data)

    return _fill_domain_resource(data)


def fill_entry(data: Dict[str, Any]) -> Dict[str, Any]:
    if "entry" not in data:
        data["entry"] = []
        return data

    for entry in data["entry"]:
        if "resource" in entry:
            _fill_entry_resource(entry["resource"])

    return data


def fill_bundle(data: Dict[str, Any]) -> Dict[str, Any]:
    data = _fill_resource_type(data)
    data = _fill_id(data)
    data = _fill_type(data)
    data = fill_entry(data)
    return data
