from typing import Any, Dict

from app.models.fhir.types import McsdResourcesWithRequiredFields


def create_coding() -> Dict[str, Any]:
    return {
        "system": "System not available",
        "display": "system was added during update cycle to bypass validation",
    }


def fill_practioner_qualification(qualification: Dict[str, Any]) -> Dict[str, Any]:
    if "code" not in qualification:
        qualification["code"] = {"coding": [create_coding()]}

    return qualification


def fill_not_available_object(not_available: Dict[str, Any]) -> Dict[str, Any]:
    """
    Helper function that will fill 'description' property of a BackboneElement
    notAvailable.
    """
    if "description" not in not_available:
        not_available["description"] = (
            "description is not available and was added during update cycle to bypass validation"
        )

    return not_available


def fill_location_position(position: Dict[str, Any]) -> Dict[str, Any]:
    if "longitude" not in position:
        position["longitude"] = 0.0

    if "latitude" not in position:
        position["latitude"] = 0.0

    return position


def fill_endpoint(data: Dict[str, Any]) -> Dict[str, Any]:
    if "status" not in data:
        data["status"] = "test"

    if "connectionType" not in data:
        data["connectionType"] = create_coding()

    if "payloadType" not in data:
        data["payloadType"] = []

    if "address" not in data:
        data["address"] = "https://example.com/"

    return data


def fill_practitioner(data: Dict[str, Any]) -> Dict[str, Any]:
    if "qualification" in data:
        for i, q in enumerate(data["qualification"]):
            filled_qualification = fill_practioner_qualification(q)
            data["qualification"][i] = filled_qualification

    return data


def fill_not_available(data: Dict[str, Any]) -> Dict[str, Any]:
    if "notAvailable" in data:
        for i, n in enumerate(data["notAvailable"]):
            filled_not_available = fill_not_available_object(n)
            data["notAvailable"][i] = filled_not_available

    return data


def fill_location(data: Dict[str, Any]) -> Dict[str, Any]:
    if "position" in data:
        filled_position = fill_location_position(data["position"])
        data["position"] = filled_position

    return data


def fill_resource(
    resource_type: McsdResourcesWithRequiredFields, data: Dict[str, Any]
) -> Dict[str, Any]:
    match resource_type:
        case McsdResourcesWithRequiredFields.ENDPOINT:
            return fill_endpoint(data)

        case McsdResourcesWithRequiredFields.PRACTITIONER:
            return fill_practitioner(data)

        case McsdResourcesWithRequiredFields.PRACTITIONER_ROLE:
            return fill_not_available(data)

        case McsdResourcesWithRequiredFields.HEALTHCARE_SERVICE:
            return fill_not_available(data)

        case McsdResourcesWithRequiredFields.LOCATION:
            return fill_location(data)
