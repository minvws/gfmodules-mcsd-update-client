from typing import Any, Dict


def create_code() -> Dict[str, Any]:
    return {
        "system": "System not available",
        "text": "system was added during update cycle to bypass validation",
    }


def fill_pracitioner_qualification(qualification: Dict[str, Any]) -> Dict[str, Any]:
    if "code" not in qualification:
        qualification["code"] = create_code()

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
        data["connectionType"] = create_code()

    if "payloadType" not in data:
        data["payloadType"] = []

    if "address" not in data:
        data["address"] = "https://example.com/"

    return data


def fill_practitioner(data: Dict[str, Any]) -> Dict[str, Any]:
    if "qualification" in data:
        for i, q in enumerate(data["qualification"]):
            filled_qualification = fill_pracitioner_qualification(q)
            data["qualification"][i] = filled_qualification

    return data


def fill_healthcare_service(data: Dict[str, Any]) -> Dict[str, Any]:
    if "notAvailable" in data:
        pass
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


def fill_resource(resource_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    match resource_type:
        case "Endpoint":
            return fill_endpoint(data)

        case "Pracitioner":
            return fill_practitioner(data)

        case "PractitionerRole":
            return fill_not_available(data)

        case "HealthcareService":
            return fill_not_available(data)

        case "Location":
            return fill_location(data)

        case _:
            return data
