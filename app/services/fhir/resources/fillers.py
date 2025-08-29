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


def fill_endpoint(data: Dict[str, Any]) -> Dict[str, Any]:
    if "payloadType" not in data:
        data["payloadType"] = []

    if "address" not in data:
        data["address"] = "https://example.com/"

    if "status" not in data:
        data["status"] = "error"

    if "connectionType" not in data:
        data["connectionType"] = create_code()

    return data


def fill_practitioner(data: Dict[str, Any]) -> Dict[str, Any]:
    if "qualification" in data:
        for i, q in enumerate(data["qualification"]):
            filled_qualification = fill_pracitioner_qualification(q)
            data["qualification"][i] = filled_qualification

    return data


def fill_resource(resource_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    match resource_type:
        case "Endpoint":
            return fill_endpoint(data)

        case "Pracitioner":
            return fill_practitioner(data)

        case _:
            return data
