from typing import Any, Dict, TypeVar, Type
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.organizationaffiliation import OrganizationAffiliation
from fhir.resources.R4B.endpoint import Endpoint
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.practitioner import Practitioner
from fhir.resources.R4B.practitionerrole import PractitionerRole
from fhir.resources.R4B.healthcareservice import HealthcareService
from pydantic import ValidationError

T = TypeVar("T", bound=DomainResource)


def create_model(model: Type[T], data: Dict[str, Any], strict: bool) -> T:
    try:
        if strict:
            resource = model.model_validate(data)
            return resource  # type: ignore

        resource = model.model_construct(**data)
        return resource  # type: ignore
    except ValidationError as e:
        raise e


def create_organization(data: Dict[str, Any], strict: bool) -> Organization:
    return create_model(Organization, data, strict)


def create_organization_affiliation(
    data: Dict[str, Any], strict: bool
) -> OrganizationAffiliation:
    return create_model(OrganizationAffiliation, data, strict)


def create_endpoint(data: Dict[str, Any], strict: bool) -> Endpoint:
    if "payloadType" not in data:
        data["payloadType"] = []

    if "address" not in data:
        data["address"] = ""

    if "status" not in data:
        code = {
            "coding": [{"system": "System not available"}],
            "text": "system was added during update to bypass validation",
        }
        data["status"] = code

    if "connectionType" not in data:
        data["connectionType"] = {
            "conding": [{"system": "System not available"}],
            "text": "system was added dyrung update to bypass validation",
        }

    return create_model(Endpoint, data, strict)


def create_location(data: Dict[str, Any], strict: bool) -> Location:
    return create_model(Location, data, strict)


def create_practitioner(data: Dict[str, Any], strict: bool) -> Practitioner:
    if "qualification" in data:
        for i, q in enumerate(data["qualification"]):
            if "code" not in q:
                code = {
                    "coding": [{"system": "System not available"}],
                    "text": "system was added during update to bypass validation",
                }
                q = {**q, "code": code}
                data["qualification"][i] = q

    return create_model(Practitioner, data, strict)


def create_practitioner_role(data: Dict[str, Any], strict: bool) -> PractitionerRole:
    return create_model(PractitionerRole, data, strict)


def create_healthcare_service(data: Dict[str, Any], strict: bool) -> HealthcareService:
    return create_model(HealthcareService, data, strict)


def create_resource(data: Dict[str, Any], strict: bool = False) -> DomainResource:
    resource_type_does_not_exist = all(
        "resourceType" in k or "resource_type" in k for k in data
    )
    if resource_type_does_not_exist:
        raise ValueError("Model is not a valid FHIR model")

    res_type_key = "resource_type" if "resource_type" in data else "resourceType"
    resource_type: str = data[res_type_key]
    match resource_type:
        case "Organization":
            return create_organization(data, strict)

        case "OrganizationAffiliation":
            return create_organization_affiliation(data, strict)

        case "Endpoint":
            return create_endpoint(data, strict)

        case "Location":
            return create_location(data, strict)

        case "Practitioner":
            return create_practitioner(data, strict)

        case "PractitionerRole":
            return create_practitioner_role(data, strict)

        case "HealthcareService":
            return create_healthcare_service(data, strict)

        case _:
            raise TypeError("Model is not an mCSD defined model")
