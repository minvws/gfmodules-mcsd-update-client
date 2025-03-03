import logging
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


logger = logging.getLogger(__name__)

T = TypeVar("T", bound=DomainResource)


def warning_message(resource: str, missing_field: str, placeholder: str) -> str:
    return f"'{resource}' does not contain '{missing_field}' which is required in FHIR resource definition, adding '{placeholder}' to bypass validations..."


def create_model(model: Type[T], data: Dict[str, Any], strict: bool) -> T:
    try:
        if strict:
            resource = model.model_validate(data)
            return resource

        resource = model.model_construct(**data)
        return resource
    except ValidationError as e:
        raise e


def create_organization(data: Dict[str, Any], strict: bool) -> Organization:
    return create_model(Organization, data, strict)


def create_organization_affiliation(
    data: Dict[str, Any], strict: bool
) -> OrganizationAffiliation:
    return create_model(OrganizationAffiliation, data, strict)


def create_endpoint(data: Dict[str, Any], strict: bool) -> Endpoint:
    if "payload_type" not in data:
        data["payload_type"] = []

        logger.warning(
            warning_message("Endpoint", "payload_type", "empty array")
            # "Endpoint does not contain 'payload_type' which is a required filled, adding empty array..."
        )
    if "address" not in data:
        data["address"] = ""
        logger.warning(
            # "Endpoint does not contain 'address' which is a required field, adding empty string..."
            warning_message("Endpoint", "address", "empty string")
        )
    return create_model(Endpoint, data, strict)


def create_location(data: Dict[str, Any], strict: bool) -> Location:
    return create_model(Location, data, strict)


def create_practitioner(data: Dict[str, Any], strict: bool) -> Practitioner:
    if "qualification" in data:
        for i, q in enumerate(data["qualification"]):
            if "code" not in q:
                logger.warning(
                    warning_message(
                        "Parctitioner", "code", "code system with warning text"
                    )
                )
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
    if "resource_type" not in data.keys():
        raise ValueError("Model is not a valid FHIR model")

    resource_type: str = data["resource_type"]
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
            raise ValidationError("Model is not an mCSD defined model")
