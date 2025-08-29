from typing import Dict, Any

from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.endpoint import Endpoint
from fhir.resources.R4B.healthcareservice import HealthcareService
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.organizationaffiliation import OrganizationAffiliation
from fhir.resources.R4B.practitioner import Practitioner
from fhir.resources.R4B.practitionerrole import PractitionerRole
from app.services.fhir.resources.fillers import fill_resource
from app.services.fhir.utils import get_resource_type, validate_resource_type


def _create_resource(resource_type: str, data: Dict[str, Any]) -> DomainResource:
    match resource_type:
        case "Organization":
            return Organization.model_validate(data)

        case "Endpoint":
            return Endpoint.model_validate(data)

        case "OrganizationAffiliation":
            return OrganizationAffiliation.model_validate(data)

        case "Location":
            return Location.model_validate(data)

        case "Practitioner":
            return Practitioner.model_validate(data)

        case "PractitionerRole":
            return PractitionerRole.model_validate(data)

        case "HealthcareService":
            return HealthcareService.model_validate(data)

        case _:
            raise ValueError(
                f"Unable to create mode for {resource_type}, value: \n{data}"
            )


def create_resource(data: Dict[str, Any], strict: bool = False) -> DomainResource:
    resource_type_does_not_exist = all(
        "resourceType" in k or "resource_type" in k for k in data
    )
    if resource_type_does_not_exist:
        raise ValueError("Model is not a valid FHIR model")

    resource_type = get_resource_type(data)
    valid_resource_type = validate_resource_type(resource_type)

    if not valid_resource_type:
        raise ValueError(f"{resource_type} is not a valid mCSD Resource")

    if strict:
        return _create_resource(resource_type, data)

    filled_resource = fill_resource(resource_type, data)
    return _create_resource(resource_type, filled_resource)
