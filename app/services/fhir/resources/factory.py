from typing import Dict, Any

from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.endpoint import Endpoint
from fhir.resources.R4B.healthcareservice import HealthcareService
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.organizationaffiliation import OrganizationAffiliation
from fhir.resources.R4B.practitioner import Practitioner
from fhir.resources.R4B.practitionerrole import PractitionerRole
from app.models.fhir.types import McsdResources, McsdResourcesWithRequiredFields
from app.services.fhir.resources.fillers import fill_resource
from app.services.fhir.utils import (
    check_for_required_fields,
    get_resource_type,
    validate_resource_type,
)


def _create_resource(
    resource_type: McsdResources,
    data: Dict[str, Any],
) -> DomainResource:
    match resource_type:
        case McsdResources.ORGANIZATION:
            return Organization.model_validate(data)

        case McsdResources.ENDPOINT:
            return Endpoint.model_validate(data)

        case McsdResources.ORGANIZATION_AFFILIATION:
            return OrganizationAffiliation.model_validate(data)

        case McsdResources.LOCATION:
            return Location.model_validate(data)

        case McsdResources.PRACTITIONER:
            return Practitioner.model_validate(data)

        case McsdResources.PRACTITIONER_ROLE:
            return PractitionerRole.model_validate(data)

        case McsdResources.HEALTHCARE_SERVICE:
            return HealthcareService.model_validate(data)


def create_resource(
    data: Dict[str, Any], fill_required_fields: bool = False
) -> DomainResource:
    resource_type_in_keys = any(
        "resourceType" in k or "resource_type" in k for k in data
    )
    if not resource_type_in_keys:
        raise KeyError(
            "Model is not a valid FHIR object, missing 'resourceType' property"
        )

    resource_type = get_resource_type(data)
    valid_mcsd_resource_type = validate_resource_type(resource_type)
    if not valid_mcsd_resource_type:
        raise ValueError(f"{resource_type} is not a valid mCSD Resource")

    mcsd_resource_type = McsdResources(resource_type)
    has_required_fields = check_for_required_fields(resource_type)
    if fill_required_fields is False or has_required_fields is False:
        return _create_resource(mcsd_resource_type, data)

    mcsd_resource_type_with_required_fileds = McsdResourcesWithRequiredFields(
        resource_type
    )
    filled_resource = fill_resource(mcsd_resource_type_with_required_fileds, data)
    return _create_resource(mcsd_resource_type, filled_resource)
