from typing import Any, Dict
import pytest
import copy

from app.services.fhir.fhir_service import FhirService
from app.services.fhir.model_factory import create_resource
from app.services.fhir.references.reference_extractor import get_references
from app.services.fhir.references.reference_namespacer import (
    namespace_resource_reference,
)
from tests.mock_data import (
    endpoint,
    endpoint_incomplete,
    healthcare_service,
    healthcare_service_incomplete,
    location,
    location_incomplete,
    organization,
    organization_affiliation,
    organization_affiliation_incomplete,
    organization_incomplete,
    practitioner,
    practitioner_incomplete,
    practitioner_role,
    practitioner_role_incomplete,
)


incomplete_resources = [
    copy.deepcopy(organization_incomplete),
    copy.deepcopy(endpoint_incomplete),
    copy.deepcopy(location_incomplete),
    copy.copy(practitioner_incomplete),
    copy.deepcopy(practitioner_role_incomplete),
    copy.deepcopy(healthcare_service_incomplete),
    copy.deepcopy(organization_affiliation_incomplete),
]

incomplete_resources_with_required_fields = [
    copy.deepcopy(endpoint_incomplete),
    copy.deepcopy(healthcare_service_incomplete),
]

complete_resources = [
    copy.deepcopy(organization),
    copy.deepcopy(endpoint),
    copy.deepcopy(practitioner),
    copy.deepcopy(practitioner_role),
    copy.deepcopy(location),
    copy.deepcopy(healthcare_service),
    copy.deepcopy(organization_affiliation),
]

complete_fhir_resource = [create_resource(res) for res in complete_resources]

fhir_references = [get_references(res) for res in complete_fhir_resource]

namespaced_resources = [
    namespace_resource_reference(res, "example")
    for res in copy.deepcopy(complete_fhir_resource)
]


@pytest.fixture
def non_mcsd_resource() -> Dict[str, Any]:
    return {"resourceType": "Medication", "status": "active"}


@pytest.fixture
def resource_with_contained_refs() -> Dict[str, Any]:
    return {
        "resourceType": "Organization",
        "id": "org-id",
        "name": "example",
        "active": True,
        "endpoint": [{"reference": "Endpoint/ep-id"}],
        "managingOrganization": {"reference": "#org-id-2"},
        "contained": [
            {"resourceType": "Organization", "id": "org-id-2", "name": "Parent-org"}
        ],
    }


@pytest.fixture()
def fhir_service_strict_validation() -> FhirService:
    return FhirService(strict_validation=True)
