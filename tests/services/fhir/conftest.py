from typing import Any, Dict
import pytest
import copy

from app.services.fhir.fhir_service import FhirService
from app.services.fhir.model_factory import create_resource
from app.services.fhir.references.reference_extractor import get_references
from app.services.fhir.references.reference_namespacer import (
    namespace_resource_reference,
)
from tests.services.mock_data import (
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
    organization_incomplete,
    endpoint_incomplete,
    location_incomplete,
    practitioner_incomplete,
    practitioner_role_incomplete,
    healthcare_service_incomplete,
    organization_affiliation_incomplete,
]

incomplete_resources_with_required_fields = [
    endpoint_incomplete,
    healthcare_service_incomplete,
]

complete_resources = [
    organization,
    endpoint,
    practitioner,
    practitioner_role,
    location,
    healthcare_service,
    organization_affiliation,
]

complete_fhir_resource = [
    create_resource(res) for res in copy.deepcopy(complete_resources)
]

fhir_references = [get_references(res) for res in copy.deepcopy(complete_fhir_resource)]

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


@pytest.fixture
def mock_bundle_of_bundles(
    mock_org: Dict[str, Any], mock_ep: Dict[str, Any]
) -> Dict[str, Any]:
    return {
        "type": "batch",
        "entry": [
            {
                "resource": {
                    "resourceType": "Bundle",
                    "type": "history",
                    "entry": [
                        {
                            "resource": mock_org,
                        }
                    ],
                }
            },
            {
                "resource": {
                    "resourceType": "Bundle",
                    "type": "history",
                    "entry": [
                        {
                            "resource": mock_ep,
                        }
                    ],
                }
            },
        ],
    }


@pytest.fixture()
def fhir_service_strict_validation() -> FhirService:
    return FhirService(strict_validation=True)
