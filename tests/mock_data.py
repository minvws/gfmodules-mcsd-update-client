from typing import Any, Dict, Final, List
from faker import Faker

faker = Faker()

organization_incomplete: Final[Dict[str, Any]] = {
    "resourceType": "Organization",
    "id": "organization-1",
    "name": "example organization",
}
endpoint_incomplete: Final[Dict[str, Any]] = {
    "resourceType": "Endpoint",
    "id": "endpoint-1",
    "connectionType": {
        "system": "http://hl7.org/fhir/endpoint-connection-type",
        "code": "EX",
        "display": "example",
    },
}

location_incomplete: Final[Dict[str, Any]] = {
    "resourceType": "Location",
    "id": "location-1",
    "name": "example location",
}

practitioner_incomplete: Final[Dict[str, Any]] = {
    "resourceType": "Practitioner",
    "id": "practitioner-1",
    "name": [{"family": "Example", "given": ["Test"]}],
}


practitioner_role_incomplete: Final[Dict[str, Any]] = {
    "resourceType": "PractitionerRole",
    "id": "practitionerrole-1",
}

healthcare_service_incomplete: Final[Dict[str, Any]] = {
    "resourceType": "HealthcareService",
    "id": "healthcareservice-1",
    "name": "example service",
    "serviceType": [
        {"coding": [{"system": "http://snomed.info/sct", "code": "394682007"}]}
    ],
}

organization_affiliation_incomplete: Final[Dict[str, Any]] = {
    "resourceType": "OrganizationAffiliation",
    "id": "affiliation-1",
    "active": True,
}

organization: Final[Dict[str, Any]] = {
    "resourceType": "Organization",
    "id": "org-id",
    "identifier": [{"system": "example", "value": "example"}],
    "active": True,
    "type": [
        {
            "coding": [{"system": "example", "code": "example", "display": "example"}],
            "text": "example",
        }
    ],
    "name": "example",
    "alias": ["example"],
    "telecom": [{"system": "example", "value": "example", "use": "example"}],
    "address": [
        {
            "city": "example",
            "state": "example",
        }
    ],
    "partOf": {"reference": "Organization/org-id"},
    "contact": [
        {
            "purpose": {
                "coding": [
                    {"system": "example", "code": "example", "display": "example"}
                ]
            },
            "name": {"family": "example", "given": ["example"]},
            "telecom": [{"system": "example", "value": "example"}],
        }
    ],
    "endpoint": [{"reference": "Endpoint/ep-id"}],
}


endpoint: Final[Dict[str, Any]] = {
    "resourceType": "Endpoint",
    "id": "ep-id",
    "status": "example",
    "connectionType": {"system": "example", "code": "example", "display": "example"},
    "name": "example",
    "managingOrganization": {"reference": "Organization/org-id"},
    "payloadType": [
        {"coding": [{"system": "example", "code": "example"}], "text": "example"}
    ],
    "address": "example",
    "header": ["example"],
}


practitioner: Final[Dict[str, Any]] = {
    "resourceType": "Practitioner",
    "id": "example",
    "identifier": [{"system": "example", "value": "example"}],
    "active": True,
    "name": [{"family": "example", "given": ["example"], "prefix": ["example"]}],
    "telecom": [{"system": "example", "value": "example", "use": "example"}],
    "address": [{"city": "example", "state": "example"}],
    "gender": "example",
    "birthDate": faker.date(),
    "qualification": [
        {
            "code": {
                "coding": [{"system": "example", "code": "example"}],
                "text": "example",
            },
            "issuer": {"reference": "Organization/org-id"},
        }
    ],
}


practitioner_role: Final[Dict[str, Any]] = {
    "resourceType": "PractitionerRole",
    "id": "example",
    "identifier": [{"system": "example", "value": "example"}],
    "active": False,
    "practitioner": {"reference": "Practitioner/pract-id"},
    "organization": {"reference": "Organization/org-id"},
    "code": [{"coding": [{"system": "example", "code": "example"}], "text": "example"}],
    "location": [{"reference": "Location/loc-id"}],
    "telecom": [{"system": "example", "value": "example"}],
}

practitioner_role_refs: Final[List[Dict[str, str]]] = [
    {"reference": "Practitioner/pract-id"},
    {"reference": "Organization/org-id"},
    {"reference": "Location/loc-id"},
]

location: Final[Dict[str, Any]] = {
    "resourceType": "Location",
    "id": "example",
    "identifier": [{"system": "example", "value": "example"}],
    "status": "example",
    "name": "example",
    "managingOrganization": {"reference": "Organization/org-id"},
    "type": [{"coding": [{"system": "example", "code": "example"}]}],
}

healthcare_service: Final[Dict[str, Any]] = {
    "resourceType": "HealthcareService",
    "id": "example",
    "identifier": [{"system": "example", "value": "example"}],
    "active": False,
    "providedBy": {"reference": "Organization/org-id"},
    "coverageArea": [{"reference": "Location/loc-id"}],
    "category": [
        {"coding": [{"system": "example", "code": "example"}], "text": "example"}
    ],
    "type": [{"coding": [{"system": "example", "code": "example"}]}],
    "location": [{"reference": "Location/loc-id-2"}],
}

organization_affiliation: Final[Dict[str, Any]] = {
    "resourceType": "OrganizationAffiliation",
    "id": "example",
    "identifier": [{"system": "example", "value": "example"}],
    "active": True,
    "organization": {"reference": "Organization/org-id"},
    "participatingOrganization": {"reference": "Organization/org-id-2"},
    "code": [{"coding": [{"system": "example", "code": "example"}]}],
    "location": [{"reference": "Location/loc-id"}],
    "network": [
        {"reference": "Organization/some-org-id"},
        {"reference": "Organization/some-other-org-id"},
    ],
}
