import pytest
from typing import Any, Dict

from app.services.fhir.capability_statement_validator import is_capability_statement_valid

@pytest.fixture
def valid_capability_statement() -> Dict[str, Any]:
    """Valid mCSD CapabilityStatement for testing"""
    return {
        "resourceType": "CapabilityStatement",
        "id": "test-server-capability",
        "url": "http://localhost/CapabilityStatement/test-server",
        "version": "v0.0.1",
        "name": "test-server",
        "title": "test-server",
        "status": "active",
        "date": "2025-10-01T00:00:00+00:00",
        "publisher": "test",
        "description": "mcsd-match capability statement",
        "kind": "instance",
        "fhirVersion": "4.0.1",
        "format": ["json"],
        "rest": [
            {
                "mode": "server",
                "documentation": "Test server supporting mCSD resources",
                "security": {
                    "service": [],
                    "description": "No security implemented"
                },
                "interaction": [
                    {
                        "code": "transaction",
                        "documentation": "Supports POST Bundles"
                    }
                ],
                "resource": [
                    {
                        "type": "Organization",
                        "profile": "http://ihe.net/fhir/StructureDefinition/IHE.mCSD.Organization",
                        "interaction": [
                            {"code": "read"},
                            {"code": "search-type"},
                            {"code": "history-type"}
                        ]
                    },
                    {
                        "type": "Practitioner",
                        "profile": "http://ihe.net/fhir/StructureDefinition/IHE.mCSD.Practitioner",
                        "interaction": [
                            {"code": "read"},
                            {"code": "search-type"},
                            {"code": "history-type"}
                        ]
                    },
                    {
                        "type": "PractitionerRole",
                        "profile": "http://ihe.net/fhir/StructureDefinition/IHE.mCSD.PractitionerRole",
                        "interaction": [
                            {"code": "read"},
                            {"code": "search-type"},
                            {"code": "history-type"}
                        ]
                    },
                    {
                        "type": "Location",
                        "profile": "http://ihe.net/fhir/StructureDefinition/IHE.mCSD.Location",
                        "interaction": [
                            {"code": "read"},
                            {"code": "search-type"},
                            {"code": "history-type"}
                        ]
                    },
                    {
                        "type": "HealthcareService",
                        "profile": "http://ihe.net/fhir/StructureDefinition/IHE.mCSD.HealthcareService",
                        "interaction": [
                            {"code": "read"},
                            {"code": "search-type"},
                            {"code": "history-type"}
                        ]
                    },
                    {
                        "type": "OrganizationAffiliation",
                        "profile": "http://ihe.net/fhir/StructureDefinition/IHE.mCSD.OrganizationAffiliation",
                        "interaction": [
                            {"code": "read"},
                            {"code": "search-type"},
                            {"code": "history-type"}
                        ]
                    },
                    {
                        "type": "Endpoint",
                        "profile": "http://ihe.net/fhir/StructureDefinition/IHE.mCSD.Endpoint",
                        "interaction": [
                            {"code": "read"},
                            {"code": "search-type"},
                            {"code": "history-type"}
                        ]
                    }
                ]
            }
        ]
    }

def test_valid_capability_statement_passes(
    valid_capability_statement: Dict[str, Any]
) -> None:
    """Test that a valid mCSD CapabilityStatement passes validation"""
    assert is_capability_statement_valid(valid_capability_statement) is True

def test_no_rest_section_fails(
    valid_capability_statement: Dict[str, Any]
) -> None:
    """Test that CapabilityStatement without/wrong/empty REST section fails"""
    valid_capability_statement["rest"][0]["mode"] = "client"
    assert is_capability_statement_valid(valid_capability_statement) is False
    valid_capability_statement["rest"] = []
    assert is_capability_statement_valid(valid_capability_statement) is False
    del valid_capability_statement["rest"]
    assert is_capability_statement_valid(valid_capability_statement) is False


def test_missing_required_resource_fails(
    valid_capability_statement: Dict[str, Any]
) -> None:
    """Test that missing a required mCSD resource fails validation"""
    # Remove Organization resource
    valid_capability_statement["rest"][0]["resource"] = [
        resource for resource in valid_capability_statement["rest"][0]["resource"]
        if resource["type"] != "Organization"
    ]
    assert is_capability_statement_valid(valid_capability_statement) is False

def test_missing_required_interaction_fails(
    valid_capability_statement: Dict[str, Any]
) -> None:
    """Test that missing required interaction fails validation"""
    # Remove 'search-type' interaction from Organization
    org_resource = next(
        resource for resource in valid_capability_statement["rest"][0]["resource"]
        if resource["type"] == "Organization"
    )
    org_resource["interaction"] = [
        interaction for interaction in org_resource["interaction"]
        if interaction["code"] != "search-type"
    ]
    assert is_capability_statement_valid(valid_capability_statement) is False

def test_no_resources_section_fails(
    valid_capability_statement: Dict[str, Any]
) -> None:
    """Test that CapabilityStatement without resources section fails"""
    del valid_capability_statement["rest"][0]["resource"]
    assert is_capability_statement_valid(valid_capability_statement) is False

def test_empty_resources_section_fails(
    valid_capability_statement: Dict[str, Any]
) -> None:
    """Test that CapabilityStatement with empty resources section fails"""
    valid_capability_statement["rest"][0]["resource"] = []
    assert is_capability_statement_valid(valid_capability_statement) is False

def test_resource_without_interactions_fails(
    valid_capability_statement: Dict[str, Any]
) -> None:
    """Test that resource without interactions fails validation"""
    # Remove interactions from Organization
    org_resource = next(
        resource for resource in valid_capability_statement["rest"][0]["resource"]
        if resource["type"] == "Organization"
    )
    del org_resource["interaction"]
    assert is_capability_statement_valid(valid_capability_statement) is False

def test_missing_required_profile_fails(
    valid_capability_statement: Dict[str, Any]
) -> None:
    """Test that missing required profile fails validation"""
    org_resource = next(
        resource for resource in valid_capability_statement["rest"][0]["resource"]
        if resource["type"] == "Organization"
    )
    del org_resource["profile"]
    assert is_capability_statement_valid(valid_capability_statement) is False


def test_profile_with_different_base_url_passes(
    valid_capability_statement: Dict[str, Any]
) -> None:
    """Test that profile suffix matching allows alternative base URLs."""
    org_resource = next(
        resource for resource in valid_capability_statement["rest"][0]["resource"]
        if resource["type"] == "Organization"
    )
    org_resource["profile"] = "https://example.org/fhir/StructureDefinition/IHE.mCSD.Organization"
    assert is_capability_statement_valid(valid_capability_statement) is True
