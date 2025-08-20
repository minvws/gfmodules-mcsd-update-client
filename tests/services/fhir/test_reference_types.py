import pytest
from fhir.resources.R4B.reference import Reference

from app.services.fhir.references.reference_misc import ReferenceType, get_reference_type, is_same_origin


@pytest.mark.parametrize("ref_str,expected", [
    ("Patient/123", ReferenceType.RELATIVE),
    ("Observation/abc-123/_history/2", ReferenceType.RELATIVE),
    ("https://fhir.example.org/Patient/123", ReferenceType.ABSOLUTE),
    ("http://example.com/fhir/Patient/42", ReferenceType.ABSOLUTE),
    ("#pr1", ReferenceType.LOCAL),
    ("http://hl7.org/fhir/StructureDefinition/bp|4.0.1", ReferenceType.CANONICAL),
])
def test_reference_reference_field_variants(ref_str: str, expected: ReferenceType) -> None:
    ref = Reference(reference=ref_str)
    assert get_reference_type(ref) == expected


def test_reference_logical_identifier_only() -> None:
    ref = Reference(
        identifier={
            "system": "http://hospital.example.org/mrn",
            "value": "123456"
        }
    )
    assert get_reference_type(ref) == ReferenceType.LOGICAL


def test_reference_both_identifier_and_reference_prefers_reference() -> None:
    ref = Reference(
        reference="Patient/123",
        identifier={"system": "http://hospital.example.org/mrn", "value": "123456"}
    )
    assert get_reference_type(ref) == ReferenceType.RELATIVE


def test_reference_unknown_when_empty() -> None:
    ref = Reference()
    assert get_reference_type(ref) == ReferenceType.UNKNOWN


@pytest.mark.parametrize("ref_str,expected", [
    ("http://hl7.org/fhir/StructureDefinition/bp", ReferenceType.ABSOLUTE),
    ("https://example.org/Patient/1?ver=1|2", ReferenceType.CANONICAL),
])
def test_reference_edge_cases(ref_str: str, expected: ReferenceType) -> None:
    ref = Reference(reference=ref_str)
    assert get_reference_type(ref) == expected


@pytest.mark.parametrize("url1,url2,expected", [
    ("https://fhir.example.org/baseR4",
     "https://fhir.example.org/Patient/123",
     True),

    ("https://fhir.example.org/baseR4",
     "https://foobar.example.org/Patient/123",
     False),

    ("https://fhir.example.org/baseR4",
     "https://foobar.example.org/baseR4",
     False),

    ("https://fhir.example.org",
     "http://fhir.example.org",
     False),

    ("https://fhir.example.org",
     "https://other.example.org",
     False),

    ("https://fhir.example.org",
     "https://fhir.example.org:443/Patient/123",
     True),

    ("http://fhir.example.org",
     "http://fhir.example.org:80/Patient/123",
     True),

    ("http://fhir.example.org:8080",
     "http://fhir.example.org:80",
     False),

    ("HTTPS://FHIR.EXAMPLE.ORG",
     "https://fhir.example.org",
     True),
])
def test_is_same_origin(url1: str, url2: str, expected: bool) -> None:
    assert is_same_origin(url1, url2) == expected
