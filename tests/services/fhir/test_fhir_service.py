from typing import Any, Dict, List
from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.endpoint import Endpoint
from fhir.resources.R4B.healthcareservice import HealthcareService
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.organizationaffiliation import OrganizationAffiliation
from fhir.resources.R4B.practitioner import Practitioner
from fhir.resources.R4B.practitionerrole import PractitionerRole
from fhir.resources.R4B.reference import Reference
from pydantic import ValidationError
import pytest
from app.models.fhir.types import BundleRequestParams
from app.services.fhir.bundle.bundle_parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService
from tests.services.fhir.conftest import (
    incomplete_resources,
    incomplete_resources_with_required_fields,
    complete_resources,
    fhir_references,
    namespaced_resources,
)


@pytest.mark.parametrize("data", incomplete_resources)
def test_create_resource_should_succeeed_when_strict_mode_is_off(
    fhir_service: FhirService, data: Dict[str, Any]
) -> None:
    actual = fhir_service.create_resource(data)

    assert isinstance(actual, DomainResource)
    assert "resourceType" in data

    resource_type = data["resourceType"]
    match resource_type:
        case "Organization":
            assert isinstance(actual, Organization)

        case "Endpoint":
            assert isinstance(actual, Endpoint)

        case "Practitioner":
            assert isinstance(actual, Practitioner)

        case "PractitionerRole":
            assert isinstance(actual, PractitionerRole)

        case "Location":
            assert isinstance(actual, Location)

        case "HealthcareService":
            assert isinstance(actual, HealthcareService)

        case "OrganizationAffiliation":
            assert isinstance(actual, OrganizationAffiliation)

        case _:
            pytest.fail()


@pytest.mark.parametrize("data", incomplete_resources_with_required_fields)
def test_create_resource_should_fail_with_incomplete_resource_when_strict_mode_is_on(
    fhir_service_strict_validation: FhirService, data: Dict[str, Any]
) -> None:
    with pytest.raises(ValidationError):
        fhir_service_strict_validation.create_resource(data)


def test_create_resource_should_fail_with_non_mcsd_resource(
    fhir_service: FhirService,
    fhir_service_strict_validation: FhirService,
    non_mcsd_resource: Dict[str, Any],
) -> None:
    with pytest.raises(TypeError):
        fhir_service.create_resource(non_mcsd_resource)

    with pytest.raises(TypeError):
        fhir_service_strict_validation.create_resource(non_mcsd_resource)


@pytest.mark.parametrize("data", complete_resources)
def test_create_resource_should_succeeed_when_strict_mode_is_one(
    fhir_service_strict_validation: FhirService, data: Dict[str, Any]
) -> None:
    actual = fhir_service_strict_validation.create_resource(data)
    assert isinstance(actual, DomainResource)
    resource_type = data["resourceType"]
    match resource_type:
        case "Organization":
            assert isinstance(actual, Organization)

        case "Endpoint":
            assert isinstance(actual, Endpoint)

        case "Practitioner":
            assert isinstance(actual, Practitioner)

        case "PractitionerRole":
            assert isinstance(actual, PractitionerRole)

        case "Location":
            assert isinstance(actual, Location)

        case "HealthcareService":
            assert isinstance(actual, HealthcareService)

        case "OrganizationAffiliation":
            assert isinstance(actual, OrganizationAffiliation)

        case _:
            pytest.fail()


@pytest.mark.parametrize(
    "data, expected_refs", zip(complete_resources, fhir_references)
)
def test_get_references_should_succeed_for_every_resource(
    fhir_service: FhirService, data: Dict[str, Any], expected_refs: List[Reference]
) -> None:
    resource = fhir_service.create_resource(data)
    actual_refs = fhir_service.get_references(resource)

    assert expected_refs == actual_refs


def test_get_references_should_ignore_contained_refs(
    fhir_service: FhirService, resource_with_contained_refs: Dict[str, Any]
) -> None:
    expected_refs = [Reference.model_construct(reference="Endpoint/ep-id")]

    resource = fhir_service.create_resource(resource_with_contained_refs)
    actual_refs = fhir_service.get_references(resource)

    assert expected_refs == actual_refs


@pytest.mark.parametrize(
    "data, expected", zip(complete_resources, namespaced_resources)
)
def test_namespace_references_should_succeed(
    fhir_service: FhirService, data: Dict[str, Any], expected: DomainResource
) -> None:
    resource = fhir_service.create_resource(data)
    actual = fhir_service.namespace_resource_references(resource, "example")

    assert expected == actual


@pytest.mark.parametrize(
    "data, expected", zip(incomplete_resources, incomplete_resources)
)
def test_namespace_references_should_not_change_anything_in_resource_when_refs_are_missing(
    fhir_service: FhirService, data: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    resource = fhir_service.create_resource(data)
    actual = fhir_service.namespace_resource_references(resource, "example")
    assert fhir_service.create_resource(expected) == actual


def test_split_reference_should_succeed(fhir_service: FhirService) -> None:
    expected = ("Organization", "org-id")
    actual_relative = fhir_service.split_reference(
        Reference.model_construct(reference="Organization/org-id")
    )
    actual_absolute = fhir_service.split_reference(
        Reference.model_construct(
            reference="http://example.com/fhir/Organization/org-id"
        )
    )
    assert expected == actual_relative
    assert expected == actual_absolute


def test_spit_reference_should_fail_when_key_reference_is_missing(
    fhir_service: FhirService,
) -> None:
    with pytest.raises(ValueError):
        fhir_service.split_reference(
            Reference.model_construct(fhir_comment="some comment")
        )


def test_split_refernece_should_fail_with_invalid_refs(
    fhir_service: FhirService,
) -> None:
    with pytest.raises(ValueError):
        fhir_service.split_reference(
            Reference.model_construct(reference="some_invalid_ref")
        )


def test_create_bundle_should_succeed_when_strict_mode_is_off(
    fhir_service: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:
    mock_org_history_bundle.pop("type")
    bundle = fhir_service.create_bundle(mock_org_history_bundle)
    assert isinstance(bundle, Bundle)


def test_create_bundle_should_succeed_when_strict_mode_is_on(
    fhir_service_strict_validation: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:
    bundle = fhir_service_strict_validation.create_bundle(mock_org_history_bundle)
    assert isinstance(bundle, Bundle)


def test_create_bundle_should_fail_when_strict_mode_is_on(
    fhir_service_strict_validation: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:
    mock_org_history_bundle.pop("type")
    with pytest.raises(ValidationError):
        fhir_service_strict_validation.create_bundle(mock_org_history_bundle)


def test_get_resource_type_and_id_from_entry_should_succeed(
    fhir_service: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:
    expected = ("Organization", "org-id")
    bundle = fhir_service.create_bundle(mock_org_history_bundle)

    assert bundle.entry is not None
    actual = fhir_service.get_resource_type_and_id_from_entry(bundle.entry[0])

    assert expected == actual


def test_get_resource_type_and_id_from_entry_should_succeed_from_url(
    fhir_service: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:

    expected = ("Organization", "org-id")
    mock_org_history_bundle["entry"][0].pop("resource")
    bundle = fhir_service.create_bundle(mock_org_history_bundle)

    assert bundle.entry is not None
    actual = fhir_service.get_resource_type_and_id_from_entry(bundle.entry[0])

    assert expected == actual


def test_get_resource_type_and_id_from_entry_should_succeed_from_entry_request(
    fhir_service: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:
    expected = ("Organization", "org-id")
    mock_org_history_bundle["entry"][0].pop("resource")
    mock_org_history_bundle["entry"][0].pop("fullUrl")

    bundle = fhir_service.create_bundle(mock_org_history_bundle)
    assert bundle.entry is not None
    actual = fhir_service.get_resource_type_and_id_from_entry(bundle.entry[0])

    assert expected == actual


def test_get_resource_type_and_id_from_entry_should_fail(
    fhir_service: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:
    mock_org_history_bundle["entry"][0].pop("resource")
    mock_org_history_bundle["entry"][0].pop("fullUrl")
    mock_org_history_bundle["entry"][0].pop("request")

    bundle = fhir_service.create_bundle(mock_org_history_bundle)
    assert bundle.entry is not None

    with pytest.raises(ValueError):
        fhir_service.get_resource_type_and_id_from_entry(bundle.entry[0])


def test_get_request_method_from_entry_should_succeed(
    fhir_service: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:
    expected = "PUT"

    bundle = fhir_service.create_bundle(mock_org_history_bundle)
    assert bundle.entry is not None
    actual = fhir_service.get_request_method_from_entry(bundle.entry[0])

    assert expected == actual


def test_get_resource_type_and_id_from_entry_should_fail_when_request_is_missing(
    fhir_service: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:
    mock_org_history_bundle["entry"][0].pop("request")
    bundle = fhir_service.create_bundle(mock_org_history_bundle)
    assert bundle.entry is not None

    with pytest.raises(ValueError):
        fhir_service.get_request_method_from_entry(bundle.entry[0])


def test_get_resource_type_and_id_should_fail_with_incorrect_method(
    fhir_service: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:
    mock_org_history_bundle["entry"][0]["request"]["method"] = "INCORRECT"
    bundle = fhir_service.create_bundle(mock_org_history_bundle)
    assert bundle.entry is not None

    with pytest.raises(ValueError):
        fhir_service.get_request_method_from_entry(bundle.entry[0])


def test_filter_history_entries_should_succeed(
    fhir_service: FhirService, mock_org_history_bundle: Dict[str, Any]
) -> None:
    mock_entry = mock_org_history_bundle["entry"][0]
    expected = [create_bundle_entry(mock_entry)]

    bundle = fhir_service.create_bundle(mock_org_history_bundle)
    assert bundle.entry is not None
    actual = fhir_service.filter_history_entries(bundle.entry)

    assert expected == actual


def test_create_bundle_request_should_succeed(fhir_service: FhirService) -> None:
    mock_node_refs = [BundleRequestParams(id="some-id", resource_type="Organization")]
    results = fhir_service.create_bundle_request(mock_node_refs)

    assert isinstance(results, Bundle)
    assert results.entry is not None
    assert len(results.entry) == 1
    assert isinstance(results.entry, List)
    assert isinstance(results.entry[0], BundleEntry)
    assert isinstance(results.entry[0].request, BundleEntryRequest) # type: ignore[attr-defined]
    assert results.entry[0].request.method == "GET" # type: ignore[attr-defined]


def test_get_entries_from_bundle_of_bundles_should_succeed(
    fhir_service: FhirService,
    mock_bundle_of_bundles: Dict[str, Any],
    mock_org_bundle_entry: Dict[str, Any],
    mock_ep_bundle_entry: Dict[str, Any],
) -> None:
    entry_1 = create_bundle_entry(mock_org_bundle_entry)
    entry_2 = create_bundle_entry(mock_ep_bundle_entry)
    expected = [entry_1, entry_2]

    bundle = fhir_service.create_bundle(mock_bundle_of_bundles)
    actual = fhir_service.get_entries_from_bundle_of_bundles(bundle)

    assert expected == actual
