import json
from typing import Any, List
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.healthcareservice import HealthcareService
from fhir.resources.R4B.reference import Reference
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.organizationaffiliation import OrganizationAffiliation
from fhir.resources.R4B.endpoint import Endpoint
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.practitioner import Practitioner, PractitionerQualification
from fhir.resources.R4B.practitionerrole import PractitionerRole


def extract_references(data: Any) -> Reference | None:
    if isinstance(data, dict):
        if "reference" in data and data["reference"][0] != "#":
            return Reference.model_construct(**data)

    if isinstance(data, Reference):
        if data.reference is not None and not data.reference.startswith("#"):
            return data

    return None


def _get_org_references(model: Organization) -> List[Reference]:
    refs: List[Reference] = []
    endpoints = model.endpoint
    if endpoints is not None:
        for endpoint in endpoints:
            ref = extract_references(endpoint)
            if ref is not None:
                refs.append(ref)

    return refs


def _get_endpoint_references(model: Endpoint) -> List[Reference]:
    refs: List[Reference] = []
    managing_org = model.managingOrganization
    if managing_org is not None:
        new_ref = extract_references(managing_org)
        if new_ref is not None:
            refs.append(new_ref)

    return refs


def _get_org_affiliation_references(
    model: OrganizationAffiliation,
) -> List[Reference]:
    refs: List[Reference] = []
    organization = model.organization
    participating_org = model.participatingOrganization
    network = model.network
    location = model.location
    healthcare_service = model.healthcareService
    endpoints = model.endpoint

    if organization is not None:
        new_ref = extract_references(organization)
        if new_ref is not None:
            refs.append(new_ref)

    if participating_org is not None:
        new_ref = extract_references(participating_org)
        if new_ref is not None:
            refs.append(new_ref)

    if network is not None:
        for org in network:
            new_ref = extract_references(org)
            if new_ref is not None:
                refs.append(new_ref)

    if location is not None:
        for loc in location:
            new_ref = extract_references(loc)
            if new_ref is not None:
                refs.append(new_ref)

    if healthcare_service is not None:
        for service in healthcare_service:
            new_ref = extract_references(service)
            if new_ref is not None:
                refs.append(new_ref)

    if endpoints is not None:
        for endpoint in endpoints:
            new_ref = extract_references(endpoint)
            if new_ref is not None:
                refs.append(new_ref)

    return refs


def _get_location_references(model: Location) -> List[Reference]:
    refs: List[Reference] = []
    managing_org = model.managingOrganization
    part_of = model.partOf
    endpoints = model.endpoint

    if managing_org is not None:
        new_ref = extract_references(managing_org)
        if new_ref is not None:
            refs.append(new_ref)

    if part_of is not None:
        new_ref = extract_references(part_of)
        if new_ref is not None:
            refs.append(new_ref)

    if endpoints is not None:
        for endpoint in endpoints:
            new_ref = extract_references(endpoint)
            if new_ref is not None:
                refs.append(new_ref)

    return refs


def _get_practitioner_references(model: Practitioner) -> List[Reference]:
    refs: List[Reference] = []
    qualifications = model.qualification
    if qualifications is not None:
        for qualification in qualifications:
            if isinstance(qualification, PractitionerQualification):
                issuer = qualification.issuer

                new_ref = extract_references(issuer)
                if new_ref is not None:
                    refs.append(new_ref)

    return refs


def _get_practitioner_role_references(model: PractitionerRole) -> List[Reference]:
    refs: List[Reference] = []
    practitioner = model.practitioner
    organization = model.organization
    locations = model.location
    healthcare_services = model.healthcareService
    endpoints = model.endpoint

    if practitioner is not None:
        new_ref = extract_references(practitioner)
        if new_ref is not None:
            refs.append(new_ref)

    if organization is not None:
        new_ref = extract_references(organization)
        if new_ref is not None:
            refs.append(new_ref)

    if locations is not None:
        for location in locations:
            new_ref = extract_references(location)
            if new_ref is not None:
                refs.append(new_ref)

    if healthcare_services is not None:
        for healthcare_service in healthcare_services:
            new_ref = extract_references(healthcare_service)
            if new_ref is not None:
                refs.append(new_ref)

    if endpoints is not None:
        for endpoint in endpoints:
            new_ref = extract_references(endpoint)
            if new_ref is not None:
                refs.append(new_ref)

    return refs


def _get_healthcare_service_references(model: HealthcareService) -> List[Reference]:
    refs: List[Reference] = []
    locations = model.location
    coverage_areas = model.coverageArea
    endpoints = model.endpoint
    provided_by = model.providedBy

    if locations is not None:
        for loc in locations:
            new_ref = extract_references(loc)
            if new_ref is not None:
                refs.append(new_ref)

    if coverage_areas is not None:
        for cov in coverage_areas:
            new_ref = extract_references(cov)
            if new_ref is not None:
                refs.append(new_ref)

    if endpoints is not None:
        for endpoint in endpoints:
            new_ref = extract_references(endpoint)
            if new_ref is not None:
                refs.append(new_ref)

    if provided_by is not None:
        new_ref = extract_references(provided_by)
        if new_ref is not None:
            refs.append(new_ref)

    return refs


def _make_unique(data: List[Reference]) -> List[Reference]:
    unique_str = list(set([json.dumps(d.model_dump()) for d in data]))
    unique_dicts = [json.loads(i) for i in unique_str]

    return [Reference(**d) for d in unique_dicts]


def get_references(
    data: DomainResource,
) -> List[Reference]:
    """
    Takes a FHIR DomainResource as an argument and returns a list of References.
    """
    refs: List[Reference] = []
    if isinstance(data, Organization):
        refs.extend(_get_org_references(data))

    if isinstance(data, Endpoint):
        refs.extend(_get_endpoint_references(data))

    if isinstance(data, OrganizationAffiliation):
        refs.extend(_get_org_affiliation_references(data))

    if isinstance(data, Location):
        refs.extend(_get_location_references(data))

    if isinstance(data, Practitioner):
        refs.extend(_get_practitioner_references(data))

    if isinstance(data, PractitionerRole):
        refs.extend(_get_practitioner_role_references(data))

    if isinstance(data, HealthcareService):
        refs.extend(_get_healthcare_service_references(data))

    return _make_unique(refs)
