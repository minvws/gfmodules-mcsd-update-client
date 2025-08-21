from copy import deepcopy
from typing import Any
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.healthcareservice import HealthcareService
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.reference import Reference
from fhir.resources.R4B.organizationaffiliation import OrganizationAffiliation
from fhir.resources.R4B.endpoint import Endpoint
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.practitioner import Practitioner, PractitionerQualification
from fhir.resources.R4B.practitionerrole import PractitionerRole

from app.services.fhir.bundle.bundle_utils import get_resource_from_reference
from app.services.fhir.references.reference_extractor import extract_references


def _namespace_reference(data: Any, namespace: str) -> Reference | None:
    ref = extract_references(data)
    if ref is None or ref.reference is None:
        return None
    res_type, _id = get_resource_from_reference(ref.reference)
    if res_type is None or _id is None:
        return None

    ref.reference = f"{res_type}/{namespace}-{_id}"
    return ref


def _namesapce_organization_references(
    data: Organization, namespace: str
) -> Organization:
    endpoints = data.endpoint
    part_of = data.partOf
    if endpoints is not None:
        for i, ep in enumerate(endpoints):
            new_ref = _namespace_reference(ep, namespace)
            if new_ref is not None:
                endpoints[i] = new_ref

        data.endpoint = endpoints

    if part_of is not None:
        new_ref = _namespace_reference(part_of, namespace)
        if new_ref is not None:
            data.partOf = new_ref
    return data


def _namespace_endpoint_references(data: Endpoint, namespace: str) -> Endpoint:
    managing_org = deepcopy(data.managingOrganization)
    if managing_org is not None:
        new_ref = _namespace_reference(managing_org, namespace)
        if new_ref is not None:
            data.managingOrganization = new_ref

    return data


def _namespace_organization_affiliation_references(
    data: OrganizationAffiliation, namespace: str
) -> OrganizationAffiliation:
    organization = data.organization
    participating_org = data.participatingOrganization
    network = data.network
    location = data.location
    healthcare_service = data.healthcareService
    endpoints = data.endpoint

    if organization is not None:
        new_ref = _namespace_reference(organization, namespace)
        if new_ref is not None:
            data.organization = new_ref

    if participating_org is not None:
        new_ref = _namespace_reference(participating_org, namespace)
        if new_ref is not None:
            data.participatingOrganization = new_ref

    if network is not None:
        for i, org in enumerate(network):
            new_ref = _namespace_reference(org, namespace)
            if new_ref is not None:
                network[i] = new_ref

        data.network = network

    if location is not None:
        for i, loc in enumerate(location):
            new_ref = _namespace_reference(loc, namespace)
            if new_ref is not None:
                location[i] = new_ref

        data.location = location

    if healthcare_service is not None:
        for i, service in enumerate(healthcare_service):
            new_ref = _namespace_reference(service, namespace)
            if new_ref is not None:
                healthcare_service[i] = new_ref

        data.healthcareService = healthcare_service

    if endpoints is not None:
        for i, endpoint in enumerate(endpoints):
            new_ref = _namespace_reference(endpoint, namespace)
            if new_ref is not None:
                endpoints[i] = new_ref

        data.endpoint = endpoints

    return data


def _namespace_location_references(data: Location, namespace: str) -> Location:
    managing_org = data.managingOrganization
    part_of = data.partOf
    endpoints = data.endpoint

    if managing_org is not None:
        new_ref = _namespace_reference(managing_org, namespace)
        if new_ref is not None:
            data.managingOrganization = new_ref

    if part_of is not None:
        new_ref = _namespace_reference(part_of, namespace)
        if new_ref is not None:
            data.partOf = new_ref

    if endpoints is not None:
        for i, endpoint in enumerate(endpoints):
            new_ref = _namespace_reference(endpoint, namespace)
            if new_ref is not None:
                endpoints[i] = new_ref

        data.endpoint = endpoints

    return data


def _namespace_practitioner_references(
    data: Practitioner, namespace: str
) -> Practitioner:
    qualification = data.qualification
    if qualification is not None:
        for i, q in enumerate(qualification):
            if isinstance(q, PractitionerQualification):
                issuer = q.issuer
                if issuer is not None:
                    new_ref = _namespace_reference(issuer, namespace)
                    if new_ref is not None:
                        qualification[i].issuer = new_ref

        data.qualification = qualification

    return data


def _namespace_pratitioner_role(
    data: PractitionerRole, namespace: str
) -> PractitionerRole:
    practitioner = data.practitioner
    organization = data.organization
    locations = data.location
    healthcare_services = data.healthcareService
    endpoints = data.endpoint

    if practitioner is not None:
        new_ref = _namespace_reference(practitioner, namespace)
        if new_ref is not None:
            data.practitioner = new_ref

    if organization is not None:
        new_ref = _namespace_reference(organization, namespace)
        if new_ref is not None:
            data.organization = new_ref

    if locations is not None:
        for i, loc in enumerate(locations):
            new_ref = _namespace_reference(loc, namespace)
            if new_ref is not None:
                locations[i] = new_ref
        data.location = locations

    if healthcare_services is not None:
        for i, service in enumerate(healthcare_services):
            new_ref = _namespace_reference(service, namespace)
            if new_ref is not None:
                healthcare_services[i] = new_ref

        data.healthcareService = healthcare_services

    if endpoints is not None:
        for i, endpoint in enumerate(endpoints):
            new_ref = _namespace_reference(endpoint, namespace)
            if new_ref is not None:
                endpoints[i] = new_ref

        data.endpoint = endpoints

    return data


def _namespace_healthcare_service(
    data: HealthcareService, namespace: str
) -> HealthcareService:
    provided_by = data.providedBy
    location = data.location
    coverage_area = data.coverageArea
    endpoint = data.endpoint

    if provided_by is not None:
        new_ref = _namespace_reference(provided_by, namespace)
        if new_ref is not None:
            data.providedBy = new_ref

    if location is not None:
        for i, loc in enumerate(location):
            new_ref = _namespace_reference(loc, namespace)
            if new_ref is not None:
                location[i] = new_ref

        data.location = location

    if coverage_area is not None:
        for i, cov in enumerate(coverage_area):
            new_ref = _namespace_reference(cov, namespace)
            if new_ref is not None:
                coverage_area[i] = new_ref

        data.coverageArea = coverage_area

    if endpoint is not None:
        for i, ep in enumerate(endpoint):
            new_ref = _namespace_reference(ep, namespace)
            if new_ref is not None:
                endpoint[i] = new_ref
        data.endpoint = endpoint

    return data


def namespace_resource_reference(
    data: DomainResource, namespace: str
) -> DomainResource:
    new_data = data
    if isinstance(data, Organization):
        new_data = _namesapce_organization_references(data, namespace)

    if isinstance(data, Endpoint):
        new_data = _namespace_endpoint_references(data, namespace)

    if isinstance(data, OrganizationAffiliation):
        new_data = _namespace_organization_affiliation_references(data, namespace)

    if isinstance(data, Location):
        new_data = _namespace_location_references(data, namespace)

    if isinstance(data, Practitioner):
        new_data = _namespace_practitioner_references(data, namespace)

    if isinstance(data, PractitionerRole):
        new_data = _namespace_pratitioner_role(data, namespace)

    if isinstance(data, HealthcareService):
        new_data = _namespace_healthcare_service(data, namespace)

    return new_data
