from typing import List

from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.endpoint import Endpoint

from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.healthcareservice import HealthcareService
from fhir.resources.R4B.organizationaffiliation import OrganizationAffiliation
from fhir.resources.R4B.practitioner import Practitioner, PractitionerQualification
from fhir.resources.R4B.practitionerrole import PractitionerRole


class DataGenerator:
    def generate_organization(
        self,
        endpoint_ids: List[str | None] = None,
        res_id: str | None = None,
        part_of: str | None = None,
    ) -> Organization:
        return Organization(
            id=res_id,
            endpoint=[
                {"reference": f"Endpoint/{endpoint_id}"}
                for endpoint_id in endpoint_ids
                if endpoint_id is not None
            ]
            if endpoint_ids is not None
            else None,
            partOf=(
                {"reference": f"Organization/{part_of}"}
                if part_of is not None
                else None
            ),
        )

    def generate_endpoint(
        self,
        res_id: str | None = None,
        org_fhir_id: str | None = None,
    ) -> Endpoint:
        return Endpoint(
            id=res_id,
            connectionType=Coding(
                system="http://example.org/connection-type",
                code="hl7-fhir-rest",
                display="HL7 FHIR REST",
            ),
            payloadType=[
                CodeableConcept(
                    coding=[
                        Coding(
                            system="http://example.org/payload-type",
                            code="application/fhir+json",
                            display="FHIR JSON",
                        )
                    ]
                )
            ],
            managingOrganization=(
                {"reference": f"Organization/{org_fhir_id}"} if org_fhir_id else None
            ),
            address="http://example.org/",
            status="active",
        )

    def generate_location(
        self,
        endpoint_ids: List[str | None] = None,
        res_id: str | None = None,
        part_of_location: str | None = None,
        managing_org: str | None = None,
    ) -> Location:
        return Location(
            id=res_id,
            managingOrganization=(
                {"reference": f"Organization/{managing_org}"} if managing_org else None
            ),
            partOf=(
                {"reference": f"Location/{part_of_location}"}
                if part_of_location is not None
                else None
            ),
            endpoint=[
                {"reference": f"Endpoint/{endpoint_id}"}
                for endpoint_id in endpoint_ids
                if endpoint_id is not None
            ]
            if endpoint_ids is not None
            else None,
        )

    def generate_healthcare_service(
        self,
        location_ids: List[str | None] = None,
        coverage_area_locations: List[str | None] = None,
        endpoint_ids: List[str | None] = None,
        res_id: str | None = None,
        provided_by_org: str | None = None,
    ) -> HealthcareService:
        return HealthcareService(
            id=res_id,
            providedBy=(
                {"reference": f"Organization/{provided_by_org}"}
                if provided_by_org is not None
                else None
            ),
            location=[
                {"reference": f"Location/{location_id}"}
                for location_id in location_ids
                if location_id is not None
            ]
            if location_ids is not None
            else None,
            coverageArea=[
                {"reference": f"Location/{location_id}"}
                for location_id in coverage_area_locations
                if location_id is not None
            ]
            if coverage_area_locations is not None
            else None,
            endpoint=[
                {"reference": f"Endpoint/{endpoint_id}"}
                for endpoint_id in endpoint_ids
                if endpoint_id is not None
            ]
            if endpoint_ids is not None
            else None,
        )

    def generate_organization_affiliation(
        self,
        res_id: str | None = None,
        organization_id: str | None = None,
        participating_organization_id: str | None = None,
        network_org_ids: List[str | None] = None,
        location_ids: List[str | None] = None,
        healthcare_service_ids: List[str | None] = None,
        endpoint_ids: List[str | None] = None,
    ) -> OrganizationAffiliation:
        return OrganizationAffiliation(
            id=res_id,
            organization=(
                {"reference": f"Organization/{organization_id}"}
                if organization_id is not None
                else None
            ),
            participatingOrganization=(
                {"reference": f"Organization/{participating_organization_id}"}
                if participating_organization_id is not None
                else None
            ),
            network=[
                {"reference": f"Organization/{org_id}"}
                for org_id in network_org_ids
                if org_id is not None
            ]
            if network_org_ids is not None
            else None,
            location=[
                {"reference": f"Location/{location_id}"}
                for location_id in location_ids
                if location_id is not None
            ]
            if location_ids is not None
            else None,
            healthcareService=[
                {"reference": f"HealthcareService/{healthcare_service_id}"}
                for healthcare_service_id in healthcare_service_ids
                if healthcare_service_id is not None
            ]
            if healthcare_service_ids is not None
            else None,
            endpoint=[
                {"reference": f"Endpoint/{endpoint_id}"}
                for endpoint_id in endpoint_ids
                if endpoint_id is not None
            ]
            if endpoint_ids is not None
            else None,
        )

    def generate_practitioner(
        self,
        res_id: str | None = None,
        qualification_issuer_org_ids: List[str | None] = None,
    ) -> Practitioner:
        return Practitioner(
            id=res_id,
            qualification=[
                self.generate_practitioner_qualification(qualification_issuer_org_id)
                for qualification_issuer_org_id in qualification_issuer_org_ids
                if qualification_issuer_org_id is not None
            ]
            if qualification_issuer_org_ids is not None
            else None,
        )

    def generate_practitioner_qualification(
        self, qualification_issuer_org_id: str | None = None
    ) -> PractitionerQualification:
        return PractitionerQualification(
            code=CodeableConcept(
                coding=[
                    Coding(
                        system="http://example.org/practitioner-qualification",
                        code="MD",
                        display="Doctor of Medicine",
                    )
                ]
            ),
            issuer={"reference": f"Organization/{qualification_issuer_org_id}"}
            if qualification_issuer_org_id is not None
            else None,
        )

    def generate_practitioner_role(
        self,
        res_id: str | None = None,
        practitioner_id: str | None = None,
        organization_id: str | None = None,
        location_ids: List[str | None] = None,
        healthcare_service_ids: List[str | None] = None,
        endpoint_ids: List[str | None] = None,
    ) -> PractitionerRole:
        return PractitionerRole(
            id=res_id,
            practitioner={"reference": f"Practitioner/{practitioner_id}"}
            if practitioner_id is not None
            else None,
            organization={"reference": f"Organization/{organization_id}"}
            if organization_id is not None
            else None,
            location=[
                {"reference": f"Location/{location_id}"}
                for location_id in location_ids if location_id is not None
            ] if location_ids is not None else None,
            healthcareService=[
                {"reference": f"HealthcareService/{healthcare_service_id}"}
                for healthcare_service_id in healthcare_service_ids if healthcare_service_id is not None
            ] if healthcare_service_ids is not None else None,
            endpoint=[
                {"reference": f"Endpoint/{endpoint_id}"}
                for endpoint_id in endpoint_ids if endpoint_id is not None
            ] if endpoint_ids is not None else None,
        )
