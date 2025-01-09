from uuid import UUID, uuid4

from faker import Faker
from fhir.resources.R4B.address import Address
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.endpoint import Endpoint

from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.organization import OrganizationContact
from fhir.resources.R4B.reference import Reference
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.healthcareservice import HealthcareService
from fhir.resources.R4B.organizationaffiliation import OrganizationAffiliation
from fhir.resources.R4B.practitioner import Practitioner, PractitionerQualification
from fhir.resources.R4B.practitionerrole import PractitionerRole


class DataGenerator:
    fake: Faker

    def __init__(self) -> None:
        self.fake = Faker()

    def generate_organization(
        self,
        endpoint_id: UUID | None = None,
        part_of: UUID | None = None,
    ) -> Organization:
        return Organization(
            identifier=[
                (Identifier(system="http://example.org/org", value=str(uuid4()))),
                (
                    Identifier(
                        system="http://fhir.nl/fhir/NamingSystem/ura",
                        value=str(self.fake.random_number(8, True)),
                    )
                ),
            ],
            active=self.fake.boolean(),
            type=[
                CodeableConcept(
                    coding=[
                        Coding(
                            system="http://example.org/org-type",
                            code=self.fake.random_element(
                                elements=("hospital", "clinic", "pharmacy", "lab")
                            ),
                            display=self.fake.random_element(
                                elements=("Hospital", "Clinic", "Pharmacy", "Lab")
                            ),
                        )
                    ]
                )
            ],
            name=self.fake.company(),
            contact=[self.generate_fake_contact()],
            address=[self.generate_address()],
            endpoint=(
                [{"reference": f"Endpoint/{endpoint_id}"}]
                if endpoint_id is not None
                else None
            ),
            partOf=(
                {"reference": f"Organization/{part_of}"}
                if part_of is not None
                else None
            ),
        )

    def generate_fake_contact(self) -> OrganizationContact:
        return OrganizationContact(
            name=HumanName(
                use=self.fake.random_element(
                    elements=(
                        "usual",
                        "official",
                        "temp",
                        "nickname",
                        "anonymous",
                        "old",
                        "maiden",
                    )
                ),
                text=self.fake.name(),
                family=self.fake.last_name(),
                given=[self.fake.first_name()],
            ),
            address=self.generate_address(),
        )

    def generate_address(self):
        return Address(
            use=self.fake.random_element(
                elements=("home", "work", "temp", "old", "billing")
            ),
            type=self.fake.random_element(elements=("postal", "physical", "both")),
            text=self.fake.address(),
            city=self.fake.city(),
            state=self.fake.state_abbr(),
            postalCode=self.fake.postcode(),
            country=self.fake.country(),
        )

    def generate_endpoint(
        self,
        org_fhir_id: str | None = None,
    ) -> Endpoint:
        return Endpoint(
            identifier=[
                (Identifier(system="http://example.org/endpoint", value=str(uuid4()))),
            ],
            connectionType=Coding(
                system="http://example.org/connection-type",
                code=self.fake.random_element(
                    elements=("hl7-fhir-rest", "hl7-fhir-messaging")
                ),
                display=self.fake.random_element(
                    elements=("HL7 FHIR REST", "HL7 FHIR Messaging")
                ),
            ),
            name=self.fake.company(),
            payloadType=[
                CodeableConcept(
                    coding=[
                        Coding(
                            system="http://example.org/payload-type",
                            code=self.fake.random_element(
                                elements=(
                                    "application/fhir+json",
                                    "application/fhir+xml",
                                )
                            ),
                            display=self.fake.random_element(
                                elements=("FHIR JSON", "FHIR XML")
                            ),
                        )
                    ]
                )
            ],
            managingOrganization=(
                Reference.construct(reference="Organization/" + str(org_fhir_id))
                if org_fhir_id
                else None
            ),
            address=self.fake.uri(),
            header=[
                self.fake.random_element(
                    elements=(
                        "Authorization: Bearer token",
                        "Content-Type: application/fhir+json",
                    )
                )
            ],
            payloadMimeType=[
                self.fake.random_element(
                    elements=("application/fhir+json", "application/fhir+xml")
                )
            ],
            status=(
                self.fake.random_element(
                    elements=("active", "suspended", "error", "off", "entered-in-error")
                )
            ),
        )

    def generate_location(
        self,
        part_of: UUID | None = None,
        endpoint_id: UUID | None = None,
        organization_id: UUID | None = None,
    ) -> Location:
        return Location(
            identifier=[
                Identifier(system="http://example.org/location", value=str(uuid4()))
            ],
            status=self.fake.random_element(
                elements=("active", "suspended", "inactive")
            ),
            name=self.fake.company(),
            description=self.fake.sentence(),
            managingOrganization=(
                Reference.construct(reference="Organization/" + str(organization_id))
                if organization_id
                else None
            ),
            partOf=(
                Reference.construct(reference="Location/" + str(part_of))
                if part_of is not None
                else None
            ),
            address=self.generate_address(),
            endpoint=(
                [Reference.construct(reference="Endpoint/" + str(endpoint_id))]
                if endpoint_id is not None
                else None
            ),
        )

    def generate_healthcare_service(
        self,
        provided_by: UUID | None = None,
        location_id: UUID | None = None,
        coverage_area: UUID | None = None,
        endpoint_id: UUID | None = None,
    ) -> HealthcareService:
        return HealthcareService(
            identifier=[
                Identifier(
                    system="http://example.org/healthcare-service", value=str(uuid4())
                )
            ],
            providedBy=(
                Reference.construct(reference="Organization/" + str(provided_by))
                if provided_by is not None
                else None
            ),
            location=(
                [Reference.construct(reference="Location/" + str(location_id))]
                if location_id is not None
                else None
            ),
            name=self.fake.company(),
            comment=self.fake.sentence(),
            extraDetails=self.fake.sentence(),
            coverageArea=(
                [Reference.construct(reference="Location/" + str(coverage_area))]
                if coverage_area is not None
                else None
            ),
            appointmentRequired=self.fake.boolean(),
            endpoint=(
                [Reference.construct(reference="Endpoint/" + str(endpoint_id))]
                if endpoint_id is not None
                else None
            ),
        )

    def generate_organization_affiliation(
        self,
        organization_id: UUID | None = None,
        participating_organization_id: UUID | None = None,
        network_id: list[UUID] = None,
        location_id: UUID | None = None,
        healthcare_service_id: UUID | None = None,
        endpoint_id: UUID | None = None,
    ) -> OrganizationAffiliation:
        if network_id is None:
            network_id = []
        return OrganizationAffiliation(
            identifier=[
                Identifier(
                    system="http://example.org/organization-affiliation",
                    value=str(uuid4()),
                )
            ],
            active=self.fake.boolean(),
            organization=(
                (Reference.construct(reference="Organization/" + str(organization_id)))
                if organization_id is not None
                else None
            ),
            participatingOrganization=(
                (
                    Reference.construct(
                        reference="Organization/" + str(participating_organization_id)
                    )
                )
                if participating_organization_id is not None
                else None
            ),
            network=[
                Reference.construct(reference="Organization/" + str(network))
                for network in network_id
            ],
            location=(
                [Reference.construct(reference="Location/" + str(location_id))]
                if location_id is not None
                else None
            ),
            healthcareService=(
                [
                    Reference.construct(
                        reference="HealthcareService/" + str(healthcare_service_id)
                    )
                ]
                if healthcare_service_id is not None
                else None
            ),
            endpoint=(
                [Reference.construct(reference="Endpoint/" + str(endpoint_id))]
                if endpoint_id is not None
                else None
            ),
        )

    def generate_practioner(
        self,
        organization_id: UUID | None = None,
    ) -> Practitioner:
        return Practitioner(
            identifier=[
                Identifier(system="http://example.org/practitioner", value=str(uuid4()))
            ],
            active=self.fake.boolean(),
            name=[
                HumanName(
                    use=self.fake.random_element(
                        elements=(
                            "usual",
                            "official",
                            "temp",
                            "nickname",
                            "anonymous",
                            "old",
                            "maiden",
                        )
                    ),
                    text=self.fake.name(),
                    family=self.fake.last_name(),
                    given=[self.fake.first_name()],
                )
            ],
            address=[self.generate_address()],
            qualification=[self.generate_practitioner_qualification(organization_id)],
        )

    def generate_practitioner_qualification(
        self, organization_id: UUID | None = None
    ) -> PractitionerQualification:
        return PractitionerQualification(
            code=CodeableConcept(
                coding=[
                    Coding(
                        system="http://example.org/practitioner-qualification",
                        code=self.fake.random_element(
                            elements=("MD", "DO", "NP", "PA", "RN", "LPN")
                        ),
                        display=self.fake.random_element(
                            elements=(
                                "Doctor of Medicine",
                                "Doctor of Osteopathy",
                                "Nurse Practitioner",
                                "Physician Assistant",
                                "Registered Nurse",
                                "Licensed Practical Nurse",
                            )
                        ),
                    )
                ]
            ),
            issuer=(
                Reference.construct(reference="Organization/" + str(organization_id))
                if organization_id is not None
                else None
            ),
            identifier=[
                Identifier(
                    system="http://example.org/practitioner-qualification",
                    value=str(uuid4()),
                )
            ],
        )

    def generate_practitioner_role(
        self,
        practitioner_id: UUID | None = None,
        organization_id: UUID | None = None,
        location_id: UUID | None = None,
        healthcare_service_id: UUID | None = None,
        endpoint_id: UUID | None = None,
    ) -> PractitionerRole:
        return PractitionerRole(
            identifier=[
                Identifier(
                    system="http://example.org/practitioner-role", value=str(uuid4())
                )
            ],
            active=self.fake.boolean(),
            practitioner=(
                Reference.construct(reference="Practitioner/" + str(practitioner_id))
                if practitioner_id is not None
                else None
            ),
            organization=(
                Reference.construct(reference="Organization/" + str(organization_id))
                if organization_id is not None
                else None
            ),
            code=[
                CodeableConcept(
                    coding=[
                        Coding(
                            system="http://example.org/practitioner-role",
                            code=self.fake.random_element(
                                elements=("doctor", "nurse", "pharmacist", "researcher")
                            ),
                            display=self.fake.random_element(
                                elements=("Doctor", "Nurse", "Pharmacist", "Researcher")
                            ),
                        )
                    ]
                )
            ],
            location=(
                [Reference.construct(reference="Location/" + str(location_id))]
                if location_id is not None
                else None
            ),
            healthcareService=(
                [
                    Reference.construct(
                        reference="HealthcareService/" + str(healthcare_service_id)
                    )
                ]
                if healthcare_service_id is not None
                else None
            ),
            endpoint=(
                [Reference.construct(reference="Endpoint/" + str(endpoint_id))]
                if endpoint_id is not None
                else None
            ),
        )
