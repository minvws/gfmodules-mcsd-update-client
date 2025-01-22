from uuid import UUID, uuid4

from faker import Faker
from fhir.resources.R4B.address import Address
from fhir.resources.R4B.careplan import CarePlan, CarePlanActivity
from fhir.resources.R4B.careteam import CareTeam, CareTeamParticipant
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.endpoint import Endpoint
from fhir.resources.R4B.healthcareservice import HealthcareService
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.meta import Meta
from fhir.resources.R4B.organization import Organization, OrganizationContact
from fhir.resources.R4B.organizationaffiliation import OrganizationAffiliation
from fhir.resources.R4B.patient import Patient
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.practitioner import Practitioner, PractitionerQualification
from fhir.resources.R4B.practitionerrole import PractitionerRole
from fhir.resources.R4B.reference import Reference


class DataGenerator:
    fake: Faker

    def __init__(self) -> None:
        self.fake = Faker()

    def generate_organization(
        self,
        endpoint_id: UUID | None = None,
        part_of: UUID | None = None,
    ) -> Organization:
        return Organization.model_construct(
            identifier=[
                (
                    Identifier.model_construct(
                        system="http://example.org/org", value=str(uuid4())
                    )
                ),
                (
                    Identifier.model_construct(
                        system="http://fhir.nl/fhir/NamingSystem/ura",
                        value=str(self.fake.random_number(8, True)),
                    )
                ),
            ],
            active=self.fake.boolean(),
            type=[
                CodeableConcept.model_construct(
                    coding=[
                        Coding.model_construct(
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
        return OrganizationContact.model_construct(
            name=HumanName.model_construct(
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
        return Address.model_construct(
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
        return Endpoint.model_construct(
            identifier=[
                (
                    Identifier.model_construct(
                        system="http://example.org/endpoint", value=str(uuid4())
                    )
                ),
            ],
            connectionType=Coding.model_construct(
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
                CodeableConcept.model_construct(
                    coding=[
                        Coding.model_construct(
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
                Reference.model_construct(reference="Organization/" + str(org_fhir_id))
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
        return Location.model_construct(
            identifier=[
                Identifier.model_construct(
                    system="http://example.org/location", value=str(uuid4())
                )
            ],
            status=self.fake.random_element(
                elements=("active", "suspended", "inactive")
            ),
            name=self.fake.company(),
            description=self.fake.sentence(),
            managingOrganization=(
                Reference.model_construct(
                    reference="Organization/" + str(organization_id)
                )
                if organization_id
                else None
            ),
            partOf=(
                Reference.model_construct(reference="Location/" + str(part_of))
                if part_of is not None
                else None
            ),
            address=self.generate_address(),
            endpoint=(
                [Reference.model_construct(reference="Endpoint/" + str(endpoint_id))]
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
        return HealthcareService.model_construct(
            identifier=[
                Identifier.model_construct(
                    system="http://example.org/healthcare-service", value=str(uuid4())
                )
            ],
            providedBy=(
                Reference.model_construct(reference="Organization/" + str(provided_by))
                if provided_by is not None
                else None
            ),
            location=(
                [Reference.model_construct(reference="Location/" + str(location_id))]
                if location_id is not None
                else None
            ),
            name=self.fake.company(),
            comment=self.fake.sentence(),
            extraDetails=self.fake.sentence(),
            coverageArea=(
                [Reference.model_construct(reference="Location/" + str(coverage_area))]
                if coverage_area is not None
                else None
            ),
            appointmentRequired=self.fake.boolean(),
            endpoint=(
                [Reference.model_construct(reference="Endpoint/" + str(endpoint_id))]
                if endpoint_id is not None
                else None
            ),
        )

    def generate_organization_affiliation(
        self,
        organization_id: UUID | None = None,
        participating_organization_id: UUID | None = None,
        network_id: list[UUID] | None = None,
        location_id: UUID | None = None,
        healthcare_service_id: UUID | None = None,
        endpoint_id: UUID | None = None,
    ) -> OrganizationAffiliation:
        if network_id is None:
            network_id = []
        return OrganizationAffiliation.model_construct(
            identifier=[
                Identifier.model_construct(
                    system="http://example.org/organization-affiliation",
                    value=str(uuid4()),
                )
            ],
            active=self.fake.boolean(),
            organization=(
                (
                    Reference.model_construct(
                        reference="Organization/" + str(organization_id)
                    )
                )
                if organization_id is not None
                else None
            ),
            participatingOrganization=(
                (
                    Reference.model_construct(
                        reference="Organization/" + str(participating_organization_id)
                    )
                )
                if participating_organization_id is not None
                else None
            ),
            network=[
                Reference.model_construct(reference="Organization/" + str(network))
                for network in network_id
            ],
            location=(
                [Reference.model_construct(reference="Location/" + str(location_id))]
                if location_id is not None
                else None
            ),
            healthcareService=(
                [
                    Reference.model_construct(
                        reference="HealthcareService/" + str(healthcare_service_id)
                    )
                ]
                if healthcare_service_id is not None
                else None
            ),
            endpoint=(
                [Reference.model_construct(reference="Endpoint/" + str(endpoint_id))]
                if endpoint_id is not None
                else None
            ),
        )

    def generate_practioner(
        self,
        organization_id: UUID | None = None,
    ) -> Practitioner:
        return Practitioner.model_construct(
            identifier=[
                Identifier.model_construct(
                    system="http://example.org/practitioner", value=str(uuid4())
                )
            ],
            active=self.fake.boolean(),
            name=[
                HumanName.model_construct(
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
        return PractitionerQualification.model_construct(
            code=CodeableConcept.model_construct(
                coding=[
                    Coding.model_construct(
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
                Reference.model_construct(
                    reference="Organization/" + str(organization_id)
                )
                if organization_id is not None
                else None
            ),
            identifier=[
                Identifier.model_construct(
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
        return PractitionerRole.model_construct(
            identifier=[
                Identifier.model_construct(
                    system="http://example.org/practitioner-role", value=str(uuid4())
                )
            ],
            active=self.fake.boolean(),
            practitioner=(
                Reference.model_construct(
                    reference="Practitioner/" + str(practitioner_id)
                )
                if practitioner_id is not None
                else None
            ),
            organization=(
                Reference.model_construct(
                    reference="Organization/" + str(organization_id)
                )
                if organization_id is not None
                else None
            ),
            code=[
                CodeableConcept.model_construct(
                    coding=[
                        Coding.model_construct(
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
                [Reference.model_construct(reference="Location/" + str(location_id))]
                if location_id is not None
                else None
            ),
            healthcareService=(
                [
                    Reference.model_construct(
                        reference="HealthcareService/" + str(healthcare_service_id)
                    )
                ]
                if healthcare_service_id is not None
                else None
            ),
            endpoint=(
                [Reference.model_construct(reference="Endpoint/" + str(endpoint_id))]
                if endpoint_id is not None
                else None
            ),
        )

    def generate_patient(self) -> Patient:
        return Patient.model_construct(
            identifier=[
                Identifier.model_construct(
                    system="http://example.org/patient", value=str(uuid4())
                )
            ],
            active=self.fake.boolean(),
            name=[
                HumanName.model_construct(
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
        )

    def generate_careplan(
        self, practitioner_id: UUID, patient_id: UUID, organization_id: UUID
    ) -> CarePlan:
        subject = Reference.model_construct(
            reference=f"http://example.org/org/Patient/{patient_id}",
            type="Patient",
            identifier=Identifier.model_construct(
                system="http://fhir.nl/fhir/NamingSystem/bsn",
                value=str(uuid4()),
                assigner=Reference.model_construct(
                    identifier=Identifier.model_construct(
                        system="http://fhir.nl/fhir/NamingSystem/ura",
                        value=str(self.fake.random_number(8, True)),
                    )
                ),
            ),
        )
        organization_ura_value = str(self.fake.random_number(8, True))
        care_team = CareTeam.model_construct(
            subject=subject,
            participant=[
                CareTeamParticipant.model_construct(
                    period=Period.model_construct(start="2024-08-27"), member=subject
                ),
                CareTeamParticipant.model_construct(
                    period=Period.model_construct(start="2024-08-27"),
                    member=Reference.model_construct(
                        reference=f"http://example.org/org/Organization/{organization_id}",
                        type="Organization",
                        identifier=Identifier.model_construct(
                            system="http://fhir.nl/fhir/NamingSystem/ura",
                            value=organization_ura_value,
                            assigner=Reference.model_construct(
                                identifier=Identifier.model_construct(
                                    system="http://fhir.nl/fhir/NamingSystem/ura",
                                    value=organization_ura_value,
                                )
                            ),
                        ),
                    ),
                ),
            ],
        )
        return CarePlan.model_construct(
            meta=Meta.model_construct(
                versionId="1",
                profile=[
                    "http://santeonnl.github.io/shared-care-planning/StructureDefinition/SCPCareplan"
                ],
            ),
            category=[
                CodeableConcept.model_construct(
                    coding=[
                        Coding.model_construct(
                            system="http://snomed.info/sct",
                            # TODO: figure out code and display for careplan categories
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
            contained=[care_team],
            status=self.fake.random_element(
                elements=(
                    "draft",
                    "active",
                    "on-hold",
                    "revoked",
                    "completed",
                    "entered-in-error",
                    "unknown",
                )
            ),
            intent=self.fake.random_element(
                elements=("proposal", "plan", "order", "option")
            ),
            subject=subject,
            careTeam=[Reference.model_construct(reference=f"CareTeam/{care_team.id}")],
            author=Reference.model_construct(
                reference=f"http://example.org/org/PractitionerRole/{practitioner_id}",
                type="PractitionerRole",
                identifier=Identifier.model_construct(
                    system="http://fhir.nl/fhir/NamingSystem/uzi",
                    value=str(uuid4()),
                    assigner=Identifier.model_construct(
                        system="http://fhir.nl/fhir/NamingSystem/ura",
                        value=str(self.fake.random_number(8, True)),
                    ),
                ),
            ),
            activity=[
                CarePlanActivity.model_construct(
                    reference=Reference.model_construct(
                        reference=self.fake.random_element(
                            elements=(
                                "Appointment",
                                "CommunicationRequest",
                                "DeviceRequest",
                                "MedicationRequest",
                                "NutritionOrder",
                                "Task",
                                "ServiceRequest",
                                "VisionPrescription",
                                "RequestGroup",
                            )
                        )
                        + f"/{uuid4()}"
                    )
                )
            ],
        )
