from uuid import UUID

import requests
from fastapi.encoders import jsonable_encoder
from fhir.resources.R4B.resource import Resource

from app.config import get_config
from seeds.generate_data import DataGenerator

generator = DataGenerator()

SUPPLIER_URL = get_config().mock_seeder.mock_supplier_url

if SUPPLIER_URL is None:
    raise ValueError("No mock supplier URL provided, set one in the config file")


def collect_response_id(resource: Resource, context: str | None = None) -> UUID:
    resource_type = resource.get_resource_type()
    id = (
        requests.post(
            url=f"{SUPPLIER_URL}{resource_type}",
            json=jsonable_encoder(resource.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        .json()
        .get("id")
    )
    print(f"Posted {context or ''} {resource_type} with ID: {id}")
    return UUID(id)


for x in range(1, 11):
    print(f"Generating data set {x}")

    endpoint = generator.generate_endpoint()
    print("Generated endpoint")
    endpoint_id = collect_response_id(endpoint)

    org = generator.generate_organization(endpoint_id=endpoint_id)
    print("Generated organization")
    org_id = collect_response_id(org)

    child_org = generator.generate_organization(
        endpoint_id=endpoint_id,
        part_of=org_id,
    )
    print("Generated child organization")
    child_org_id = collect_response_id(child_org, "child")

    loc = generator.generate_location(organization_id=org_id, endpoint_id=endpoint_id)
    print("Generated location")
    loc_id = collect_response_id(loc)

    child_loc = generator.generate_location(
        organization_id=org_id,
        endpoint_id=endpoint_id,
        part_of=loc_id,
    )
    print("Generated child location")
    child_loc_response_id = collect_response_id(child_loc, "child")

    health_serv = generator.generate_healthcare_service(
        provided_by=org_id,
        location_id=loc_id,
        coverage_area=loc_id,
        endpoint_id=endpoint_id,
    )
    print("Generated healthcare service")
    health_serv_id = collect_response_id(health_serv)

    practitioner = generator.generate_practioner(
        organization_id=org_id,
    )
    print("Generated practitioner")
    practitioner_id = collect_response_id(practitioner)

    practitioner_role = generator.generate_practitioner_role(
        practitioner_id=practitioner_id,
        organization_id=org_id,
        location_id=loc_id,
        healthcare_service_id=health_serv_id,
        endpoint_id=endpoint_id,
    )
    print("Generated practitioner role")
    practitioner_role_response_id = collect_response_id(practitioner_role)

    # Setup some affiliations between multiple organizations
    org2 = generator.generate_organization(endpoint_id=endpoint_id)
    print("Generated second organization")
    org2_id = collect_response_id(org2, "second")

    org_afil = generator.generate_organization_affiliation(
        organization_id=org_id,
        participating_organization_id=org2_id,
        network_id=[org_id, child_org_id],
        location_id=loc_id,
        healthcare_service_id=health_serv_id,
        endpoint_id=endpoint_id,
    )
    print("Generated organization affiliation")
    org_afil_id = collect_response_id(org_afil)

    patient = generator.generate_patient()
    print("Generated patient")
    patient_id = collect_response_id(patient)

    careplan = generator.generate_careplan(
        practitioner_id=practitioner_role_response_id,
        patient_id=patient_id,
        organization_id=org_id,
    )
    print("Generated CarePlan")
    careplan_response_id = collect_response_id(careplan)
