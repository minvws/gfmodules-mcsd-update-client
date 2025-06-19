import requests
from fastapi.encoders import jsonable_encoder
from seeds.generate_data import DataGenerator
import sys

generator = DataGenerator()

def generate_data():
    if len(sys.argv) != 2:
        raise ValueError("Usage: python seed_hapi.py <mock_supplier_url> \n For example: python seed_hapi.py http://example.com/fhir/")

    _SUPPLIER_URL = sys.argv[1]

    for x in range(10):
        print(f"Generating data set {x+1}")

        endpoint = generator.generate_endpoint()
        print("Generated endpoint")
        endpoint_response = requests.post(
            url=_SUPPLIER_URL+"Endpoint",
            json=jsonable_encoder(endpoint.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        endpoint_id = endpoint_response.json().get("id")
        print(f"Posted endpoint with ID: {endpoint_id}")

        org = generator.generate_organization(endpoint_ids=[endpoint_id])
        print("Generated organization")
        org_response = requests.post(
            url=_SUPPLIER_URL+"Organization",
            json=jsonable_encoder(org.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        org_id = org_response.json().get("id")
        print(f"Posted organization with ID: {org_id}")

        child_org = generator.generate_organization(
            endpoint_ids=[endpoint_id],
            part_of=org_id,
        )
        print("Generated child organization")
        child_response = requests.post(
            url=_SUPPLIER_URL+"Organization",
            json=jsonable_encoder(child_org.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        child_org_id = child_response.json().get("id")
        print(f"Posted child organization with ID: {child_org_id}")

        loc = generator.generate_location(
            managing_org=org_id,
            endpoint_ids=[endpoint_id],
        )
        print("Generated location")
        loc_response = requests.post(
            url=_SUPPLIER_URL+"Location",
            json=jsonable_encoder(loc.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        loc_id = loc_response.json().get("id")
        print(f"Posted location with ID: {loc_id}")

        child_loc = generator.generate_location(
            managing_org=org_id,
            endpoint_ids=[endpoint_id],
            part_of_location=loc_id,
        )
        print("Generated child location")
        child_loc_resp = requests.post(
            url=_SUPPLIER_URL+"Location",
            json=jsonable_encoder(child_loc.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        child_loc_response_id = child_loc_resp.json().get("id")
        print("Posted child location with ID: ", child_loc_response_id)

        health_serv = generator.generate_healthcare_service(
            provided_by_org=org_id,
            location_ids=[loc_id],
            coverage_area_locations=[loc_id],
            endpoint_ids=[endpoint_id],
        )
        print("Generated healthcare service")
        health_serv_response = requests.post(
            url=_SUPPLIER_URL+"HealthcareService",
            json=jsonable_encoder(health_serv.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        health_serv_id = health_serv_response.json().get("id")
        print(f"Posted healthcare service with ID: {health_serv_id}")

        practitioner = generator.generate_practitioner(
            qualification_issuer_org_ids=[org_id],
        )
        print("Generated practitioner")
        practitioner_response = requests.post(
            url=_SUPPLIER_URL+"Practitioner",
            json=jsonable_encoder(practitioner.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        practitioner_id = practitioner_response.json().get("id")
        print(f"Posted practitioner with ID: {practitioner_id}")

        practitioner_role = generator.generate_practitioner_role(
            practitioner_id=practitioner_id,
            organization_id=org_id,
            location_ids=[loc_id],
            healthcare_service_ids=[health_serv_id],
            endpoint_ids=[endpoint_id],
        )
        print("Generated practitioner role")
        practitioner_role_response = requests.post(
            url=_SUPPLIER_URL+"PractitionerRole",
            json=jsonable_encoder(practitioner_role.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        practitioner_role_response_id = practitioner_role_response.json().get("id")
        print("Posted practitioner role with ID: ", practitioner_role_response_id)

        # Setup some affiliations between multiple organizations
        org2 = generator.generate_organization(endpoint_ids=[endpoint_id])
        print("Generated second organization")
        org2_response = requests.post(
            url=_SUPPLIER_URL+"Organization",
            json=jsonable_encoder(org2.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        org2_id = org2_response.json().get("id")
        print(f"Posted second organization with ID: {org2_id}")

        org_afil = generator.generate_organization_affiliation(
            organization_id=org_id,
            participating_organization_id=org2_id,
            network_org_ids=[org_id, child_org_id],
            location_ids=[loc_id],
            healthcare_service_ids=[health_serv_id],
            endpoint_ids=[endpoint_id],
        )
        print("Generated organization affiliation")
        org_afil_response = requests.post(
            url=_SUPPLIER_URL+"OrganizationAffiliation",
            json=jsonable_encoder(org_afil.model_dump()),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        org_afil_id = org_afil_response.json().get("id")
        print("Posted organization affiliation with ID: ", org_afil_id)

if __name__ == "__main__":
    generate_data()