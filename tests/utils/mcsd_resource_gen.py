from random import choice
from typing import Any, Dict
from uuid import uuid4

from app.services.request_services.supplier_request_service import McsdResources
from seeds.generate_data import DataGenerator
import os
import json

dg = DataGenerator()


def setup_fhir_resource(
    base_path: str,
    mcsd_res_type: McsdResources,
    max_depth: int,
    version: int = 0,
    resource_id: str | None = None,
) -> str | None:
    if resource_id is None:
        resource_id = str(uuid4())
    if max_depth <= 0:
        return None
    match mcsd_res_type:
        case McsdResources.ORGANIZATION_AFFILIATION:
            resource = dg.generate_organization_affiliation(
                res_id=resource_id,
                organization_id=setup_fhir_resource(
                    base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                ),
                participating_organization_id=setup_fhir_resource(
                    base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                ),
                network_org_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                    ),
                    setup_fhir_resource(
                        base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                    ),
                ],
                location_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.LOCATION, max_depth - 1, 0
                    ),
                    setup_fhir_resource(
                        base_path, McsdResources.LOCATION, max_depth - 1, 0
                    ),
                ],
                healthcare_service_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.HEALTH_CARE_SERVICE, max_depth - 1, 0
                    ),
                    setup_fhir_resource(
                        base_path, McsdResources.HEALTH_CARE_SERVICE, max_depth - 1, 0
                    ),
                ],
                endpoint_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.ENDPOINT, max_depth - 1, 0
                    )
                ],
            ).model_dump_json()
        case McsdResources.PRACTITIONER_ROLE:
            resource = dg.generate_practitioner_role(
                res_id=resource_id,
                practitioner_id=setup_fhir_resource(
                    base_path, McsdResources.PRACTITIONER, max_depth - 1, 0
                ),
                organization_id=setup_fhir_resource(
                    base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                ),
                location_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.LOCATION, max_depth - 1, 0
                    ),
                    setup_fhir_resource(
                        base_path, McsdResources.LOCATION, max_depth - 1, 0
                    ),
                ],
                healthcare_service_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.HEALTH_CARE_SERVICE, max_depth - 1, 0
                    ),
                    setup_fhir_resource(
                        base_path, McsdResources.HEALTH_CARE_SERVICE, max_depth - 1, 0
                    ),
                ],
                endpoint_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.ENDPOINT, max_depth - 1, 0
                    )
                ],
            ).model_dump_json()
        case McsdResources.HEALTH_CARE_SERVICE:
            resource = dg.generate_healthcare_service(
                res_id=resource_id,
                location_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.LOCATION, max_depth - 1, 0
                    ),
                    setup_fhir_resource(
                        base_path, McsdResources.LOCATION, max_depth - 1, 0
                    ),
                ],
                coverage_area_locations=[
                    setup_fhir_resource(
                        base_path, McsdResources.LOCATION, max_depth - 1, 0
                    ),
                    setup_fhir_resource(
                        base_path, McsdResources.LOCATION, max_depth - 1, 0
                    ),
                ],
                provided_by_org=setup_fhir_resource(
                    base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                ),
                endpoint_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.ENDPOINT, max_depth - 1, 0
                    )
                ],
            ).model_dump_json()
        case McsdResources.LOCATION:
            resource = dg.generate_location(
                res_id=resource_id,
                part_of_location=setup_fhir_resource(
                    base_path, McsdResources.LOCATION, max_depth - 1, 0
                ),
                endpoint_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.ENDPOINT, max_depth - 1, 0
                    )
                ],
                managing_org=setup_fhir_resource(
                    base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                ),
            ).model_dump_json()
        case McsdResources.PRACTITIONER:
            resource = dg.generate_practitioner(
                res_id=resource_id,
                qualification_issuer_org_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                    ),
                    setup_fhir_resource(
                        base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                    ),
                ],
            ).model_dump_json()
        case McsdResources.ENDPOINT:
            resource = dg.generate_endpoint(
                res_id=resource_id,
                org_fhir_id=setup_fhir_resource(
                    base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                ),
            ).model_dump_json()
        case McsdResources.ORGANIZATION:
            resource = dg.generate_organization(
                res_id=resource_id,
                endpoint_ids=[
                    setup_fhir_resource(
                        base_path, McsdResources.ENDPOINT, max_depth - 1, 0
                    )
                ],
                part_of=setup_fhir_resource(
                    base_path, McsdResources.ORGANIZATION, max_depth - 1, 0
                ),
            ).model_dump_json()
    save_resource_version(
        resource_id_folder=f"{base_path}/{mcsd_res_type.value}/{resource_id}",
        resource=resource,
        version=version,
    )
    return resource_id


def save_resource_version(
    resource_id_folder: str, resource: Dict[str, Any], version: int = 0
) -> None:
    os.makedirs(resource_id_folder, exist_ok=True)
    version_file = os.path.join(resource_id_folder, f"{version + 1}.json")
    with open(version_file, "w") as f:
        if isinstance(resource, str):
            resource = json.loads(resource)
            json.dump(resource, f, indent=4)


def generate_history_bundles(base_path: str) -> None:
    global_history_bundle = {
        "resourceType": "Bundle",
        "id": str(uuid4()),
        "type": "history",
        "total": 0,
        "entry": [],
    }

    for resource_type in McsdResources:
        resource_folder = os.path.join(base_path, resource_type.value)
        resource_history_bundle = {
            "resourceType": "Bundle",
            "id": str(uuid4()),
            "type": "history",
            "total": 0,
            "entry": [],
        }
        for resource_id in os.listdir(resource_folder):
            resource_id_folder = os.path.join(resource_folder, resource_id)
            versions = sorted(os.listdir(resource_id_folder))
            history_bundle = {
                "resourceType": "Bundle",
                "id": str(uuid4()),
                "type": "history",
                "total": 0,
                "entry": [],
            }

            for version in versions:
                version_file = os.path.join(resource_id_folder, version)
                with open(version_file, "r") as f:
                    resource = json.load(f)
                    methods = ["POST", "PUT"]
                    if version.replace(".json", "") != "1":
                        methods.append("DELETE")
                    entry = {
                        "fullUrl": f"{resource_type.value}/{resource_id}/{version.replace('.json', '')}",
                        "resource": resource,
                        "request": {
                            "method": choice(methods),
                            "url": f"{resource_type.value}/{resource_id}/{version.replace('.json', '')}",
                        },
                    }
                    assert isinstance(history_bundle["entry"], list)
                    assert isinstance(resource_history_bundle["entry"], list)
                    assert isinstance(global_history_bundle["entry"], list)
                    assert isinstance(history_bundle["total"], int)
                    assert isinstance(resource_history_bundle["total"], int)
                    assert isinstance(global_history_bundle["total"], int)
                    history_bundle["entry"].append(entry)
                    history_bundle["total"] += 1
                    resource_history_bundle["entry"].append(entry)
                    resource_history_bundle["total"] += 1
                    global_history_bundle["entry"].append(entry)
                    global_history_bundle["total"] += 1

            with open(
                os.path.join(
                    resource_folder, resource_id, f"{resource_id}_history.json"
                ),
                "w",
            ) as f:
                json.dump(history_bundle, f, indent=4)
        with open(
            os.path.join(resource_folder, f"{resource_type.value}_history.json"), "w"
        ) as f:
            json.dump(resource_history_bundle, f, indent=4)
    import sys
    import resource
    print(sys.getsizeof(resource_history_bundle))
    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


    with open(os.path.join(base_path, "all_resources_history.json"), "w") as f:
        json.dump(global_history_bundle, f, indent=4)
