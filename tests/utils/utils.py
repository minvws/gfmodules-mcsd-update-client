import os
import platform
import shutil
from fhir.resources.R4B.bundle import BundleEntry
import psutil
from typing import Any, Dict, List, Set
from uuid import uuid4

from app.models.adjacency.node import (
    Node,
    NodeReference,
    NodeUpdateData,
)
from app.models.fhir.types import McsdResources
from app.services.fhir.fhir_service import FhirService
import numpy as np
from app.container import get_database
from app.db.repositories.resource_map_repository import ResourceMapRepository
from .mcsd_resource_gen import (
    generate_history_bundles,
    generate_post_bundle,
    setup_fhir_resource,
)


def create_file_structure(
    base_path: str, resource_count: int, version_count: int, max_depth: int = 1
) -> None:
    shutil.rmtree(base_path, ignore_errors=True)
    os.makedirs(base_path, exist_ok=True)

    for resource_type in McsdResources:
        resource_folder = os.path.join(base_path, resource_type.value)
        os.makedirs(resource_folder, exist_ok=True)

        for _ in range(resource_count):
            resource_id = str(uuid4())
            for version in range(version_count):
                setup_fhir_resource(
                    base_path,
                    resource_type,
                    max_depth=max_depth,
                    version=version,
                    resource_id=resource_id,
                )

    generate_history_bundles(base_path)
    generate_post_bundle(base_path)


def generate_report(
    test_name: str,
    durations: Dict[str, List[float]],
    iterations: int,
    monitor_data: List[Dict[str, Any]],
    total_resources: int,
) -> Dict[str, Any]:
    memory = [x["memory_mb"] for x in monitor_data]
    cpu = [x["cpu_percent"] for x in monitor_data]
    reads = [x["read_bytes"] for x in monitor_data]
    writes = [x["write_bytes"] for x in monitor_data]

    def summarize(values: List[float]) -> Dict[str, Any]:
        return {
            "p1": np.percentile(values, 1),
            "p50": np.percentile(values, 50),
            "p75": np.percentile(values, 75),
            "p99": np.percentile(values, 99),
            "max": max(values),
        }

    return {
        "test_name": test_name,
        "hardware": {
            "cpu": platform.processor(),
            "cores": psutil.cpu_count(logical=False),
            "logical_cores": psutil.cpu_count(logical=True),
            "ram_gb": round(psutil.virtual_memory().total / (1024**3), 2),
            "platform": platform.platform(),
        },
        "iterations": iterations,
        "duration_milliseconds": {
            "total_with_patch": summarize(durations["total"]),
            "true_update": summarize(durations["true_update"]),
            "patch": summarize(durations["patch"]),
        },
        "total_resources": total_resources,
        "resource_usage_summary": {
            "cpu_percent": summarize(cpu),
            "memory_mb": summarize(memory),
            "disk_read_mb": summarize([b / 1024**2 for b in reads]),
            "disk_write_mb": summarize([b / 1024**2 for b in writes]),
        },
    }


def check_if_in_db(directory_id: str, ids_set: Set[str]) -> int:
    errors = 0
    print("Checking if all resources are in the database")
    db = get_database()
    with db.get_db_session() as session:
        repo = session.get_repository(ResourceMapRepository)
        result = repo.find(directory_id=directory_id)
        for resource_item in result:
            if (
                resource_item.directory_resource_id in ids_set
            ):  # Ids set does not contain deleted resources as latest version
                ids_set.discard(resource_item.directory_resource_id)
    if ids_set:
        print(f"Resource {ids_set} not found in the database")
        errors += len(ids_set)
    return errors


def create_mock_node(
    bundle_entry: BundleEntry,
    node_refs: List[NodeReference],
    fhir_service: FhirService,
    directory_hash: int | None = None,
    update_client_hash: int | None = None,
    node_update_data: NodeUpdateData | None = None,
) -> Node:
    """
    Helper function to create mock nodes
    """
    res_type, id = fhir_service.get_resource_type_and_id_from_entry(bundle_entry)
    method = fhir_service.get_request_method_from_entry(bundle_entry)

    return Node(
        resource_id=id,
        resource_type=res_type,
        references=node_refs,
        method=method,
        directory_entry=bundle_entry,
        directory_hash=directory_hash,
        update_client_hash=update_client_hash,
        update_data=node_update_data,
    )
