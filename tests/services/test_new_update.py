import json
import os
import time
import threading
import psutil
import pytest

from typing import Any, Dict, List
from fastapi.testclient import TestClient
from unittest.mock import patch
import requests
from yarl import URL
from app.container import get_database
from fhir.resources.R4B.bundle import Bundle, BundleEntry
from utils.utils import (
    check_if_in_db,
    create_file_structure,
    generate_report,
)

from app.stats import Stats, get_stats

MOCK_DATA_PATH = "tests/mock_data"
TEST_RESULTS_PATH = "tests/new_testing_results"

monitor_data = []
monitoring = False
iteration = 0


def monitor_resources(interval: float = 0.01) -> None:
    process = psutil.Process()
    while monitoring:
        mem = process.memory_info().rss / 1024**2
        cpu = process.cpu_percent(interval=None)
        io = process.io_counters()
        monitor_data.append(
            {
                "memory_mb": mem,
                "cpu_percent": cpu,
                "read_bytes": io.read_bytes,
                "write_bytes": io.write_bytes,
            }
        )
        time.sleep(interval)

def _read_mock_data(file_path: str) -> Bundle:
            with open(file_path, "r") as file:
                return Bundle(**json.load(file))

def mock_do_request(method: str, url: URL, json_data: Dict[str, Any] | None = None) -> requests.Response:
    global iteration
    with get_stats().timer(f"{iteration}.mock_get_resource_history"):
        if str(url) == "http://testserver/test":
            return_bundle = Bundle(type="batch-response", entry=[])
            assert json_data is not None
            for entry in json_data.get('entry', []):
                if not entry.get('request'):
                    continue
                if entry['request'].get('method') == 'GET':
                    resource_type = entry['request'].get('url').split('/')[1]
                    file_path = f"{MOCK_DATA_PATH}/{resource_type}/{resource_type}_history.json"
                    bundle = _read_mock_data(file_path)
                    return_bundle.entry.append(BundleEntry(resource=bundle))
            response = requests.Response()
            response.status_code = 200
            response._content = return_bundle.model_dump_json(indent=4).encode('utf-8')
            return response
        if str(url) == "http://testserver/consumer/test":
            response = requests.Response()
            assert json_data is not None
            for entry in json_data.get('entry', []):
                if not entry.get('request'):
                    continue
                if entry['request'].get('method') == 'POST' or entry['request'].get('method') == 'PUT':
                    resource = entry['resource']
                    store_consumer(resource)
                    response._content = b'{"resourceType": "Bundle", "type": "batch-response", "entry": []}'
                    response.status_code = 200
                if entry['request'].get('method') == 'GET':
                    resource_type = entry['request'].get('url').split('/')[1]
                    resource_id = entry['request'].get('url').split('/')[2]
                    resource = get_from_consumer(resource_type, resource_id)

                    if resource is not None:
                        return_bundle = Bundle(type="batch-response", entry=[])
                        return_bundle.entry.append(BundleEntry(resource=resource))
                        response._content = return_bundle.model_dump_json(indent=4).encode('utf-8')
                        response.status_code = 200
            
            if response.status_code != 200:
                response = requests.Response()
                response.status_code = 200
                response._content = json.dumps({
                    "resourceType": "OperationOutcome",
                    "issue": [
                        {
                            "severity": "information",
                            "code": "informational",
                            "details": {
                                "text": f"Resource not found: method {method}, url {url}, json_data {json_data}"
                            }
                        }
                    ]
                }).encode('utf-8')
            return response
        assert False, "Should not reach here"

def store_consumer(resource: Dict[str, Any]) -> None:
    if not os.path.exists(f"{MOCK_DATA_PATH}/consumer_data/{resource['resourceType']}"):
        os.makedirs(f"{MOCK_DATA_PATH}/consumer_data/{resource['resourceType']}")
    file_path = f"{MOCK_DATA_PATH}/consumer_data/{resource['resourceType']}/{resource['id']}.json"
    with open(file_path, "w") as file:
        json.dump(resource, file)

def get_from_consumer(resource_type: str, resource_id: str) -> Any | None:
    file_path = f"{MOCK_DATA_PATH}/consumer_data/{resource_type}/{resource_id}.json"
    if not os.path.exists(file_path):
        return None
    with open(file_path, "r") as file:
        return json.load(file)

def mock_get_history_batch(url: URL) -> tuple[URL | None, List[BundleEntry]]:
    global iteration
    with get_stats().timer(f"{iteration}.mock_get_resource_history"):
        resource_type = url.path.split("/")[2] # type:ignore
        file_path = f"{MOCK_DATA_PATH}/{resource_type}/{resource_type}_history.json"
        with open(file_path, "r") as file:
            bundle = Bundle(**json.load(file))
            return None, bundle.entry
    assert False, "Should not reach here"
    return None, []
            
@pytest.mark.filterwarnings("ignore:Pydantic serializer warnings")
@pytest.mark.parametrize(
    "resource_count, version_count, max_depth, test_name",
    [
        (1, 1, 1, "very_simple"),
        (2, 2, 1, "no_depth"),
        (5, 3, 1, "more_resources"),
        # (5, 3, 2, "just_a_little_depth"),
        # (5, 3, 4, "medium"),
        # (5, 3, 5, "difficult"),
        # (5, 3, 6, "hard"),
        # (5, 3, 7, "very_hard"),
        # (5, 3, 8, "extreme"),
        # (5, 3, 9, "crazy"),
        # (5, 3, 10, "insane"),
        # (10, 5, 10, "impossible"),
    ],
)
@patch(
    "app.services_new.api.fhir_api.FhirApi.get_history_batch",
    side_effect=mock_get_history_batch,
)
@patch(
    "app.services_new.api.api_service.AuthenticationBasedApi.do_request",
    side_effect=mock_do_request,
)
def test_consumer_update_with_timing(
    mock_do_request: requests.Response,
    mock_get_history_batch: Bundle,
    api_client: TestClient, resource_count: int, version_count: int, max_depth: int, test_name: str
) -> None:
    global monitoring
    monitoring = True

    global monitor_data
    monitor_data = []

    errors = 0
    iterations = 30
    global iteration


    print(f"Running test: {test_name}")
    db = get_database()
    db.truncate_tables()

    create_file_structure(
        base_path=f"{MOCK_DATA_PATH}",
        resource_count=resource_count,
        version_count=version_count,
        max_depth=max_depth,
    )

    monitor_thread = threading.Thread(target=monitor_resources)
    try:
        monitor_thread.start()
        for iteration in range(iterations):
            print(f"Iteration {iteration + 1}/{iterations}")
            _response = api_client.post("/update_resources/new/test-supplier")
            print("Update done: mCSD resources are updated")
    finally:
        monitoring = False
        monitor_thread.join()

    ids_set = set()
    with open(
        f"{MOCK_DATA_PATH}/all_resources_history.json", "r"
    ) as file:
        bundle_data = json.load(file)
        for entry in bundle_data["entry"]:
            res_id = entry["resource"]["id"]
            ids_set.add(res_id)

    errors += check_if_in_db(ids_set=ids_set)

    print("Generating test report")

    stats = get_stats()
    assert isinstance(stats, Stats)
    assert hasattr(stats, "client")
    report = stats.client.get_memory()

    durations = []
    for idx, item in enumerate(report["mcsd.update_supplier"]):
        durations.append(item - sum(report[f"{idx}.mock_get_resource_history"]))

    report = generate_report(
        test_name, durations, iterations, monitor_data, total_resources=len(ids_set)
    )

    if not os.path.exists(TEST_RESULTS_PATH):
        os.makedirs(TEST_RESULTS_PATH)
    with open(f"{TEST_RESULTS_PATH}/test_{test_name}.json", "w") as f:
        json.dump(report, f, indent=4)