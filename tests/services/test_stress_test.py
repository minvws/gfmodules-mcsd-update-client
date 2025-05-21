import json
import os
import time
import threading
import psutil
import pytest

from typing import Any, Dict, List, Tuple
from fastapi.testclient import TestClient
from unittest.mock import patch
import requests
from yarl import URL
from app.container import get_database
from fhir.resources.R4B.bundle import Bundle, BundleEntry
from app.models.supplier.dto import SupplierDto
from tests.utils.utils import (
    check_if_in_db,
    create_file_structure,
    generate_report,
)

from app.stats import Stats, get_stats

MOCK_DATA_PATH = "tests/mock_data"
TEST_RESULTS_PATH = "tests/testing_results"
CACHE: Dict[str, Any] = {}

monitor_data = []
monitoring = False
iteration = 0


def mock_bundles_cache() -> Dict[str, Any]:
    global CACHE
    if CACHE:
        return CACHE
    cache = {}
    for root, _, files in os.walk(MOCK_DATA_PATH):
        for f in files:
            if f.endswith(".json"):
                path = os.path.join(root, f)
                with open(path, "r") as file:
                    cache[path] = json.load(file)
    CACHE = cache
    return CACHE


def monitor_resources(interval: float = 0.1) -> None:
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


def mock_do_request(
    method: str, url: URL, json_data: Dict[str, Any] | None = None
) -> requests.Response:
    global iteration
    with get_stats().timer(f"{iteration}.patch_timing"):
        if str(url) == "http://testserver/test":
            return_bundle = Bundle(type="batch-response", entry=[])
            assert json_data is not None
            for entry in json_data.get("entry", []):
                if not entry.get("request"):
                    continue
                if entry["request"].get("method") == "GET":
                    resource_type = entry["request"].get("url").split("/")[1]
                    file_path = (
                        f"{MOCK_DATA_PATH}/{resource_type}/{resource_type}_history.json"
                    )
                    bundle = Bundle(**CACHE[file_path])
                    return_bundle.entry.append(BundleEntry(resource=bundle))
            response = requests.Response()
            response.status_code = 200
            response._content = return_bundle.model_dump_json(indent=4).encode("utf-8")
            return response
        if str(url) == "http://testserver/consumer/test":
            response = requests.Response()
            assert json_data is not None
            for entry in json_data.get("entry", []):
                if not entry.get("request"):
                    continue
                if (
                    entry["request"].get("method") == "POST"
                    or entry["request"].get("method") == "PUT"
                ):
                    resource = entry["resource"]
                    store_consumer(resource)
                    response._content = b'{"resourceType": "Bundle", "type": "batch-response", "entry": []}'
                    response.status_code = 200
                if entry["request"].get("method") == "GET":
                    resource_type = entry["request"].get("url").split("/")[1]
                    resource_id = entry["request"].get("url").split("/")[2]
                    resource = get_from_consumer(resource_type, resource_id)

                    if resource is not None:
                        return_bundle = Bundle(type="batch-response", entry=[])
                        return_bundle.entry.append(BundleEntry(resource=resource))
                        response._content = return_bundle.model_dump_json(
                            indent=4
                        ).encode("utf-8")
                        response.status_code = 200

            if response.status_code != 200:
                response = requests.Response()
                response.status_code = 200
                response._content = json.dumps(
                    {
                        "resourceType": "OperationOutcome",
                        "issue": [
                            {
                                "severity": "information",
                                "code": "informational",
                                "details": {
                                    "text": f"Resource not found: method {method}, url {url}, json_data {json_data}"
                                },
                            }
                        ],
                    }
                ).encode("utf-8")
            return response
        assert False, "Should not reach here"


def store_consumer(resource: Dict[str, Any]) -> None:
    key = f"consumer_data/{resource['resourceType']}/{resource['id']}.json"
    CACHE[key] = resource


def get_from_consumer(resource_type: str, resource_id: str) -> Any | None:
    key = f"consumer_data/{resource_type}/{resource_id}.json"
    return CACHE.get(key)


def mock_get_history_batch(url: URL) -> tuple[URL | None, List[BundleEntry]]:
    global iteration
    with get_stats().timer(f"{iteration}.patch_timing"):
        resource_type = url.path.split("/")[2]  # type:ignore
        file_path = f"{MOCK_DATA_PATH}/{resource_type}/{resource_type}_history.json"
        if file_path not in CACHE:
            assert False, f"File {file_path} not found in cache"
        bundle = Bundle(**CACHE[file_path])
        return None, bundle.entry


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
    "app.services.api.fhir_api.FhirApi.get_history_batch",
    side_effect=mock_get_history_batch,
)
@patch(
    "app.services.api.api_service.AuthenticationBasedApiService.do_request",
    side_effect=mock_do_request,
)
@patch(
    "app.services.supplier_provider.api_provider.SupplierApiProvider.get_all_suppliers",
    return_value=[
        SupplierDto(
            id="test-supplier",
            name="Test Supplier",
            endpoint="http://testserver/test",
            is_deleted=False,
            ura_number="12345678",
        )
    ],
)
@patch(
    "app.services.supplier_provider.api_provider.SupplierApiProvider.get_one_supplier",
    return_value=SupplierDto(
        id="test-supplier",
        name="Test Supplier",
        endpoint="http://testserver/test",
        is_deleted=False,
        ura_number="12345678",
    ),
)
def test_stress_test_update(
    mock_do_request: requests.Response,
    mock_get_history_batch: Tuple[URL | None, List[BundleEntry]],
    mock_get_all_suppliers: List[SupplierDto],
    mock_get_one_supplier: SupplierDto,
    api_client: TestClient,
    resource_count: int,
    version_count: int,
    max_depth: int,
    test_name: str,
) -> None:
    global monitoring
    monitoring = True

    global monitor_data
    monitor_data = []

    iterations = 10
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

    global CACHE
    CACHE = {}
    CACHE = mock_bundles_cache()

    monitor_thread = threading.Thread(target=monitor_resources)
    try:
        monitor_thread.start()
        for iteration in range(iterations):
            print(f"Iteration {iteration + 1}/{iterations}")
            with get_stats().timer("mcsd.update_supplier"):
                _response = api_client.post("/update_resources/test-supplier")
            print(f"Update done: mCSD resources are updated: {_response.json()}")
    finally:
        monitoring = False
        monitor_thread.join()

    latest_entries: Dict[str, Tuple[int, str]] = {}

    all_histories = CACHE.get("tests/mock_data/all_resources_history.json")
    assert all_histories is not None, "All resources history file not found in cache"

    for entry in all_histories.get("entry", []):
        request_url = entry["request"]["url"]
        method = entry["request"]["method"]

        parts = request_url.split("/")
        if len(parts) < 3:
            assert False, f"Invalid request URL: {request_url}"

        res_id = parts[1]
        version = int(parts[2])

        if res_id not in latest_entries or version > latest_entries[res_id][0]:
            latest_entries[res_id] = (version, method)

    final_resources = {
        res_id
        for res_id, (version, method) in latest_entries.items()
        if method != "DELETE"
    }

    assert check_if_in_db(supplier_id="test-supplier", ids_set=final_resources) == 0

    print("Generating test report")

    stats = get_stats()
    assert isinstance(stats, Stats)
    assert hasattr(stats, "client")
    report = stats.client.get_memory()

    true_update_durations = []
    patch_durations = []
    for idx, item in enumerate(report["mcsd.update_supplier"]):
        patch_durations.append(sum(report[f"{idx}.patch_timing"]))
        true_update_durations.append(item - sum(report[f"{idx}.patch_timing"]))

    durations = {
        "total": report["mcsd.update_supplier"],
        "true_update": true_update_durations,
        "patch": patch_durations,
    }

    report = generate_report(
        test_name,
        durations,
        iterations,
        monitor_data,
        total_resources=len(final_resources),
    )

    if not os.path.exists(TEST_RESULTS_PATH):
        os.makedirs(TEST_RESULTS_PATH)
    with open(f"{TEST_RESULTS_PATH}/test_{test_name}.json", "w") as f:
        json.dump(report, f, indent=4)
