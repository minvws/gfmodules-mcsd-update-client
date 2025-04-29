from token import PERCENT
from datetime import datetime
import json
import os
import time
import threading
import psutil
import pytest
import numpy as np

from typing import Any, Dict, List, Sequence
from fastapi.testclient import TestClient
from unittest.mock import patch
from fastapi.responses import JSONResponse
import requests
from yarl import URL
from app.container import get_database
from fhir.resources.R4B.bundle import Bundle, BundleEntry
from app.models.supplier.dto import SupplierDto
from utils.utils import (
    check_if_in_db,
    create_file_structure,
    generate_report,
)
from fhir.resources.R4B.bundle import BundleEntry

from app.stats import Stats, get_stats
from app.services.fhir.bundle.bundle_utils import filter_history_entries

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

def mock_do_request(method: str, url: URL, json: Dict[str, Any] | None = None) -> requests.Response:

    if str(url) == "http://testserver/supplier":
        global iteration
        with get_stats().timer(f"{iteration}.mock_get_resource_history"):
            return_bundle = Bundle(type="batch-response", entry=[])
            for entry in json.get('entry', []):
                if not entry.get('request'):
                    continue
                if entry['request'].get('method') == 'GET':
                    with get_stats().timer("reading from file"):
                        resource_type = entry['request'].get('url').split('/')[1]
                        file_path = f"{MOCK_DATA_PATH}/{resource_type}/{resource_type}_history.json"
                        bundle = _read_mock_data(file_path)
                        return_bundle.entry.append(BundleEntry(resource=bundle))
            response = requests.Response()
            response.status_code = 200
            with get_stats().timer("json_dump"):
                response._content = return_bundle.model_dump_json(indent=4).encode('utf-8')
            return response
    if str(url) == "http://testserver/consumer/test":
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"resourceType": "Bundle", "type": "batch-response", "entry": []}'
        return response
    assert False, "Should not reach here"


def mock_get_history_batch(url: URL) -> tuple[URL | None, List[BundleEntry]]:
    global iteration
    with get_stats().timer(f"{iteration}.mock_get_resource_history"):
        resource_type = url.path.split("/")[2]
        file_path = f"{MOCK_DATA_PATH}/{resource_type}/{resource_type}_history.json"
        with open(file_path, "r") as file:
            bundle = Bundle(**json.load(file))
            entries = filter_history_entries(bundle.entry)
            return None, entries
    assert False, "Should not reach here"
    return None, []

@pytest.mark.filterwarnings("ignore:Pydantic serializer warnings")
@pytest.mark.parametrize(
    "resource_count, version_count, max_depth, test_name",
    [
        # (1, 1, 1, "very_simple"),
        # (2, 2, 1, "no_depth"),
        # (5, 3, 1, "more_resources"),
        # (5, 3, 2, "just_a_little_depth"),
        # (5, 3, 4, "medium"),
        # (5, 3, 5, "difficult"),
        # (5, 3, 6, "hard"),
        # (5, 3, 7, "very_hard"),
        # (5, 3, 8, "extreme"),
        # (5, 3, 9, "crazy"),
        # (5, 3, 10, "insane"),
        (10, 5, 10, "impossible"),
    ],
)
@patch(
    "app.services_new.api.suppliers_api.SuppliersApi.get_all",
    return_value=[
        SupplierDto(id="test", name="test", endpoint="http://testserver/supplier")
    ],
)
@patch(
    "app.services_new.api.suppliers_api.SuppliersApi.get_one",
    return_value=SupplierDto(id="test", name="test", endpoint="http://testserver/supplier"),
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
    mock_get_one: SupplierDto,
    mock_get_all: Sequence[SupplierDto],
    api_client: TestClient, resource_count: int, version_count: int, max_depth: int, test_name: str
) -> None:
    global monitoring
    monitoring = True

    global monitor_data
    monitor_data = []

    errors = 0
    iterations = 1
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
            response = api_client.post(f"/update_resources/new/test")
            print("Update done: mCSD resources are updated")

            print(response.text)

            # ok_status_code = response.status_code == 200 or False
            # good_updated_message = response.json()[0]["message"] == "organizations and endpoints are updated" or False
            # if ok_status_code is False or good_updated_message is False:
            #     errors += 1
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


    #print(report)
    for key, value in sorted(report.items(), key=lambda x: sum(x[1]), reverse=True):
        print(f"{key}:")
        print(f"\t p1:{str(np.round(np.percentile(value, 1), 4))}")
        print(f"\t p50:{str(np.round(np.percentile(value, 50), 4))}")
        print(f"\t p75:{str(np.round(np.percentile(value, 75), 4))}")
        print(f"\t p99:{str(np.round(np.percentile(value, 99), 4))}")
        print(f"\t min:{str(np.min(value))}")
        print(f"\t max:{str(np.max(value))}")
        print(f"\t count:{len(value)}")
        print(f"\t sum:{str(np.sum(value))}")

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
