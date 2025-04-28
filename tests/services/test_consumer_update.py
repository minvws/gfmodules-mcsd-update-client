# from datetime import datetime
# import json
# import time
# import threading
# import psutil
# import pytest
# import os
# from typing import Sequence
# from fastapi.testclient import TestClient
# from unittest.mock import patch
# from fastapi.responses import JSONResponse
# from app.container import get_database
# from app.models.fhir.r4.types import Bundle
# from app.models.supplier.dto import SupplierDto
# from utils.utils import (
#     check_if_in_db,
#     create_file_structure,
#     generate_report,
# )

# from app.stats import Stats, get_stats

# MOCK_DATA_PATH = "tests/mock_data"
# TEST_RESULTS_PATH = "tests/testing_results"

# monitor_data = []
# monitoring = False
# iteration = 0


# def monitor_resources(interval: float = 0.01) -> None:
#     process = psutil.Process()
#     while monitoring:
#         mem = process.memory_info().rss / 1024**2
#         cpu = process.cpu_percent(interval=None)
#         io = process.io_counters()
#         monitor_data.append(
#             {
#                 "memory_mb": mem,
#                 "cpu_percent": cpu,
#                 "read_bytes": io.read_bytes,
#                 "write_bytes": io.write_bytes,
#             }
#         )
#         time.sleep(interval)

# def mock_get_resource_history(
#         supplier_id: str,
#         resource_type: str | None = None,
#         resource_id: str | None = None,
#         _since: datetime | None = None,
#     ) -> Bundle:
#         def _read_mock_data(file_path: str) -> Bundle:
#             with open(file_path, "r") as file:
#                 return Bundle(**json.load(file))
#         global iteration
#         with get_stats().timer(f"{iteration}.mock_get_resource_history"):
#             if resource_type is None and resource_id is None:
#                 print("Total history bundle is requested")
#                 return _read_mock_data(
#                     f"{MOCK_DATA_PATH}/all_resources_history.json"
#                 )
#             if resource_id is None:
#                 print(
#                     f"All from resourcetype history bundle is requested: {resource_type}"
#                 )
#                 return _read_mock_data(
#                     f"{MOCK_DATA_PATH}/{resource_type}/{resource_type}_history.json"
#                 )                
#             # print(f"Specific resource history bundle is requested: {resource_type} - {resource_id}")
#             return _read_mock_data(
#                 f"{MOCK_DATA_PATH}/{resource_type}/{resource_id}/{resource_id}_history.json"
#             )


# @pytest.mark.filterwarnings("ignore:Pydantic serializer warnings")
# @pytest.mark.parametrize(
#     "resource_count, version_count, max_depth, test_name",
#     [
#         (1, 1, 1, "very_simple"),
#         (2, 2, 1, "no_depth"),
#         (5, 3, 1, "more_resources"),
#         # (5, 3, 2, "just_a_little_depth"),
#         # (5, 3, 4, "medium"),
#         # (5, 3, 5, "difficult"),
#         # (5, 3, 6, "hard"),
#         # (5, 3, 7, "very_hard"),
#         # (5, 3, 8, "extreme"),
#         # (5, 3, 9, "crazy"),
#         # (5, 3, 10, "insane"),
#         # (10, 5, 10, "impossible"),
#     ],
# )
# @patch(
#     "app.services.entity_services.supplier_service.SupplierService.get_all",
#     return_value=[
#         SupplierDto(id="test", name="test", endpoint="http://testserver/supplier")
#     ],
# )
# @patch(
#     "app.services.entity_services.supplier_service.SupplierService.get_one",
#     return_value=SupplierDto(id="test", name="test", endpoint="http://testserver/supplier"),
# )
# @patch(
#     "app.services.request_services.supplier_request_service.SupplierRequestsService.get_resource_history",
#     side_effect=mock_get_resource_history,
# )
# @patch(
#     "app.services.request_services.consumer_request_service.ConsumerRequestService.post_bundle",
#     return_value=JSONResponse({"message": "ok"}),
# )
# def test_consumer_update_with_timing(
#     mock_post_bundle: JSONResponse,
#     mock_get_resource_history: Bundle,
#     mock_get_one: SupplierDto,
#     mock_get_all: Sequence[SupplierDto],
#     api_client: TestClient, resource_count: int, version_count: int, max_depth: int, test_name: str
# ) -> None:
#     global monitoring
#     monitoring = True

#     global monitor_data
#     monitor_data = []

#     errors = 0
#     iterations = 30
#     global iteration


#     print(f"Running test: {test_name}")
#     db = get_database()
#     db.truncate_tables()

#     create_file_structure(
#         base_path=f"{MOCK_DATA_PATH}",
#         resource_count=resource_count,
#         version_count=version_count,
#         max_depth=max_depth,
#     )

#     monitor_thread = threading.Thread(target=monitor_resources)
#     try:
#         monitor_thread.start()
#         for iteration in range(iterations):
#             print(f"Iteration {iteration + 1}/{iterations}")
#             response = api_client.post("/update_resources")
#             print("Update done: mCSD resources are updated")

#             ok_status_code = response.status_code == 200 or False  
#             good_updated_message = response.json()[0]["message"] == "organizations and endpoints are updated" or False  
#             if ok_status_code is False or good_updated_message is False: 
#                 errors += 1
#     finally:
#         monitoring = False
#         monitor_thread.join()

#     ids_set = set()
#     with open(
#         f"{MOCK_DATA_PATH}/all_resources_history.json", "r"
#     ) as file:
#         bundle_data = json.load(file)
#         for entry in bundle_data["entry"]:
#             res_id = entry["resource"]["id"]
#             ids_set.add(res_id)

#     errors += check_if_in_db(ids_set=ids_set)

#     print("Generating test report")

#     stats = get_stats()
#     assert isinstance(stats, Stats)
#     assert hasattr(stats, "client")
#     report = stats.client.get_memory()

#     durations = []
#     for idx, item in enumerate(report["mcsd.update_supplier"]):
#         durations.append(item - sum(report[f"{idx}.mock_get_resource_history"]))

#     report = generate_report(
#         test_name, durations, iterations, monitor_data, total_resources=len(ids_set)
#     )

#     if not os.path.exists(TEST_RESULTS_PATH):
#         os.makedirs(TEST_RESULTS_PATH)
#     with open(f"{TEST_RESULTS_PATH}/test_{test_name}.json", "w") as f:
#         json.dump(report, f, indent=4)
