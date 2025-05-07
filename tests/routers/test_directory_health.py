from datetime import datetime, timedelta
from typing import Any, Dict, List
from fastapi.testclient import TestClient
import pytest
from app.container import get_database
from app.db.db import Database
from app.db.entities.supplier_info import SupplierInfo

def __insert_info(
        db: Database, 
        supplier_id: str, 
        last_success_sync: datetime,
        failed_sync_count: int = 0,
        failed_attempts: int = 0,
    ) -> SupplierInfo:
    """Insert a supplier info record into the database."""
    with db.get_db_session() as db_session:
        info = SupplierInfo(
            supplier_id=supplier_id,
            failed_sync_count=failed_sync_count,
            failed_attempts=failed_attempts,
            last_success_sync=last_success_sync,
        )
        db_session.add(info)
        db_session.commit()
    return info

@pytest.mark.parametrize(
    "supplier_syncs, expected_status",
    [
        (
            [60, 100],  # All healthy
            {"status": "ok"},
        ),
        (
            [60, 150],  # One unhealthy
            {"status": "error"},
        ),
    ]
)
def test_directory_health_matrix(api_client: TestClient, supplier_syncs: List[str], expected_status: Dict[str,str]) -> None:
    for idx, sync_seconds in enumerate(supplier_syncs, start=1):
        __insert_info(
            db=get_database(),
            supplier_id=f"supplier_{idx}",
            last_success_sync=datetime.now() - timedelta(seconds=float(sync_seconds)),
        )
    
    response = api_client.get("/directory_health")
    assert response.status_code == 200
    assert response.json() == expected_status
    
@pytest.mark.parametrize(
    "suppliers_data, expected_metrics",
    [
        (
            [
                {"supplier_id": "supplier_1", "failed_sync_count": 5, "failed_attempts": 2, "last_success_sync": datetime.now() - timedelta(seconds=60)},
                {"supplier_id": "supplier_2", "failed_sync_count": 3, "failed_attempts": 1, "last_success_sync": datetime.now() - timedelta(seconds=120)},
            ],
            [
                '# HELP supplier_failed_sync_total Total failed syncs',
                '# TYPE supplier_failed_sync_total counter',
                'supplier_failed_sync_total{supplier_id="supplier_1"} 5',
                'supplier_failed_sync_total{supplier_id="supplier_2"} 3',
                '# HELP supplier_failed_attempts Consecutive failed attempts',
                '# TYPE supplier_failed_attempts gauge',
                'supplier_failed_attempts{supplier_id="supplier_1"} 2',
                'supplier_failed_attempts{supplier_id="supplier_2"} 1',
                '# HELP supplier_last_success_sync Timestamp of last success',
                '# TYPE supplier_last_success_sync gauge',
                f'supplier_last_success_sync{{supplier_id="supplier_1"}} {int((datetime.now() - timedelta(seconds=60)).timestamp())}',
                f'supplier_last_success_sync{{supplier_id="supplier_2"}} {int((datetime.now() - timedelta(seconds=120)).timestamp())}',
            ],
        ),
        (
            [
                {"supplier_id": "supplier_1", "failed_sync_count": 0, "failed_attempts": 0, "last_success_sync": None},
            ],
            [
                '# HELP supplier_failed_sync_total Total failed syncs',
                '# TYPE supplier_failed_sync_total counter',
                'supplier_failed_sync_total{supplier_id="supplier_1"} 0',
                '# HELP supplier_failed_attempts Consecutive failed attempts',
                '# TYPE supplier_failed_attempts gauge',
                'supplier_failed_attempts{supplier_id="supplier_1"} 0',
                '# HELP supplier_last_success_sync Timestamp of last success',
                '# TYPE supplier_last_success_sync gauge',
            ],
        ),
    ],
)
def test_directory_metrics(api_client: TestClient, suppliers_data: List[Dict[str, Any]], expected_metrics: List[str]) -> None:
    db = get_database()
    for supplier in suppliers_data:
        __insert_info(
            db=db,
            supplier_id=supplier["supplier_id"],
            last_success_sync=supplier["last_success_sync"],
            failed_sync_count=supplier["failed_sync_count"],
            failed_attempts=supplier["failed_attempts"],
        )

    response = api_client.get("/directory_metrics")
    assert response.status_code == 200
    response_lines = response.text.strip().split("\n")
    for expected_line in expected_metrics:
        assert expected_line in response_lines