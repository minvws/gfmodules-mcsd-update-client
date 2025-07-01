from datetime import datetime, timedelta
from typing import Any, Dict, List
from fastapi.testclient import TestClient
import pytest
from app.container import get_database
from app.db.db import Database
from app.db.entities.ignored_directory import IgnoredDirectory
from app.db.entities.directory_info import DirectoryInfo

def __insert_info(
        db: Database, 
        directory_id: str, 
        last_success_sync: datetime,
        failed_sync_count: int = 0,
        failed_attempts: int = 0,
        in_ignore_list: bool = False,
    ) -> DirectoryInfo:
    """Insert a directory info record into the database."""
    with db.get_db_session() as db_session:
        info = DirectoryInfo(
            directory_id=directory_id,
            failed_sync_count=failed_sync_count,
            failed_attempts=failed_attempts,
            last_success_sync=last_success_sync,
        )
        db_session.add(info)
        if in_ignore_list:
            ignore = IgnoredDirectory(
                directory_id=directory_id,
            )
            db_session.add(ignore)
        db_session.commit()
    return info

@pytest.mark.parametrize(
    "directory_syncs, in_ignore_list, expected_status",
    [
        (
            [60, 100],  # All healthy
            False, # No ignore list
            {"status": "ok"}, # OK because all directories are healthy
        ),
        (
            [60, 150],  # One unhealthy
            False, # No ignore list
            {"status": "error"}, # Error because one directory is unhealthy
        ),
        (
            [60, 100],  # All healthy
            True, # In ignore list
            {"status": "ok"}, # OK because in ignore list
        ),
        (
            [60, 150],  # One unhealthy
            True, # In ignore list
            {"status": "ok"}, # OK because in ignore list
        ),
    ]
)
def test_directory_health_matrix(api_client: TestClient, directory_syncs: List[str], in_ignore_list: bool, expected_status: Dict[str,str]) -> None:
    for idx, sync_seconds in enumerate(directory_syncs, start=1):
        __insert_info(
            db=get_database(),
            directory_id=f"directory_{idx}",
            last_success_sync=datetime.now() - timedelta(seconds=float(sync_seconds)),
            in_ignore_list=in_ignore_list,
        )
    
    response = api_client.get("/directory_health")
    assert response.status_code == 200
    assert response.json() == expected_status
    
@pytest.mark.parametrize(
    "directories_data, expected_metrics",
    [
        (
            [
                {"directory_id": "directory_1", "failed_sync_count": 5, "failed_attempts": 2, "last_success_sync": datetime.now() - timedelta(seconds=60)},
                {"directory_id": "directory_2", "failed_sync_count": 3, "failed_attempts": 1, "last_success_sync": datetime.now() - timedelta(seconds=120)},
            ],
            [
                '# HELP directory_failed_sync_total Total failed syncs',
                '# TYPE directory_failed_sync_total counter',
                'directory_failed_sync_total{directory_id="directory_1"} 5',
                'directory_failed_sync_total{directory_id="directory_2"} 3',
                '# HELP directory_failed_attempts Consecutive failed attempts',
                '# TYPE directory_failed_attempts gauge',
                'directory_failed_attempts{directory_id="directory_1"} 2',
                'directory_failed_attempts{directory_id="directory_2"} 1',
                '# HELP directory_last_success_sync Timestamp of last success',
                '# TYPE directory_last_success_sync gauge',
                f'directory_last_success_sync{{directory_id="directory_1"}} {int((datetime.now() - timedelta(seconds=60)).timestamp())}',
                f'directory_last_success_sync{{directory_id="directory_2"}} {int((datetime.now() - timedelta(seconds=120)).timestamp())}',
            ],
        ),
        (
            [
                {"directory_id": "directory_1", "failed_sync_count": 0, "failed_attempts": 0, "last_success_sync": None},
            ],
            [
                '# HELP directory_failed_sync_total Total failed syncs',
                '# TYPE directory_failed_sync_total counter',
                'directory_failed_sync_total{directory_id="directory_1"} 0',
                '# HELP directory_failed_attempts Consecutive failed attempts',
                '# TYPE directory_failed_attempts gauge',
                'directory_failed_attempts{directory_id="directory_1"} 0',
                '# HELP directory_last_success_sync Timestamp of last success',
                '# TYPE directory_last_success_sync gauge',
            ],
        ),
    ],
)
def test_directory_metrics(api_client: TestClient, directories_data: List[Dict[str, Any]], expected_metrics: List[str]) -> None:
    db = get_database()
    for directory in directories_data:
        __insert_info(
            db=db,
            directory_id=directory["directory_id"],
            last_success_sync=directory["last_success_sync"],
            failed_sync_count=directory["failed_sync_count"],
            failed_attempts=directory["failed_attempts"],
        )

    response = api_client.get("/directory_metrics")
    assert response.status_code == 200
    response_lines = response.text.strip().split("\n")
    for expected_line in expected_metrics:
        assert expected_line in response_lines