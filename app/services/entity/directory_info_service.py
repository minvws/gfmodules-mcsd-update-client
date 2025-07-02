from datetime import datetime, timezone
import logging
from typing import List, Sequence

from app.db.db import Database
from app.db.entities.directory_info import DirectoryInfo
from app.db.repositories.directory_info_repository import DirectoryInfoRepository

logger = logging.getLogger(__name__)


class DirectoryInfoService:
    def __init__(self, database: Database, directory_stale_timeout_seconds: int) -> None:
        self.__database = database
        self.__directory_stale_timeout_seconds = directory_stale_timeout_seconds

    def delete_directory_info(self, directory_id: str) -> None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            info = repository.get(directory_id)
            session.delete(info)
            session.commit()


    def get_directory_info(self, directory_id: str) -> DirectoryInfo:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            return repository.get(directory_id)
        
    def get_all_directories_info(self, include_ignored: bool = False) -> Sequence[DirectoryInfo]:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            return repository.get_all(include_ignored=include_ignored)

    def update_directory_info(self, info: DirectoryInfo) -> None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            repository.update(info)
    
    def health_check(self) -> bool:
        def is_directory_healthy(directory_info: DirectoryInfo) -> bool:
            if not directory_info.last_success_sync:
                return False
            time_since_last_success = (datetime.now(timezone.utc) - directory_info.last_success_sync.astimezone(timezone.utc)).total_seconds()
            return time_since_last_success < self.__directory_stale_timeout_seconds

        directories = self.get_all_directories_info()
        return all([
            is_directory_healthy(directory_info)
            for directory_info in directories
        ])
    
    def get_prometheus_metrics(self) -> List[str]:
        lines = []

        lines.append("# HELP directory_failed_sync_total Total failed syncs")
        lines.append("# TYPE directory_failed_sync_total counter")

        lines.append("# HELP directory_failed_attempts Consecutive failed attempts")
        lines.append("# TYPE directory_failed_attempts gauge")

        lines.append("# HELP directory_last_success_sync Timestamp of last success")
        lines.append("# TYPE directory_last_success_sync gauge")

        for s in self.get_all_directories_info():
            labels = f'directory_id="{s.directory_id}"'

            lines.append(f"directory_failed_sync_total{{{labels}}} {s.failed_sync_count}")
            lines.append(f"directory_failed_attempts{{{labels}}} {s.failed_attempts}")

            if s.last_success_sync:
                ts = int(s.last_success_sync.timestamp())
                lines.append(f"directory_last_success_sync{{{labels}}} {ts}")
        
        return lines
