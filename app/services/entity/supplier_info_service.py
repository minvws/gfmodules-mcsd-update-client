from datetime import datetime, timezone
import logging
from typing import List, Sequence

from app.db.db import Database
from app.db.entities.supplier_info import SupplierInfo
from app.db.repositories.supplier_info_repository import SupplierInfoRepository

logger = logging.getLogger(__name__)


class SupplierInfoService:
    def __init__(self, database: Database, supplier_stale_timeout_seconds: int) -> None:
        self.__database = database
        self.__supplier_stale_timeout_seconds = supplier_stale_timeout_seconds

    def get_supplier_info(self, supplier_id: str) -> SupplierInfo:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierInfoRepository)
            return repository.get(supplier_id)
        
    def get_all_suppliers_info(self, include_ignored: bool = False) -> Sequence[SupplierInfo]:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierInfoRepository)
            return repository.get_all(include_ignored=include_ignored)

    def update_supplier_info(self, info: SupplierInfo) -> None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierInfoRepository)
            repository.update(info)
    
    def health_check(self) -> bool:
        def is_supplier_healthy(supplier_info: SupplierInfo) -> bool:
            if not supplier_info.last_success_sync:
                return False
            time_since_last_success = (datetime.now(timezone.utc) - supplier_info.last_success_sync.astimezone(timezone.utc)).total_seconds()
            return time_since_last_success < self.__supplier_stale_timeout_seconds

        suppliers = self.get_all_suppliers_info()
        return all([
            is_supplier_healthy(supplier_info)
            for supplier_info in suppliers
        ])
    
    def get_prometheus_metrics(self) -> List[str]:
        lines = []

        lines.append("# HELP supplier_failed_sync_total Total failed syncs")
        lines.append("# TYPE supplier_failed_sync_total counter")

        lines.append("# HELP supplier_failed_attempts Consecutive failed attempts")
        lines.append("# TYPE supplier_failed_attempts gauge")

        lines.append("# HELP supplier_last_success_sync Timestamp of last success")
        lines.append("# TYPE supplier_last_success_sync gauge")

        for s in self.get_all_suppliers_info():
            labels = f'supplier_id="{s.supplier_id}"'

            lines.append(f"supplier_failed_sync_total{{{labels}}} {s.failed_sync_count}")
            lines.append(f"supplier_failed_attempts{{{labels}}} {s.failed_attempts}")

            if s.last_success_sync:
                ts = int(s.last_success_sync.timestamp())
                lines.append(f"supplier_last_success_sync{{{labels}}} {ts}")
        
        return lines
