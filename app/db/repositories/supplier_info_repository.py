import logging
from typing import Sequence

from sqlalchemy.exc import DatabaseError
from app.db.decorator import repository
from app.db.entities.supplier_info import SupplierInfo
from app.db.repositories.repository_base import RepositoryBase
from sqlalchemy import select

logger = logging.getLogger(__name__)


@repository(SupplierInfo)
class SupplierInfoRepository(RepositoryBase):

    def get(self, supplier_id: str) -> SupplierInfo:
        query = select(SupplierInfo).filter_by(supplier_id=supplier_id)
        info = self.db_session.session.execute(query).scalars().first()
        if info is None:
            info = self.create(supplier_id)
        return info
    
    def get_all(self) -> Sequence[SupplierInfo]:
        query = select(SupplierInfo).order_by(SupplierInfo.supplier_id)
        return self.db_session.session.execute(query).scalars().all()

    def update(self, info: SupplierInfo) -> SupplierInfo:
        try:
            self.db_session.add(info)
            self.db_session.commit()
            self.db_session.session.refresh(info)
            return info
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to update supplier info {info.supplier_id}: {e}")
            raise

    def create(self, supplier_id: str) -> SupplierInfo:
        try:
            info = SupplierInfo(supplier_id=supplier_id, failed_sync_count=0, failed_attempts=0)
            self.db_session.add(info)
            self.db_session.commit()
            return info
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to add suppler info {info.supplier_id}: {e}")
            raise
