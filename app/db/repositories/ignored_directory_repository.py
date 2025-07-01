import logging
from typing import Sequence

from app.db.decorator import repository
from app.db.entities.ignored_directory import IgnoredDirectory
from app.db.repositories.repository_base import RepositoryBase
from sqlalchemy import select

logger = logging.getLogger(__name__)


@repository(IgnoredDirectory)
class IgnoredDirectoryRepository(RepositoryBase):
    def get(self, directory_id: str) -> IgnoredDirectory | None:
        query = select(IgnoredDirectory).where(IgnoredDirectory.directory_id == directory_id)
        return self.db_session.session.execute(query).scalars().first()
    
    def get_all(self) -> Sequence[IgnoredDirectory]:
        query = select(IgnoredDirectory)
        return self.db_session.session.execute(query).scalars().all()
    
