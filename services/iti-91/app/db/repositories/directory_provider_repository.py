from sqlalchemy import select

from app.db.entities.directory_provider import DirectoryProvider
from app.db.repositories.repository_base import RepositoryBase


class DirectoryProviderRepository(RepositoryBase):
    def get_by_id(self, id_: int) -> DirectoryProvider | None:
        stmt = select(DirectoryProvider).where(DirectoryProvider.id == id_)
        return self.db_session.session.scalars(stmt).first()

    def get_by_url(self, url: str) -> DirectoryProvider | None:
        stmt = select(DirectoryProvider).where(DirectoryProvider.url == url)
        return self.db_session.session.scalars(stmt).first()

    def get_all(self, include_disabled: bool = True) -> list[DirectoryProvider]:
        stmt = select(DirectoryProvider)
        if not include_disabled:
            stmt = stmt.where(DirectoryProvider.enabled.is_(True))
        return list(self.db_session.session.scalars(stmt).all())
