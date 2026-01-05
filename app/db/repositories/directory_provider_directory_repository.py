from datetime import datetime

from sqlalchemy import select

from app.db.entities.directory_provider_directory import DirectoryProviderDirectory
from app.db.repositories.repository_base import RepositoryBase


class DirectoryProviderDirectoryRepository(RepositoryBase):
    def get_by_provider(self, provider_id: int) -> list[DirectoryProviderDirectory]:
        stmt = select(DirectoryProviderDirectory).where(
            DirectoryProviderDirectory.provider_id == provider_id
        )
        return list(self.db_session.session.scalars(stmt).all())

    def get_active_by_provider(self, provider_id: int) -> list[DirectoryProviderDirectory]:
        stmt = select(DirectoryProviderDirectory).where(
            DirectoryProviderDirectory.provider_id == provider_id,
            DirectoryProviderDirectory.removed_at.is_(None),
        )
        return list(self.db_session.session.scalars(stmt).all())

    def count_active_for_directory(self, directory_id: str) -> int:
        stmt = select(DirectoryProviderDirectory).where(
            DirectoryProviderDirectory.directory_id == directory_id,
            DirectoryProviderDirectory.removed_at.is_(None),
        )
        return len(list(self.db_session.session.scalars(stmt).all()))

    def upsert_seen(self, provider_id: int, directory_id: str, now: datetime) -> None:
        entry = self.db_session.session.get(
            DirectoryProviderDirectory, {"provider_id": provider_id, "directory_id": directory_id}
        )
        if entry is None:
            entry = DirectoryProviderDirectory(
                provider_id=provider_id,
                directory_id=directory_id,
                first_seen_at=now,
                last_seen_at=now,
                removed_at=None,
            )
            self.db_session.session.add(entry)
            return
        entry.last_seen_at = now
        entry.removed_at = None

    def mark_removed_if_not_seen(self, provider_id: int, seen_directory_ids: set[str], now: datetime) -> list[str]:
        removed: list[str] = []
        active = self.get_active_by_provider(provider_id)
        for row in active:
            if row.directory_id in seen_directory_ids:
                continue
            row.removed_at = now
            removed.append(row.directory_id)
        return removed
