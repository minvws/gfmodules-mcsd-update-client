from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timezone

from fastapi.exceptions import HTTPException

from app.db.db import Database
from app.db.entities.resource_map import ResourceMap
from app.db.repositories.resource_map_repository import ResourceMapRepository
from app.models.resource_map.dto import (
    ResourceMapDeleteDto,
    ResourceMapDto,
    ResourceMapUpdateDto,
)

logger = logging.getLogger(__name__)


class ResourceMapService:
    """
    Service to manage resource maps in the database.

    Performance note:
    - The original implementation performed a DB transaction for each DTO.
    - For directory updates this can become a hotspot.
    - `apply_dtos` applies multiple DTOs in a single session/transaction.
    """

    def __init__(self, database: Database) -> None:
        self.__database = database

    def get(
        self,
        directory_id: str | None = None,
        resource_type: str | None = None,
        directory_resource_id: str | None = None,
        update_client_resource_id: str | None = None,
    ) -> ResourceMap | None:
        """Retrieve a resource map based on provided parameters."""
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            params = {
                "resource_type": resource_type,
                "directory_resource_id": directory_resource_id,
                "update_client_resource_id": update_client_resource_id,
                "directory_id": directory_id,
            }
            filtered_params = {k: v for k, v in params.items() if v is not None}
            return repository.get(**filtered_params)

    def get_one(
        self,
        directory_id: str | None = None,
        resource_type: str | None = None,
        directory_resource_id: str | None = None,
        update_client_resource_id: str | None = None,
    ) -> ResourceMap:
        """Retrieve a single resource map based on provided parameters."""
        resource_map = self.get(
            directory_id=directory_id,
            resource_type=resource_type,
            directory_resource_id=directory_resource_id,
            update_client_resource_id=update_client_resource_id,
        )
        if resource_map is None:
            raise HTTPException(status_code=404, detail="Not Found")

        return resource_map

    def find(
        self,
        directory_id: str | None = None,
        resource_type: str | None = None,
        directory_resource_id: str | None = None,
        update_client_resource_id: str | None = None,
    ) -> Sequence[ResourceMap]:
        """Find resource maps based on provided parameters."""
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            return repository.find(
                directory_id=directory_id,
                resource_type=resource_type,
                directory_resource_id=directory_resource_id,
                update_client_resource_id=update_client_resource_id,
            )

    def add_one(self, dto: ResourceMapDto) -> ResourceMap:
        """Add a new resource map to the database."""
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            if repository.resource_map_exists(
                directory_resource_id=dto.directory_resource_id,
                update_client_resource_id=dto.update_client_resource_id,
            ):
                raise HTTPException(status_code=400, detail="Resource Map already exist")

            new_map = ResourceMap(**dto.model_dump())
            return repository.create(new_map)

    def update_one(self, dto: ResourceMapUpdateDto) -> ResourceMap:
        """Update an existing resource map in the database."""
        target = self.get_one(
            directory_id=dto.directory_id,
            resource_type=dto.resource_type,
            directory_resource_id=dto.directory_resource_id,
        )
        now = datetime.now(timezone.utc)

        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            # Touch timestamps explicitly; SQLAlchemy may optimize away no-op updates.
            target.last_update = now
            target.modified_at = now
            return repository.update(target)

    def delete_one(self, dto: ResourceMapDeleteDto) -> None:
        """Delete a resource map from the database."""
        target = self.get_one(
            directory_id=dto.directory_id,
            resource_type=dto.resource_type,
            directory_resource_id=dto.directory_resource_id,
        )

        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            return repository.delete(target)

    def apply_dtos(self, dtos: Sequence[ResourceMapDto | ResourceMapUpdateDto | ResourceMapDeleteDto]) -> None:
        """
        Apply multiple DTOs in a single DB session/transaction.

        - DTOs are de-duplicated by (directory_id, resource_type, directory_resource_id).
        - Missing mappings during an update are treated as recoverable and will be created.
        - Deletes remove the mapping entry.
        """
        if not dtos:
            return

        deduped: dict[tuple[str, str, str], ResourceMapDto | ResourceMapUpdateDto | ResourceMapDeleteDto] = {}
        for dto in dtos:
            key = (dto.directory_id, dto.resource_type, dto.directory_resource_id)  # type: ignore[attr-defined]
            deduped[key] = dto

        now = datetime.now(timezone.utc)

        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)

            for (directory_id, resource_type, directory_resource_id), dto in deduped.items():
                existing = repository.get(
                    directory_id=directory_id,
                    resource_type=resource_type,
                    directory_resource_id=directory_resource_id,
                )

                if isinstance(dto, ResourceMapDto):
                    if existing is None:
                        session.add(ResourceMap(**dto.model_dump()))
                    else:
                        # Mapping already exists; keep it consistent and touch timestamps.
                        existing.update_client_resource_id = dto.update_client_resource_id
                        existing.last_update = now
                        existing.modified_at = now
                        session.add(existing)

                elif isinstance(dto, ResourceMapUpdateDto):
                    if existing is None:
                        # Recoverable inconsistency: create the mapping if missing.
                        update_client_resource_id = f"{directory_id}-{directory_resource_id}"
                        session.add(
                            ResourceMap(
                                directory_id=directory_id,
                                resource_type=resource_type,
                                directory_resource_id=directory_resource_id,
                                update_client_resource_id=update_client_resource_id,
                                last_update=now,
                                created_at=now,
                                modified_at=now,
                            )
                        )
                    else:
                        existing.last_update = now
                        existing.modified_at = now
                        session.add(existing)


                elif isinstance(dto, ResourceMapDeleteDto):
                    if existing is not None:
                        session.delete(existing)
                    else:
                        logger.debug(
                            "Skipping delete; mapping not found. directory_id=%s resource_type=%s directory_resource_id=%s",
                            directory_id,
                            resource_type,
                            directory_resource_id,
                        )

            session.commit()
