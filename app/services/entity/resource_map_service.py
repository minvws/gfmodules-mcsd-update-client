from collections.abc import Sequence
from fastapi.exceptions import HTTPException
from app.db.db import Database
from app.db.entities.resource_map import ResourceMap
from app.db.repositories.resource_map_repository import ResourceMapRepository
from app.models.resource_map.dto import (
    ResourceMapDto,
    ResourceMapUpdateDto,
    ResourceMapDeleteDto,
)


class ResourceMapService:
    def __init__(self, database: Database) -> None:
        self.__database = database

    def get(
        self,
        directory_id: str | None = None,
        resource_type: str | None = None,
        directory_resource_id: str | None = None,
        update_client_resource_id: str | None = None,
    ) -> ResourceMap | None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            params = {
                "resource_type": resource_type,
                "directory_resource_id": directory_resource_id,
                "update_client_resource_id": update_client_resource_id,
                "directory_id": directory_id,
            }
            filtered_params = {k: v for k, v in params.items() if v is not None}
            resource_map = repository.get(**filtered_params)

            return resource_map

    def get_one(
        self,
        directory_id: str | None = None,
        resource_type: str | None = None,
        directory_resource_id: str | None = None,
        update_client_resource_id: str | None = None,
    ) -> ResourceMap:
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
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            return repository.find(
                directory_id=directory_id,
                resource_type=resource_type,
                directory_resource_id=directory_resource_id,
                update_client_resource_id=update_client_resource_id,
            )

    def add_one(self, dto: ResourceMapDto) -> ResourceMap:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            if repository.resource_map_exists(
                directory_resource_id=dto.directory_resource_id,
                update_client_resource_id=dto.update_client_resource_id,
            ):
                raise HTTPException(
                    status_code=400, detail="Resource Map already exist"
                )

            new_map = ResourceMap(**dto.model_dump())
            return repository.create(new_map)

    def update_one(self, dto: ResourceMapUpdateDto) -> ResourceMap:
        target = self.get_one(
            directory_id=dto.directory_id,
            resource_type=dto.resource_type,
            directory_resource_id=dto.directory_resource_id,
        )
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            target.resource_type = dto.resource_type

            return repository.update(target)

    def delete_one(self, dto: ResourceMapDeleteDto) -> None:
        target = self.get_one(
            directory_id=dto.directory_id,
            resource_type=dto.resource_type,
            directory_resource_id=dto.directory_resource_id,
        )

        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            return repository.delete(target)
