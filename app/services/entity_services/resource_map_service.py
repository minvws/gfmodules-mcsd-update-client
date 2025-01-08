from collections.abc import Sequence
from fastapi.exceptions import HTTPException
from app.db.db import Database
from app.db.entities.resource_map import ResourceMap
from app.db.repositories.resource_map_repository import ResourceMapRepository
from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto, ResourceMapDeleteDto


class ResourceMapService:
    def __init__(self, database: Database) -> None:
        self.__database = database

    def get(
        self,
        supplier_id: str | None = None,
        resource_type: str | None = None,
        supplier_resource_id: str | None = None,
        consumer_resource_id: str | None = None,
    ) -> ResourceMap | None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            params = {
                "resource_type": resource_type,
                "supplier_resource_id": supplier_resource_id,
                "consumer_resource_id": consumer_resource_id,
                "supplier_id": supplier_id,
            }
            filtered_params = {k: v for k, v in params.items() if v is not None}
            resource_map = repository.get(**filtered_params)

            return resource_map

    def get_one(
        self,
        supplier_id: str | None = None,
        resource_type: str | None = None,
        supplier_resource_id: str | None = None,
        consumer_resource_id: str | None = None,
    ) -> ResourceMap:
        resource_map = self.get(
            supplier_id=supplier_id,
            resource_type=resource_type,
            supplier_resource_id=supplier_resource_id,
            consumer_resource_id=consumer_resource_id
        )
        if resource_map is None:
            raise HTTPException(status_code=404, detail="Not Found")

        return resource_map

    def find(
        self,
        supplier_id: str | None = None,
        resource_type: str | None = None,
        supplier_resource_id: str | None = None,
        consumer_resource_id: str | None = None,
        history_size: int | None = None,
    ) -> Sequence[ResourceMap]:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            return repository.find(
                supplier_id=supplier_id,
                resource_type=resource_type,
                supplier_resource_id=supplier_resource_id,
                consumer_resource_id=consumer_resource_id,
                history_size=history_size,
            )

    def add_one(self, dto: ResourceMapDto) -> ResourceMap:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            if repository.resource_map_exists(
                supplier_resource_id=dto.supplier_resource_id,
                consumer_resource_id=dto.consumer_resource_id,
            ):
                raise HTTPException(
                    status_code=400, detail="Resource Map already exist"
                )

            new_map = ResourceMap(**dto.model_dump())
            return repository.create(new_map)

    def update_one(self, dto: ResourceMapUpdateDto) -> ResourceMap:
        target = self.get_one(
            supplier_id=dto.supplier_id,
            resource_type=dto.resource_type,
            supplier_resource_id=dto.supplier_resource_id,
        )
        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            target.resource_type = dto.resource_type
            target.history_size = dto.history_size

            return repository.update(target)

    def delete_one(self, dto: ResourceMapDeleteDto) -> None:
        target = self.get_one(
            supplier_id=dto.supplier_id,
            resource_type=dto.resource_type,
            supplier_resource_id=dto.supplier_resource_id,
        )

        with self.__database.get_db_session() as session:
            repository = session.get_repository(ResourceMapRepository)
            return repository.delete(target)
