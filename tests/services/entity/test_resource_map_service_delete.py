from app.models.resource_map.dto import ResourceMapDeleteDto, ResourceMapDto
from app.services.entity.resource_map_service import ResourceMapService


def test_apply_dtos_should_delete_mapping(resource_map_service: ResourceMapService) -> None:
    directory_id = "dir-1"
    resource_type = "Organization"
    directory_resource_id = "org-1"
    update_client_resource_id = "uc-1"

    resource_map_service.apply_dtos(
        [
            ResourceMapDto(
                directory_id=directory_id,
                resource_type=resource_type,
                directory_resource_id=directory_resource_id,
                update_client_resource_id=update_client_resource_id,
            )
        ]
    )
    assert (
        resource_map_service.get(
            directory_id=directory_id,
            resource_type=resource_type,
            directory_resource_id=directory_resource_id,
        )
        is not None
    )

    resource_map_service.apply_dtos(
        [
            ResourceMapDeleteDto(
                directory_id=directory_id,
                resource_type=resource_type,
                directory_resource_id=directory_resource_id,
            )
        ]
    )

    assert (
        resource_map_service.get(
            directory_id=directory_id,
            resource_type=resource_type,
            directory_resource_id=directory_resource_id,
        )
        is None
    )
