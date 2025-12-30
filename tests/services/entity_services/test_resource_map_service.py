from fastapi import HTTPException
import pytest
from sqlalchemy.exc import IntegrityError
from datetime import datetime

from app.services.entity.resource_map_service import ResourceMapService
from app.models.resource_map.dto import (
    ResourceMapDto,
    ResourceMapUpdateDto,
    ResourceMapDeleteDto,
)


@pytest.fixture
def mock_dto() -> ResourceMapDto:
    return ResourceMapDto(
        directory_id="example id",
        resource_type="Organization",
        directory_resource_id="some_resource_id",
        update_client_resource_id="some_update_client_resource_id",
    )


def test_get_should_return_a_resource_map_when_exists(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    expected = resource_map_service.add_one(mock_dto)
    actual = resource_map_service.get(
        directory_resource_id=mock_dto.directory_resource_id
    )
    assert actual is not None
    for k in actual.__table__.columns.keys():
        actual_val = getattr(actual, k)
        expected_val = getattr(expected, k)
        if isinstance(actual_val, datetime) and isinstance(expected_val, datetime):
            assert actual_val.replace(tzinfo=None) == expected_val.replace(tzinfo=None)
        else:
            assert actual_val == expected_val


def test_get_should_return_none_when_resource_map_does_not_exists(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    expected = resource_map_service.add_one(mock_dto)
    actual = resource_map_service.get(directory_resource_id="wrong_id")
    assert expected != actual
    assert actual is None


def test_get_one_should_return_a_resource_map_when_exist(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    expected = resource_map_service.add_one(mock_dto)
    actual = resource_map_service.get_one(
        directory_resource_id=mock_dto.directory_resource_id
    )
    for k in actual.__table__.columns.keys():
        actual_val = getattr(actual, k)
        expected_val = getattr(expected, k)
        if isinstance(actual_val, datetime) and isinstance(expected_val, datetime):
            assert actual_val.replace(tzinfo=None) == expected_val.replace(tzinfo=None)
        else:
            assert actual_val == expected_val


def test_get_one_should_raise_excption_when_map_does_not_exist(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)
    with pytest.raises(HTTPException):
        resource_map_service.get_one(directory_resource_id="wrong_id")


def test_find_should_return_results_when_param_matches(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    expected = resource_map_service.add_one(mock_dto)
    results = resource_map_service.find(
        directory_id=expected.directory_id,
        directory_resource_id=expected.directory_resource_id,
    )
    actual = results[0]
    assert len(results) > 0
    for k in actual.__table__.columns.keys():
        actual_val = getattr(actual, k)
        expected_val = getattr(expected, k)
        if isinstance(actual_val, datetime) and isinstance(expected_val, datetime):
            assert actual_val.replace(tzinfo=None) == expected_val.replace(tzinfo=None)
        else:
            assert actual_val == expected_val


def test_find_should_return_empty_list_when_params_do_not_match(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)
    results = resource_map_service.find(directory_id="wrong_id")
    assert len(results) == 0


def test_add_one_should_fail_when_resource_map_exist(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)
    with pytest.raises(HTTPException):
        resource_map_service.add_one(mock_dto)


def test_update_one_should_succeed_when_resource_map_exist(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)
    update_dto = ResourceMapUpdateDto(
        directory_id=mock_dto.directory_id,
        resource_type=mock_dto.resource_type,
        directory_resource_id=mock_dto.directory_resource_id,
    )
    expected = resource_map_service.update_one(update_dto)
    actual = resource_map_service.get_one(
        directory_resource_id=mock_dto.directory_resource_id
    )

    for k in actual.__table__.columns.keys():
        assert getattr(actual, k) == getattr(expected, k)


def test_update_one_should_raise_exception_when_resource_map_does_not_exist(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)
    update_dto = ResourceMapUpdateDto(
        resource_type=mock_dto.resource_type,
        directory_id="123",
        directory_resource_id="wrong_id",
    )

    with pytest.raises(HTTPException):
        resource_map_service.update_one(update_dto)


def test_delete_one_should_succeed_when_resource_map_exist(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)

    delete_dto = ResourceMapDeleteDto(
        directory_id=mock_dto.directory_id,
        resource_type=mock_dto.resource_type,
        directory_resource_id=mock_dto.directory_resource_id,
    )
    resource_map_service.delete_one(delete_dto)
    with pytest.raises(HTTPException):
        resource_map_service.get_one(directory_resource_id=mock_dto.directory_resource_id)


def test_unique_constraint_on_directory_id_and_directory_resource_id(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)
    duplicate_dto = ResourceMapDto(
        directory_id=mock_dto.directory_id,
        resource_type="Organization",
        directory_resource_id=mock_dto.directory_resource_id,
        update_client_resource_id="another_update_client_resource_id",
    )
    with pytest.raises(IntegrityError):
        resource_map_service.add_one(duplicate_dto)
