from fastapi import HTTPException
import pytest
from sqlalchemy.exc import IntegrityError

from app.services.entity.resource_map_service import ResourceMapService
from app.models.resource_map.dto import (
    ResourceMapDto,
    ResourceMapUpdateDto,
    ResourceMapDeleteDto,
)


@pytest.fixture
def mock_dto() -> ResourceMapDto:
    return ResourceMapDto(
        supplier_id="example id",
        resource_type="Organization",
        supplier_resource_id="some_resource_id",
        consumer_resource_id="some_consumer_resource_id",
        history_size=1,
    )


def test_get_should_return_a_resource_map_when_exists(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    expected = resource_map_service.add_one(mock_dto)
    actual = resource_map_service.get(
        supplier_resource_id=mock_dto.supplier_resource_id
    )
    assert actual is not None
    for k in actual.__table__.columns.keys():
        assert getattr(actual, k) == getattr(expected, k)


def test_get_should_return_none_when_resource_map_does_not_exists(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    expected = resource_map_service.add_one(mock_dto)
    actual = resource_map_service.get(supplier_resource_id="wrong_id")
    assert expected != actual
    assert actual is None


def test_get_one_should_return_a_resource_map_when_exist(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    expected = resource_map_service.add_one(mock_dto)
    actual = resource_map_service.get_one(
        supplier_resource_id=mock_dto.supplier_resource_id
    )
    for k in actual.__table__.columns.keys():
        assert getattr(actual, k) == getattr(expected, k)


def test_get_one_should_raise_excption_when_map_does_not_exist(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)
    with pytest.raises(HTTPException):
        resource_map_service.get_one(supplier_resource_id="wrong_id")


def test_find_should_return_results_when_param_matches(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    expected = resource_map_service.add_one(mock_dto)
    results = resource_map_service.find(
        supplier_id=expected.supplier_id,
        supplier_resource_id=expected.supplier_resource_id,
    )
    actual = results[0]
    assert len(results) > 0
    for k in actual.__table__.columns.keys():
        assert getattr(actual, k) == getattr(expected, k)


def test_find_should_return_empty_list_when_params_do_not_match(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)
    results = resource_map_service.find(supplier_id="wrong_id")
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
    original = resource_map_service.add_one(mock_dto)
    update_dto = ResourceMapUpdateDto(
        supplier_id=mock_dto.supplier_id,
        resource_type=mock_dto.resource_type,
        supplier_resource_id=mock_dto.supplier_resource_id,
        history_size=2,
    )
    expected = resource_map_service.update_one(update_dto)
    actual = resource_map_service.get_one(
        supplier_resource_id=mock_dto.supplier_resource_id
    )

    for k in actual.__table__.columns.keys():
        assert getattr(actual, k) == getattr(expected, k)

    assert original.last_update != actual.last_update


def test_update_one_should_raise_exception_when_resource_map_does_not_exist(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)
    update_dto = ResourceMapUpdateDto(
        resource_type=mock_dto.resource_type,
        supplier_id="123",
        supplier_resource_id="wrong_id",
        history_size=2,
    )

    with pytest.raises(HTTPException):
        resource_map_service.update_one(update_dto)


def test_delete_one_should_succeed_when_resource_map_exist(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)

    delete_dto = ResourceMapDeleteDto(
        supplier_id=mock_dto.supplier_id,
        resource_type=mock_dto.resource_type,
        supplier_resource_id=mock_dto.supplier_resource_id,
    )
    resource_map_service.delete_one(delete_dto)
    with pytest.raises(HTTPException):
        resource_map_service.get_one(supplier_resource_id=mock_dto.supplier_resource_id)


def test_unique_constraint_on_supplier_id_and_supplier_resource_id(
    resource_map_service: ResourceMapService, mock_dto: ResourceMapDto
) -> None:
    resource_map_service.add_one(mock_dto)
    duplicate_dto = ResourceMapDto(
        supplier_id=mock_dto.supplier_id,
        resource_type="Organization",
        supplier_resource_id=mock_dto.supplier_resource_id,
        consumer_resource_id="another_consumer_resource_id",
        history_size=2,
    )
    with pytest.raises(IntegrityError):
        resource_map_service.add_one(duplicate_dto)
