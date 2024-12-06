from fastapi import HTTPException
import pytest
from app.db.entities.supplier import Supplier
from app.models.supplier.dto import SupplierDto, SupplierUpdateDto
from app.services.entity_services.supplier_service import SupplierService


@pytest.fixture
def mock_dto() -> SupplierDto:
    return SupplierDto(
        id="12345657", name="name", endpoint="http://some.endpoint"
    )


def test_add_one_should_succeed_when_supplier_is_new(
    supplier_service: SupplierService, mock_dto: SupplierDto
) -> None:
    expected = supplier_service.add_one(mock_dto)
    assert mock_dto.id == expected.id


def test_add_one_should_raise_exception_when_supplier_exists(
    supplier_service: SupplierService, mock_dto: SupplierDto
) -> None:
    supplier_service.add_one(mock_dto)
    new_dto = SupplierDto(
        id=mock_dto.id, name="some new name", endpoint="http://example.nl"
    )
    with pytest.raises(HTTPException):
        supplier_service.add_one(new_dto)


def test_get_one_should_succeed_when_id_is_correct(
    supplier_service: SupplierService, mock_dto: SupplierDto
) -> None:
    expected = supplier_service.add_one(mock_dto)
    actual = supplier_service.get_one(mock_dto.id)
    assert actual is not None
    assert isinstance(actual, Supplier)
    for k in actual.__table__.columns.keys():
        assert getattr(actual, k) == getattr(expected, k)


def test_get_one_should_raise_exception_when_id_does_not_exists(
    supplier_service: SupplierService, mock_dto: SupplierDto
) -> None:
    supplier_service.add_one(mock_dto)
    with pytest.raises(HTTPException):
        supplier_service.get_one("some wrong id")


def test_update_one_should_succeed_when_supplier_exists(
    supplier_service: SupplierService, mock_dto: SupplierDto
) -> None:
    supplier_service.add_one(mock_dto)
    expected = supplier_service.update_one(
        mock_dto.id, SupplierUpdateDto(name="new name", endpoint="http://new.endpoint")
    )
    actual = supplier_service.get_one(mock_dto.id)

    for k in actual.__table__.columns.keys():
        assert getattr(actual, k) == getattr(expected, k)


def test_update_one_should_raise_exception_when_id_does_not_exists(
    supplier_service: SupplierService, mock_dto: SupplierDto
) -> None:
    supplier_service.add_one(mock_dto)
    with pytest.raises(HTTPException):
        supplier_service.update_one(
            "incorrect_id",
            SupplierUpdateDto(name="new name", endpoint="http://new.endpoint"),
        )


def test_delete_one_should_suceed_when_supplier_exists(
    supplier_service: SupplierService, mock_dto: SupplierDto
) -> None:
    supplier_service.add_one(mock_dto)
    supplier_service.delete_one(mock_dto.id)

    with pytest.raises(HTTPException) as e:
        supplier_service.get_one(mock_dto.id)
        assert isinstance(e, HTTPException)
        assert e.status_code == 404


def test_delete_one_should_raise_exception_when_id_does_not_exists(
    supplier_service: SupplierService, mock_dto: SupplierDto
) -> None:
    supplier_service.add_one(mock_dto)
    with pytest.raises(HTTPException):
        supplier_service.delete_one("incorrect_id")
