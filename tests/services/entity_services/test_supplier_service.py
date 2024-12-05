from fastapi import HTTPException
import pytest
from app.models.supplier.dto import SupplierCreateDto
from app.services.entity_services.supplier_service import SupplierService


@pytest.fixture
def mock_dto() -> SupplierCreateDto:
    return SupplierCreateDto(
        id="12345657", name="name", endpoint="http://some.endpoint"
    )


def test_add_one_should_succeed_when_supplier_is_new(
    supplier_service: SupplierService, mock_dto: SupplierCreateDto
) -> None:
    expected = supplier_service.add_one(mock_dto)
    assert mock_dto.id == expected.id


def test_add_one_should_fail_when_supplier_exists(
    supplier_service: SupplierService, mock_dto: SupplierCreateDto
) -> None:
    supplier_service.add_one(mock_dto)
    new_dto = SupplierCreateDto(
        id=mock_dto.id, name="some new name", endpoint="http://example.nl"
    )
    with pytest.raises(HTTPException):
        supplier_service.add_one(new_dto)
