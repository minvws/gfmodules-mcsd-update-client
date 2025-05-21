from fastapi import HTTPException
import pytest
from app.models.supplier.dto import SupplierDto
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.supplier_provider.json_provider import SupplierJsonProvider


@pytest.fixture
def json_data() -> list[tuple[str, str, str]]:
    return [
        ("1", "Supplier 1", "http://supplier1.com"),
        ("2", "Supplier 2", "http://supplier2.com"),
    ]


@pytest.fixture
def json_provider(json_data: list[tuple[str, str, str]], supplier_ignored_directory_service: SupplierIgnoredDirectoryService
) -> SupplierJsonProvider:
    return SupplierJsonProvider([
        SupplierDto(id=id, name=name, endpoint=endpoint)
        for id, name, endpoint in json_data
    ], supplier_ignored_directory_service=supplier_ignored_directory_service)

def test_get_all_suppliers_should_ignore_ignored_if_specified(
    json_provider: SupplierJsonProvider,
    supplier_ignored_directory_service: SupplierIgnoredDirectoryService,
) -> None:
    supplier_ignored_directory_service.add_directory_to_ignore_list("1")
    
    # Test without ignored suppliers
    result = json_provider.get_all_suppliers(include_ignored=False)
    assert result is not None
    assert len(result) == 1
    assert result[0].id == "2"
    
    # Test with ignored suppliers
    result = json_provider.get_all_suppliers(include_ignored=True)
    assert result is not None
    assert len(result) == 2

    result = json_provider.get_all_suppliers_include_ignored(include_ignored_ids=[])
    assert result is not None
    assert len(result) == 1
    result = json_provider.get_all_suppliers_include_ignored(include_ignored_ids=["1"])
    assert result is not None
    assert len(result) == 2

def test_get_all_suppliers_should_return_all_suppliers(
    json_provider: SupplierJsonProvider,
) -> None:
    result = json_provider.get_all_suppliers()
    assert len(result) == 2
    assert isinstance(result, list)
    assert isinstance(result[0], SupplierDto)
    assert isinstance(result[1], SupplierDto)
    assert result[0].id == "1"
    assert result[0].name == "Supplier 1"
    assert result[1].id == "2"
    assert result[1].name == "Supplier 2"


def test_get_one_supplier_should_return_correct_supplier(
    json_provider: SupplierJsonProvider,
) -> None:
    result = json_provider.get_one_supplier("1")
    assert result is not None
    assert isinstance(result, SupplierDto)
    assert result.id == "1"
    assert result.name == "Supplier 1"


def test_get_one_supplier_should_return_none_if_not_found(
    json_provider: SupplierJsonProvider,
) -> None:
    with pytest.raises(HTTPException):
        json_provider.get_one_supplier("3")
