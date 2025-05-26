from typing import List
import pytest
from app.models.supplier.dto import SupplierDto
from app.services.supplier_provider.supplier_provider import SupplierProvider


class MockSupplierProvider(SupplierProvider):
    def __init__(self, suppliers: list[SupplierDto]) -> None:
        self.suppliers = suppliers

    def get_all_suppliers(self, include_ignored: bool = False) -> List[SupplierDto]:
        return self.suppliers
    
    def get_all_suppliers_include_ignored(self, include_ignored_ids: List[str]) -> List[SupplierDto]:
        return self.suppliers

    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        supplier =  next((s for s in self.suppliers if s.id == supplier_id), None)
        if supplier is None:
            raise ValueError(f"Supplier with ID '{supplier_id}' not found.")
        return supplier


@pytest.fixture
def mock_suppliers() -> list[SupplierDto]:
    return [
        SupplierDto(
            id="1", name="Supplier 1", endpoint="http://supplier1.com", is_deleted=False
        ),
        SupplierDto(
            id="2", name="Supplier 2", endpoint="http://supplier2.com", is_deleted=False
        ),
    ]


@pytest.fixture
def supplier_provider(mock_suppliers: list[SupplierDto]) -> MockSupplierProvider:
    return MockSupplierProvider(mock_suppliers)


def test_get_all_suppliers_should_return_suppliers(
    supplier_provider: MockSupplierProvider, mock_suppliers: list[SupplierDto]
) -> None:
    result = supplier_provider.get_all_suppliers()
    assert result == mock_suppliers
    assert len(result) == 2
    assert result[0].id == "1"
    assert result[1].name == "Supplier 2"


def test_get_all_suppliers_should_return_none_when_no_suppliers() -> None:
    provider = MockSupplierProvider([])
    result = provider.get_all_suppliers()
    assert len(result) == 0


def test_get_one_supplier_should_return_supplier_when_exists(
    supplier_provider: MockSupplierProvider,
) -> None:
    result = supplier_provider.get_one_supplier("1")
    assert result is not None
    assert result.id == "1"
    assert result.name == "Supplier 1"


def test_get_one_supplier_should_return_none_when_not_exists(
    supplier_provider: MockSupplierProvider,
) -> None:
    with pytest.raises(ValueError):
        supplier_provider.get_one_supplier("3")


def test_get_one_supplier_should_return_correct_supplier(
    supplier_provider: MockSupplierProvider,
) -> None:
    result = supplier_provider.get_one_supplier("2")
    assert result is not None
    assert result.id == "2"
    assert result.name == "Supplier 2"
