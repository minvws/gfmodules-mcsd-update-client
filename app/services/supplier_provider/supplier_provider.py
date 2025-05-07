from abc import ABC
import abc
from typing import List

from app.models.supplier.dto import SupplierDto


class SupplierProvider(ABC):
    """
    Abstract base class for supplier provider services.
    This class defines the interface for interacting with supplier data.
    Implementations of this class should provide concrete logic for the
    following methods:
    Methods:
        get_all_suppliers() -> List[SupplierDto]:
            Retrieve a list of all suppliers.
        get_one_supplier(supplier_id: str) -> SupplierDto:
            Retrieve a specific supplier by their unique identifier.
    """

    @abc.abstractmethod
    def get_all_suppliers(self) -> List[SupplierDto]:
        """
        Returns a list of all suppliers or raises Exception if the supplier provider could not be reached.
        """
        pass

    @abc.abstractmethod
    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        """
        Returns a specific supplier by their unique identifier or raises Exception if the supplier provider could not be reached.
        """
        pass
