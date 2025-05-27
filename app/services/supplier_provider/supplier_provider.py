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
        get_all_suppliers(include_ignored: bool = False) -> List[SupplierDto]:
            Retrieve a list of all suppliers. If `include_ignored` is True, ignored suppliers are included; otherwise, they are filtered out.
        get_all_suppliers_include_ignored(include_ignored_ids: List[str]) -> List[SupplierDto]:
            Retrieve a list of all suppliers, including those specified in the ignore list.
        get_one_supplier(supplier_id: str) -> SupplierDto:
            Retrieve a specific supplier by their unique identifier.
    """

    @abc.abstractmethod
    def get_all_suppliers(self, include_ignored: bool = False) -> List[SupplierDto]:
        """
        Returns a list of all suppliers, also including ignored ones if specified, otherwise these are filtered out.
        or raises Exception if the supplier provider could not be reached.
        """
        pass

    @abc.abstractmethod
    def get_all_suppliers_include_ignored(self, include_ignored_ids: List[str]) -> List[SupplierDto]:
        """
        Returns a list of all suppliers including, if specified, the suppliers which id is in the ignore list, otherwise these are filtered out.
        Raises Exception if the supplier provider could not be reached.
        """
        pass

    @abc.abstractmethod
    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        """
        Returns a specific supplier by their unique identifier or raises Exception if the supplier provider could not be reached.
        """
        pass
