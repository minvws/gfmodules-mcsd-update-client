from pydantic import BaseModel


class SupplierBase(BaseModel):
    name: str
    endpoint: str
    is_deleted: bool = False


class SupplierDto(SupplierBase):
    id: str


class SupplierUpdateDto(SupplierBase):
    pass
