from pydantic import BaseModel


class SupplierBase(BaseModel):
    name: str
    endpoint: str


class SupplierDto(SupplierBase):
    id: str


class SupplierUpdateDto(SupplierBase):
    pass
