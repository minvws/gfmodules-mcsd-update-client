from pydantic import BaseModel


class SupplierBase(BaseModel):
    name: str
    endpoint: str


class SupplierCreateDto(SupplierBase):
    id: str


class SupplierUpdateDto(SupplierBase):
    pass
