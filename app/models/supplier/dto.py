from pydantic import BaseModel


class SupplierBase(BaseModel):
    ura_number: str
    name: str
    endpoint: str
    is_deleted: bool = False


class SupplierDto(SupplierBase):
    id: str


class SupplierUpdateDto(SupplierBase):
    pass
