from pydantic import BaseModel


class DirectoryBase(BaseModel):
    name: str
    endpoint: str
    is_deleted: bool = False


class DirectoryDto(DirectoryBase):
    id: str


class DirectoryUpdateDto(DirectoryBase):
    pass
