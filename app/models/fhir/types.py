from typing import Literal

from pydantic import BaseModel

HttpValidVerbs = Literal["GET", "POST", "PATCH", "POST", "PUT", "HEAD", "DELETE"]


class BundleRequestParams(BaseModel):
    id: str
    resource_type: str
