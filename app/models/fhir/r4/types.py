from datetime import datetime
from typing import List

from pydantic import BaseModel, ConfigDict, Field, AliasChoices


class Meta(BaseModel):
    model_config = ConfigDict(extra="allow")

    versionId: str | int = Field(alias="versionId")
    lastUpdated: datetime | None = Field(
        default=None,
    )
    source: str | None = Field(
        alias="source",
        default=None,
    )
    security: str | None = Field(
        alias="security",
        default=None,
    )
    tag: str | None = Field(alias="tag", default=None)


class Resource(BaseModel):
    model_config = ConfigDict(extra="allow")

    resource_type: str = Field(
        alias="resourceType",
        validation_alias=AliasChoices("resourceType", "resource_type"),
    )
    id: str | None = Field(alias="id", default=None)
    implicitRules: str | None = Field(
        default=None,
    )
    meta: Meta = Field(
        alias="meta",
    )


class Link(BaseModel):
    model_config = ConfigDict(extra="allow")

    relation: str | None = Field(
        alias="relation",
        default=None,
    )
    url: str | None = Field(alias="url", default=None)


class Request(BaseModel):
    model_config = ConfigDict(extra="allow")

    method: str | None = Field(alias="method", default=None)
    url: str | None = Field(alias="url", default=None)


class Response(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str | None = Field(alias="status", default=None)
    uri: str | None = Field(alias="uri", default=None)
    etag: str = Field(alias="etag")


class Entry(BaseModel):
    model_config = ConfigDict(extra="allow")

    link: Link | None = Field(alias="link", default=None)
    request: Request = Field(alias="request")
    response: Response = Field(alias="response")
    resource: Resource | None = Field(alias="resource", default=None)


class Bundle(BaseModel):
    model_config = ConfigDict(extra="allow")

    total: int | None = Field(alias="total", default=None)
    type: str = Field(alias="type")
    link: List[Link] | None = Field(alias="link", default=None)
    entry: List[Entry] | None = None
