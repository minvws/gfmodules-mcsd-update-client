from datetime import datetime
from typing import List

from pydantic import BaseModel, ConfigDict, Field, AliasChoices

class Coding(BaseModel):
    model_config = ConfigDict(extra="allow")

    system: str | None = Field(alias="system", default=None)
    version: str | None = Field(alias="version", default=None)
    code: str | None = Field(alias="code", default=None)
    display: str | None = Field(alias="display", default=None)
    userSelected: bool | None = Field(alias="userSelected", default=None)

class Meta(BaseModel):
    model_config = ConfigDict(extra="allow")

    versionId: str | int  | None = Field(alias="versionId", default=None)
    lastUpdated: datetime | None = Field(default=None)
    source: str | None = Field(alias="source", default=None)
    security: List[Coding] | None = Field(alias="security", default=None)
    tag: List[Coding] | None = Field(alias="tag", default=None)


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
    meta: Meta | None = Field(
        alias="meta",
        default=None,
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
    etag: str | None = Field(alias="etag", default=None)


class Entry(BaseModel):
    model_config = ConfigDict(extra="allow")

    link: Link | None = Field(alias="link", default=None)
    request: Request = Field(alias="request")
    response: Response = Field(alias="response")
    resource: Resource | None = Field(alias="resource", default=None)
    fullUrl: str = Field(alias="fullUrl")


class Bundle(BaseModel):
    model_config = ConfigDict(extra="allow")

    total: int | None = Field(alias="total", default=None)
    type: str = Field(alias="type")
    link: List[Link] | None = Field(alias="link", default=None)
    entry: List[Entry] | None = None
