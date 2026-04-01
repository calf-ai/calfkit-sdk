from typing import Annotated, Any, Literal

from pydantic import BaseModel, Discriminator, Field


class TextPart(BaseModel):
    kind: Literal["text"] = "text"
    text: str
    metadata: dict[str, Any] | None = None


class FilePart(BaseModel):
    kind: Literal["file"] = "file"
    media_type: str
    uri: str | None = None
    data: str | None = None
    metadata: dict[str, Any] | None = None


class DataPart(BaseModel):
    kind: Literal["data"] = "data"
    data: dict[str, Any] | list[Any] | Any
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")
    metadata: dict[str, Any] | None = None


class ToolCallPart(BaseModel):
    kind: Literal["tool"] = "tool"
    tool_call_id: str
    kwargs: dict[str, Any]
    tool_name: str
    metadata: dict[str, Any] | None = None


ContentPart = Annotated[TextPart | FilePart | DataPart | ToolCallPart, Discriminator("kind")]
