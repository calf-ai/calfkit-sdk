from typing import Any, TypeVar

from pydantic import BaseModel, model_validator


class UnsetType:
    __slots__ = ()

    def __bool__(self):
        return False

    def __repr__(self):
        return "UNSET"


UNSET = UnsetType()


class CompactBaseModel(BaseModel):
    """Base model that excludes unset and None values during serialization."""

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("exclude_unset", True)
        kwargs.setdefault("exclude_none", True)
        return super().model_dump(**kwargs)

    def model_dump_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("exclude_unset", True)
        kwargs.setdefault("exclude_none", True)
        return super().model_dump_json(**kwargs)
