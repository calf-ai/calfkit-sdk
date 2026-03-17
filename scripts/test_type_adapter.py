"""Test whether Pydantic TypeAdapter can unify BaseModel, dataclass, and TypedDict construction."""

from dataclasses import dataclass
from typing import TypedDict

from pydantic import BaseModel, TypeAdapter


# --- Three different type forms ---

class MyBaseModel(BaseModel):
    username: str
    score: int


@dataclass
class MyDataclass:
    username: str
    score: int


class MyTypedDict(TypedDict):
    username: str
    score: int


raw = {"username": "alice", "score": 42}


for label, typ in [
    ("BaseModel", MyBaseModel),
    ("dataclass", MyDataclass),
    ("TypedDict", MyTypedDict),
]:
    print(f"=== {label} ===")
    adapter = TypeAdapter(typ)
    result = adapter.validate_python(raw)
    print(f"  type(result) = {type(result)}")
    print(f"  result = {result}")
    print(f"  isinstance check: {isinstance(result, typ)}")
    print()
