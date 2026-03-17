"""Test whether a generic class method can instantiate its TypeVar."""

from typing import Any, Generic, get_args

from pydantic import BaseModel
from typing_extensions import TypeVar

DataT = TypeVar("DataT", default=Any)


class MyInput(BaseModel):
    username: str
    score: int


class MyClass(Generic[DataT]):
    def build(self, raw: dict) -> DataT:
        # Can we do DataT(raw) or DataT.model_validate(raw)?
        # DataT is a TypeVar — let's see what it is at runtime
        print(f"  DataT = {DataT}")
        print(f"  type(DataT) = {type(DataT)}")

        # Attempt 1: call DataT directly
        try:
            result = DataT(**raw)
            print(f"  DataT(**raw) = {result}")
        except Exception as e:
            print(f"  DataT(**raw) failed: {type(e).__name__}: {e}")

        # Attempt 2: try __orig_class__ from inside the method
        orig = getattr(self, "__orig_class__", None)
        print(f"  __orig_class__ = {orig}")
        if orig:
            args = get_args(orig)
            if args:
                concrete = args[0]
                print(f"  concrete type = {concrete}")
                try:
                    result = concrete(**raw)
                    print(f"  concrete(**raw) = {result}, type = {type(result)}")
                    return result
                except Exception as e:
                    print(f"  concrete(**raw) failed: {e}")

        return raw  # type: ignore


print("=== MyClass[MyInput]().build() ===")
obj = MyClass[MyInput]()
result = obj.build({"username": "alice", "score": 42})
print(f"  final result type: {type(result)}")

print("\n=== MyClass().build() (no type param) ===")
obj2 = MyClass()
result2 = obj2.build({"username": "alice", "score": 42})
print(f"  final result type: {type(result2)}")
