"""Test whether decorator-set attributes on methods are inherited by subclasses."""

from typing import Any
from collections.abc import Callable


def publish_to(topic_name: str) -> Callable[[Any], Any]:
    def decorator(fn: Any) -> Any:
        fn._publish_to_topic_name = topic_name
        return fn
    return decorator


class Parent:
    @publish_to("my-topic")
    def run(self):
        return "Parent.run"


class ChildNoOverride(Parent):
    """Inherits run() without overriding."""
    pass


class ChildOverride(Parent):
    """Overrides run() without decorator."""
    def run(self):
        return "ChildOverride.run"


class ChildOverrideDecorated(Parent):
    """Overrides run() with decorator."""
    @publish_to("child-topic")
    def run(self):
        return "ChildOverrideDecorated.run"


for label, cls in [
    ("Parent", Parent),
    ("ChildNoOverride", ChildNoOverride),
    ("ChildOverride (no decorator)", ChildOverride),
    ("ChildOverrideDecorated", ChildOverrideDecorated),
]:
    print(f"\n=== {label} ===")
    fn = cls.run  # unbound
    attr = getattr(fn, "_publish_to_topic_name", "NOT SET")
    print(f"  cls.run._publish_to_topic_name = {attr}")
    print(f"  'run' in cls.__dict__? {('run' in cls.__dict__)}")
    print(f"  cls.run is Parent.run? {cls.run is Parent.run}")
