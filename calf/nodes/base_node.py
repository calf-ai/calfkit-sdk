from abc import ABC
from collections.abc import Callable
from functools import cached_property
from typing import Any


def subscribe_to(topic_name: str) -> Callable[[Any], Any]:
    def decorator(fn: Any) -> Any:
        fn._subscribe_to_topic_name = topic_name
        return fn

    return decorator


def publish_to(topic_name: str) -> Callable[[Any], Any]:
    def decorator(fn: Any) -> Any:
        fn._publish_to_topic_name = topic_name
        return fn

    return decorator


class BaseNode(ABC):
    _handler_registry: dict[Callable[..., Any], dict[str, str]] = {}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.bound_registry: dict[Callable[..., Any], dict[str, str]] = {
            fn.__get__(self, type(self)): topics_dict
            for fn, topics_dict in self._handler_registry.items()
        }

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        cls._handler_registry = {}

        for attr in cls.__dict__.values():
            publish_to_topic_name = getattr(attr, "_publish_to_topic_name", None)
            subscribe_to_topic_name = getattr(attr, "_subscribe_to_topic_name", None)
            if publish_to_topic_name:
                cls._handler_registry[attr] = {"publish_topic": publish_to_topic_name}
            if subscribe_to_topic_name:
                cls._handler_registry[attr] = cls._handler_registry.get(attr, {}) | {
                    "subscribe_topic": subscribe_to_topic_name
                }

    @cached_property
    def subscribed_topic(self) -> str | None:
        for topics_dict in self._handler_registry.values():
            if "subscribe_topic" in topics_dict:
                return topics_dict["subscribe_topic"]
        return None

    @cached_property
    def publish_to_topic(self) -> str | None:
        for topics_dict in self._handler_registry.values():
            if "publish_topic" in topics_dict:
                return topics_dict["publish_topic"]
        return None
