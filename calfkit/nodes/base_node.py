from abc import ABC
from collections.abc import Callable
from functools import cached_property
from typing import Any, TypedDict


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


class TopicsDict(TypedDict, total=False):
    """Describes the pub/sub wiring for a single handler method."""

    publish_topic: str
    subscribe_topic: str
    subscribe_topics: list[str]


class BaseNode(ABC):
    """Effectively a node is the data plane, defining the internal wiring and logic.
    When provided to a NodeRunner, node logic can be deployed."""

    _handler_registry: dict[Callable[..., Any], TopicsDict] = {}

    def __init__(self, name: str | None = None, *args: Any, **kwargs: Any) -> None:
        self.name = name
        self.bound_registry: dict[Callable[..., Any], TopicsDict] = {
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

    async def invoke(self, *args: Any, **kwargs: Any) -> str:
        raise NotImplementedError()

    async def _invoke_from_node(self, *args: Any, **kwargs: Any) -> None:
        """Internal use method for other nodes to use and communicate with this node

        Args:
            TBD

        Returns:
            TBD
        """
        raise NotImplementedError()
