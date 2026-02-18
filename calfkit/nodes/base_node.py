from abc import ABC
from collections.abc import Callable
from functools import cached_property
from typing import Any, TypedDict

from pydantic import BaseModel


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


def subscribe_private(topic_template: str) -> Callable[[Any], Any]:
    """Declare a private topic subscription resolved at init time from the node's name.

    The template uses Python str.format() syntax with `{name}` as the placeholder,
    e.g. ``@subscribe_private("agent_router.{name}.replies")``.
    """

    def decorator(fn: Any) -> Any:
        fn._private_topic_template = topic_template
        return fn

    return decorator


class TopicsDict(TypedDict, total=False):
    """Describes the pub/sub wiring for a single handler method."""

    publish_topic: str
    subscribe_topics: list[str]
    shared_subscribe_topic: str
    private_topic_template: str


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
        if name is not None:
            self._resolve_private_topics()

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        cls._handler_registry = {}

        for attr in cls.__dict__.values():
            publish_to_topic_name = getattr(attr, "_publish_to_topic_name", None)
            subscribe_to_topic_name = getattr(attr, "_subscribe_to_topic_name", None)
            private_topic_template = getattr(attr, "_private_topic_template", None)
            if publish_to_topic_name:
                cls._handler_registry[attr] = {"publish_topic": publish_to_topic_name}
            if subscribe_to_topic_name:
                cls._handler_registry[attr] = cls._handler_registry.get(attr, {})
                cls._handler_registry[attr]["shared_subscribe_topic"] = subscribe_to_topic_name
                cls._handler_registry[attr]["subscribe_topics"] = [subscribe_to_topic_name]
            if private_topic_template:
                cls._handler_registry[attr] = cls._handler_registry.get(attr, {})
                cls._handler_registry[attr]["private_topic_template"] = private_topic_template

    def _resolve_private_topics(self) -> None:
        """Resolve private topic templates for named nodes.

        When a handler is decorated with @subscribe_private, its template
        is resolved using the node's name, and the handler is subscribed
        to both the public and private topics.
        """
        for handler, topics in list(self.bound_registry.items()):
            template = topics.get("private_topic_template")
            subscribe_topics = topics.get("subscribe_topics", [])
            if template and self.name:
                resolved = template.format(name=self.name)
                self.bound_registry[handler] = {
                    **topics,
                    "subscribe_topics": [*subscribe_topics, resolved],
                    "private_topic_template": resolved,
                }

    @cached_property
    def subscribed_topic(self) -> str | None:
        for topics_dict in self._handler_registry.values():
            if "shared_subscribe_topic" in topics_dict:
                return topics_dict["shared_subscribe_topic"]
        return None

    @cached_property
    def publish_to_topic(self) -> str | None:
        for topics_dict in self._handler_registry.values():
            if "publish_topic" in topics_dict:
                return topics_dict["publish_topic"]
        return None

    @cached_property
    def private_subscribed_topic(self) -> str | None:
        if self.name is None:
            return None
        for topics_dict in self.bound_registry.values():
            if "private_topic_template" in topics_dict:
                return topics_dict["private_topic_template"]
        return None

    @cached_property
    def input_message_schema(self) -> type[BaseModel]: ...

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
