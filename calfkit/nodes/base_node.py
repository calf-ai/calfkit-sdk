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


def entrypoint(topic_template: str) -> Callable[[Any], Any]:
    """Declare an entrypoint topic — the topic where this node receives incoming work.

    The template uses Python str.format() syntax with `{name}` as the placeholder,
    e.g. ``@entrypoint("agent_router.private.{name}")``.
    """

    def decorator(fn: Any) -> Any:
        fn._entrypoint_topic_template = topic_template
        return fn

    return decorator


def returnpoint(topic_template: str) -> Callable[[Any], Any]:
    """Declare a returnpoint topic — the topic where this node receives delegated responses.

    The template uses Python str.format() syntax with `{name}` as the placeholder,
    e.g. ``@returnpoint("tool_node.handoff.response.{name}")``.
    """

    def decorator(fn: Any) -> Any:
        fn._returnpoint_topic_template = topic_template
        return fn

    return decorator


class TopicsDict(TypedDict, total=False):
    """Describes the pub/sub wiring for a single handler method."""

    publish_topic: str
    subscribe_topics: list[str]
    shared_subscribe_topic: str
    entrypoint_topic_template: str
    returnpoint_topic_template: str


class BaseNode(ABC):
    """Effectively a node is the data plane, defining the internal wiring and logic.
    When provided to a NodeRunner, node logic can be deployed."""

    _handler_registry: dict[Callable[..., Any], TopicsDict] = {}

    def __init__(
        self,
        name: str | None = None,
        *args: Any,
        input_topic: str | list[str] | None = None,
        output_topic: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.name = name
        self.bound_registry: dict[Callable[..., Any], TopicsDict] = {
            fn.__get__(self, type(self)): topics_dict
            for fn, topics_dict in self._handler_registry.items()
        }
        if name is not None:
            self._resolve_private_topics()
        if input_topic is not None or output_topic is not None:
            self._apply_topic_overrides(input_topic, output_topic)

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        cls._handler_registry = {}

        for attr in cls.__dict__.values():
            publish_to_topic_name = getattr(attr, "_publish_to_topic_name", None)
            subscribe_to_topic_name = getattr(attr, "_subscribe_to_topic_name", None)
            entrypoint_template = getattr(attr, "_entrypoint_topic_template", None)
            returnpoint_template = getattr(attr, "_returnpoint_topic_template", None)
            if publish_to_topic_name:
                cls._handler_registry[attr] = {"publish_topic": publish_to_topic_name}
            if subscribe_to_topic_name:
                cls._handler_registry[attr] = cls._handler_registry.get(attr, {})
                cls._handler_registry[attr]["shared_subscribe_topic"] = subscribe_to_topic_name
                cls._handler_registry[attr]["subscribe_topics"] = [subscribe_to_topic_name]
            if entrypoint_template:
                cls._handler_registry[attr] = cls._handler_registry.get(attr, {})
                cls._handler_registry[attr]["entrypoint_topic_template"] = entrypoint_template
            if returnpoint_template:
                cls._handler_registry[attr] = cls._handler_registry.get(attr, {})
                cls._handler_registry[attr]["returnpoint_topic_template"] = returnpoint_template

    def _resolve_private_topics(self) -> None:
        """Resolve entrypoint/returnpoint topic templates for named nodes.

        When a handler is decorated with @entrypoint or @returnpoint, its
        template is resolved using the node's name, and the handler is
        subscribed to both the public and private topics.
        """
        for handler, topics in list(self.bound_registry.items()):
            # Copy to avoid mutating the class-level _handler_registry dicts
            updated: TopicsDict = {**topics}
            subscribe_topics = updated.get("subscribe_topics", [])
            for key in ("entrypoint_topic_template", "returnpoint_topic_template"):
                template = updated.get(key)
                if isinstance(template, str) and self.name:
                    resolved = template.format(name=self.name)
                    subscribe_topics = [*subscribe_topics, resolved]
                    updated["subscribe_topics"] = subscribe_topics
                    updated[key] = resolved
            self.bound_registry[handler] = updated

    def _apply_topic_overrides(
        self,
        input_topic: str | list[str] | None,
        output_topic: str | None,
    ) -> None:
        input_topics: list[str] | None = None
        if isinstance(input_topic, str):
            input_topics = [input_topic]
        elif isinstance(input_topic, list):
            input_topics = input_topic

        for handler, topics in list(self.bound_registry.items()):
            if (input_topics is not None and "shared_subscribe_topic" in topics) or (
                output_topic is not None and "publish_topic" in topics
            ):
                # Copy to avoid mutating class-level _handler_registry dicts
                # (bound_registry shares references for unnamed nodes)
                updated: TopicsDict = {**topics}
                if input_topics is not None and "shared_subscribe_topic" in updated:
                    old_shared = updated["shared_subscribe_topic"]
                    updated["shared_subscribe_topic"] = input_topics[0]
                    # Replace old shared topic with all new input topics
                    updated["subscribe_topics"] = [
                        t for t in updated.get("subscribe_topics", []) if t != old_shared
                    ] + input_topics
                if output_topic is not None and "publish_topic" in updated:
                    updated["publish_topic"] = output_topic
                self.bound_registry[handler] = updated

        if input_topics is not None:
            self.__dict__["subscribed_topic"] = input_topics[0]
        if output_topic is not None:
            self.__dict__["publish_to_topic"] = output_topic

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
    def entrypoint_topic(self) -> str | None:
        if self.name is None:
            return None
        for topics_dict in self.bound_registry.values():
            if "entrypoint_topic_template" in topics_dict:
                return topics_dict["entrypoint_topic_template"]
        return None

    @cached_property
    def returnpoint_topic(self) -> str | None:
        if self.name is None:
            return None
        for topics_dict in self.bound_registry.values():
            if "returnpoint_topic_template" in topics_dict:
                return topics_dict["returnpoint_topic_template"]
        return None

    @cached_property
    def input_message_schema(self) -> type[BaseModel]:
        raise NotImplementedError("input_message_schema is not implemented")

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
