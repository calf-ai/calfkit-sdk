import functools
import inspect
import itertools
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from boltons.typeutils import classproperty
from faststream import FastStream

from calf.broker.broker import Broker
from calf.contracts.topics import Topics
from calf.runtime import CalfRuntime

# Sentinel attribute name for handler metadata
_HANDLER_METADATA_ATTR = "__calf_handler_metadata__"

# Sentinels for default topic resolution
_USE_DEFAULT_ON = object()
_USE_DEFAULT_POST_TO = object()


@dataclass
class HandlerMetadata:
    """Stores metadata for @on and @post_to decorators."""

    subscribers: list[dict[str, Any]] = field(default_factory=list)
    publishers: list[dict[str, Any]] = field(default_factory=list)


def on(
    *topics: str, pattern: str | None = None, **subscriber_kwargs
) -> Callable[[Callable], Callable]:
    """Decorator to mark a method as a message subscriber.

    Registration with FastStream is deferred until instance creation,
    allowing proper handling of instance methods with 'self'.

    Args:
        topics: Topics to subscribe to.
        pattern: Optional pattern for topic matching.
        **subscriber_kwargs: Passed to FastStream's subscriber().

    Example:
        @on("my-topic")
        def handle_message(self, msg: str):
            print(f"Received: {msg}")
    """

    def decorator(func: Callable) -> Callable:
        if not hasattr(func, _HANDLER_METADATA_ATTR):
            setattr(func, _HANDLER_METADATA_ATTR, HandlerMetadata())

        metadata: HandlerMetadata = getattr(func, _HANDLER_METADATA_ATTR)
        metadata.subscribers.append(
            {
                "topics": topics,
                "pattern": pattern,
                "kwargs": subscriber_kwargs,
            }
        )
        return func

    return decorator


def post_to(topic: str, **publisher_kwargs) -> Callable[[Callable], Callable]:
    """Decorator to mark a method as a message publisher.

    Registration with FastStream is deferred until instance creation,
    allowing proper handling of instance methods with 'self'.

    Args:
        topic: Topic to publish to.
        **publisher_kwargs: Passed to FastStream's publisher().

    Example:
        @post_to("output-topic")
        def process(self, msg: str) -> str:
            return f"Processed: {msg}"
    """

    def decorator(func: Callable) -> Callable:
        if not hasattr(func, _HANDLER_METADATA_ATTR):
            setattr(func, _HANDLER_METADATA_ATTR, HandlerMetadata())

        metadata: HandlerMetadata = getattr(func, _HANDLER_METADATA_ATTR)
        metadata.publishers.append(
            {
                "topic": topic,
                "kwargs": publisher_kwargs,
            }
        )
        return func

    return decorator


def on_default(func: Callable) -> Callable:
    """Decorator to subscribe to the class's default topic ({ClassName}.{Topics.INVOKED}).

    Usage:
        @on_default
        async def handler(self, msg):
            ...
    """
    if not hasattr(func, _HANDLER_METADATA_ATTR):
        setattr(func, _HANDLER_METADATA_ATTR, HandlerMetadata())

    metadata: HandlerMetadata = getattr(func, _HANDLER_METADATA_ATTR)
    metadata.subscribers.append(
        {
            "topics": (_USE_DEFAULT_ON,),
            "pattern": None,
            "kwargs": {},
        }
    )
    return func


def post_to_default(func: Callable) -> Callable:
    """Decorator to publish to the class's default topic ({ClassName}.{Topics.NON_USER_RESPONSE}).

    Usage:
        @post_to_default
        async def handler(self, msg) -> str:
            return "response"
    """
    if not hasattr(func, _HANDLER_METADATA_ATTR):
        setattr(func, _HANDLER_METADATA_ATTR, HandlerMetadata())

    metadata: HandlerMetadata = getattr(func, _HANDLER_METADATA_ATTR)
    metadata.publishers.append(
        {
            "topic": _USE_DEFAULT_POST_TO,
            "kwargs": {},
        }
    )
    return func


class BaseAtomicNode(ABC):
    """Base class for atomic Calf nodes with message handling capabilities.

    Subclasses can use @on and @post_to decorators on instance methods
    to subscribe to topics and publish messages.
    """

    _counter = itertools.count()
    _handler_methods: dict[str, Callable]
    runtime = CalfRuntime

    def __init_subclass__(cls, **kwargs) -> None:
        """Collect all decorated handler methods when subclass is defined."""
        super().__init_subclass__(**kwargs)

        cls._handler_methods = {}
        for name, value in cls.__dict__.items():
            if callable(value) and hasattr(value, _HANDLER_METADATA_ATTR):
                cls._handler_methods[name] = value

    def __init__(self, name: str | None = None) -> None:
        if not CalfRuntime.initialized:
            raise RuntimeError("Calf runtime not initialized. Run `initialize()`")

        self.name = name if name else f"calf-node-{next(self._counter)}"

        # Register all decorated handlers with FastStream
        self._register_handlers()

    def _register_handlers(self) -> None:
        """Register all decorated methods with FastStream using bound methods."""
        for method_name, unbound_method in self._handler_methods.items():
            bound_method = getattr(self, method_name)
            metadata: HandlerMetadata = getattr(unbound_method, _HANDLER_METADATA_ATTR)
            self._register_single_handler(bound_method, metadata)

    def _register_single_handler(self, bound_method: Callable, metadata: HandlerMetadata) -> None:
        """Register a single handler method with FastStream."""
        broker = self.runtime.calf
        cls = type(self)

        # Create wrapper function that FastStream will see (without self in signature)
        handler: Callable = self._create_handler_wrapper(bound_method)

        # Apply decorators in reverse order to maintain correct semantics
        # Publishers first (outer), then subscribers (inner)
        for pub_info in reversed(metadata.publishers):
            topic = pub_info["topic"]
            if topic is _USE_DEFAULT_POST_TO:
                topic = cls.get_default_post_to_topic()
            handler = broker.publisher(topic, **pub_info["kwargs"])(handler)

        for sub_info in reversed(metadata.subscribers):
            topics = sub_info["topics"]
            if len(topics) == 1 and topics[0] is _USE_DEFAULT_ON:
                topics = (cls.get_default_on_topic(),)

            sub_kwargs = sub_info["kwargs"].copy()
            if sub_info["pattern"]:
                sub_kwargs["pattern"] = sub_info["pattern"]

            handler = broker.subscriber(*topics, **sub_kwargs)(handler)

    def _create_handler_wrapper(self, bound_method: Callable) -> Callable:
        """Create a wrapper function for a bound method."""
        if inspect.iscoroutinefunction(bound_method):

            @functools.wraps(bound_method)
            async def async_handler(*args, **kwargs):
                return await bound_method(*args, **kwargs)

            return async_handler
        else:

            @functools.wraps(bound_method)
            def sync_handler(*args, **kwargs):
                return bound_method(*args, **kwargs)

            return sync_handler

    async def run_node(self) -> None:
        """Run the node's FastStream application."""
        await self.runnable.run()

    @property
    def runnable(self) -> FastStream:
        """Get the FastStream application instance."""
        return self.runtime.runnable

    @classproperty
    def calf(cls) -> Broker:  # noqa: N805
        """Get the broker instance."""
        return cls.runtime.calf

    @classmethod
    def get_default_on_topic(cls) -> str:
        """Return the default subscribe topic: {ClassName}.{Topics.INVOKED}."""
        return f"{Topics.INVOKE}.{cls.__name__}"

    @classmethod
    def get_default_post_to_topic(cls) -> str:
        """Return the default publish topic: {ClassName}.{Topics.NON_USER_RESPONSE}."""
        return f"{Topics.NON_USER_RESPONSE}.{cls.__name__}"
