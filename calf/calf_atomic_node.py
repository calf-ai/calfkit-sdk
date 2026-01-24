from abc import ABC
from dataclasses import dataclass, field
import functools
import inspect
import itertools
from typing import Any, Callable, Optional

from faststream import FastStream
from calf.runtime import CalfRuntime


# Sentinel attribute name for handler metadata
_HANDLER_METADATA_ATTR = "__calf_handler_metadata__"


@dataclass
class HandlerMetadata:
    """Stores metadata for @on and @post_to decorators."""
    subscribers: list[dict[str, Any]] = field(default_factory=list)
    publishers: list[dict[str, Any]] = field(default_factory=list)


def on(*topics: str, pattern: Optional[str] = None, **subscriber_kwargs) -> Callable[[Callable], Callable]:
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
        metadata.subscribers.append({
            'topics': topics,
            'pattern': pattern,
            'kwargs': subscriber_kwargs,
        })
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
        metadata.publishers.append({
            'topic': topic,
            'kwargs': publisher_kwargs,
        })
        return func
    return decorator


class CalfAtomicNode(ABC):
    """Base class for atomic Calf nodes with message handling capabilities.

    Subclasses can use @on and @post_to decorators on instance methods
    to subscribe to topics and publish messages.
    """

    _counter = itertools.count()
    _handler_methods: dict[str, Callable]

    def __init_subclass__(cls, **kwargs) -> None:
        """Collect all decorated handler methods when subclass is defined."""
        super().__init_subclass__(**kwargs)

        cls._handler_methods = {}
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if hasattr(method, _HANDLER_METADATA_ATTR):
                cls._handler_methods[name] = method

    def __init__(self, name: Optional[str] = None) -> None:
        if not CalfRuntime.initialized:
            raise RuntimeError("Calf runtime not initialized. Run `initialize()`")

        self.name = name if name else f"calf-node-{next(self._counter)}"
        self.runtime = CalfRuntime

        # Register all decorated handlers with FastStream
        self._register_handlers()

    def _register_handlers(self) -> None:
        """Register all decorated methods with FastStream using bound methods."""
        for method_name, unbound_method in self._handler_methods.items():
            bound_method = getattr(self, method_name)
            metadata: HandlerMetadata = getattr(unbound_method, _HANDLER_METADATA_ATTR)
            self._register_single_handler(bound_method, metadata)

    def _register_single_handler(
        self,
        bound_method: Callable,
        metadata: HandlerMetadata
    ) -> None:
        """Register a single handler method with FastStream."""
        broker = self.runtime.calf

        # Create wrapper function that FastStream will see (without self in signature)
        handler: Callable = self._create_handler_wrapper(bound_method)

        # Apply decorators in reverse order to maintain correct semantics
        # Publishers first (outer), then subscribers (inner)
        for pub_info in reversed(metadata.publishers):
            handler = broker.publisher(
                pub_info['topic'],
                **pub_info['kwargs']
            )(handler)

        for sub_info in reversed(metadata.subscribers):
            sub_kwargs = sub_info['kwargs'].copy()
            if sub_info['pattern']:
                sub_kwargs['pattern'] = sub_info['pattern']

            handler = broker.subscriber(
                *sub_info['topics'],
                **sub_kwargs
            )(handler)

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

    @property
    def calf(self):
        """Get the broker instance."""
        return self.runtime.calf
