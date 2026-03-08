import json
from collections.abc import Callable
from typing import Any

from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.base_node import BaseNode


def _adapt_filter(filter_fn: Callable[..., bool]) -> Callable[..., bool]:
    """Adapt a user's EventEnvelope-level filter to FastStream's raw message filter.

    FastStream filters receive a ``StreamMessage`` with ``.body`` as raw bytes,
    not the deserialized application object.  This adapter deserializes the body
    and calls the user's filter with a proper ``EventEnvelope``.
    """

    async def raw_filter(msg: Any) -> bool:
        try:
            body = json.loads(msg.body)
            envelope = EventEnvelope.model_validate(body)
            return filter_fn(envelope)
        except Exception:
            return False

    return raw_filter


async def _noop_handler(msg: EventEnvelope) -> EventEnvelope:
    """Default handler for subscribers with filters.

    Silently discards messages that don't match any filter,
    preventing FastStream's ``HandlerNotFoundError``.
    """
    return EventEnvelope()


class NodesService:
    def __init__(self, broker: BrokerClient):
        self._broker = broker
        self._subscribers: list[Any] = []

    def register_node(
        self,
        node: BaseNode,
        *,
        max_workers: int = 1,
        # group_id explicitly set as to avoid duplicated processing for separate deployments
        group_id: str | None = None,  # Don't touch unless you know what you're doing
        extra_publish_kwargs: dict[str, Any] = {},
        extra_subscribe_kwargs: dict[str, Any] = {},
    ) -> None:
        if group_id is None and node.name is not None:
            group_id = node.name
        for handler_fn, topics_dict in node.bound_registry.items():
            pub: str | None = topics_dict.get("publish_topic")
            if pub is not None:
                handler_fn = self._broker.publisher(pub, **extra_publish_kwargs)(handler_fn)
            subscribe_topics: list[str] = topics_dict.get("subscribe_topics", [])
            filter_fn = topics_dict.get("filter")
            for sub_topic in subscribe_topics:
                subscriber = self._broker.subscriber(
                    sub_topic,
                    max_workers=max_workers,
                    group_id=group_id,
                    **extra_subscribe_kwargs,
                )
                if filter_fn is not None:
                    handler_fn = subscriber(handler_fn, filter=_adapt_filter(filter_fn))
                    subscriber(_noop_handler)
                else:
                    handler_fn = subscriber(handler_fn)
                self._subscribers.append(subscriber)

    async def start_subscribers(self) -> None:
        """Start all registered subscribers.

        Use this to start consumers that were registered after the broker
        has already been started (e.g. dynamically spawned nodes).
        """
        for sub in self._subscribers:
            await sub.start()

    async def run(self) -> None:
        """Blocking function to run registered nodes as services."""
        await self._broker.run_app()
