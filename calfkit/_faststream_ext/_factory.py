"""Constructor wiring for ``KeyOrderedSubscriber``.

This mirrors the ~25 lines of ``faststream.kafka.subscriber.factory.create_subscriber``
that build the config/specification/calls triple — duplicated of necessity, not choice:
the upstream factory hardcodes its class selection in an ``if`` chain with no injection
seam, so there is no way to ask it to construct a different subscriber class. The canary
suite pins the mirrored argument set against the upstream factory's signature so drift
fails in CI.

The config MUST be a real ``KafkaSubscriberConfig``: its ``__post_init__`` derives
``enable_auto_commit`` from the resolved ack policy — the ACK_FIRST wiring this
subscriber's whole no-offset-tracking design rests on.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError
from faststream.kafka.subscriber.config import (
    KafkaSubscriberConfig,
    KafkaSubscriberSpecificationConfig,
)
from faststream.kafka.subscriber.factory import _validate_input_for_misconfigure
from faststream.kafka.subscriber.specification import KafkaSubscriberSpecification

from ._subscriber import KeyOrderedSubscriber

if TYPE_CHECKING:
    from faststream.kafka.configs import KafkaBrokerConfig


def create_key_ordered_subscriber(
    *topics: str,
    group_id: str | None,
    max_workers: int,
    connection_args: dict[str, Any],
    config: KafkaBrokerConfig,
) -> KeyOrderedSubscriber:
    """Build (not register) a ``KeyOrderedSubscriber``.

    Deliberately narrow relative to the stock builder: no ``batch``/``pattern``/
    ``partitions``/``listener`` modes, no per-subscriber ack policy (ACK_FIRST is a
    design requirement, resolved from the broker config and enforced here), stock
    defaults for the spec-level params (``no_reply=False``, ``include_in_schema=True``).
    The registration mixin rejects anything outside this surface by name.
    """
    if max_workers < 1:
        msg = f"key-ordered dispatch needs max_workers >= 1, got {max_workers}"
        raise SetupError(msg)

    # The upstream validator's concurrent-manual-commit branch never fires for us (we
    # resolve to ACK_FIRST or refuse to construct), but its topic/pattern/partition
    # cross-checks are shared invariants — run them exactly as the stock factory does.
    _validate_input_for_misconfigure(
        *topics,
        ack_policy=EMPTY,
        max_workers=max_workers,
        pattern=None,
        partitions=(),
    )

    subscriber_config = KafkaSubscriberConfig(
        topics=topics,
        partitions=(),
        connection_args=connection_args,
        group_id=group_id,
        listener=None,
        pattern=None,
        no_reply=False,
        _outer_config=config,
        _ack_policy=EMPTY,  # resolve broker-level policy, defaulting to ACK_FIRST
    )
    if not subscriber_config.ack_first:
        msg = (
            "key-ordered dispatch requires AckPolicy.ACK_FIRST (commit-on-receipt): "
            f"the broker resolves to {subscriber_config.ack_policy!r}. Other policies "
            "would need per-record offset tracking to commit safely under out-of-order "
            "completion, which this subscriber deliberately does not implement."
        )
        raise SetupError(msg)

    calls = CallsCollection[Any]()
    specification = KafkaSubscriberSpecification(
        _outer_config=config,
        calls=calls,
        specification_config=KafkaSubscriberSpecificationConfig(
            topics=topics,
            partitions=(),
            pattern=None,
            title_=None,
            description_=None,
            include_in_schema=True,
        ),
    )
    return KeyOrderedSubscriber(subscriber_config, specification, calls, max_workers=max_workers)
