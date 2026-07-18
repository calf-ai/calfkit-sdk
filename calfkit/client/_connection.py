"""The client's Kafka connection profile (max-message-bytes design §4.2).

``Client.connect`` resolves connection facts once — bootstrap address, security options
(derived from the FastStream ``security=`` object via ``parse_security``), and the
``max_message_bytes`` knob — and freezes them here. Every Kafka client calfkit builds
draws from this one carrier, so the guard (producer ``max_request_size``) and the floor
(consumer fetch caps) can never drift apart across the mesh (a "half-open" config).

The two derivation methods encode the knob's asymmetry: producers get a **guard** (an
oversized publish raises ``MessageSizeTooLargeError`` client-side), consumers get a
**capacity floor** (aiokafka consumers cannot reject on size — the floor just guarantees
receivers can fetch what a compliant producer was allowed to send).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ktables import KafkaConnectionConfig

#: The default client-wide message-size knob (bytes). The guard applies to the serialized
#: record (payload + ~100 B protocol overhead), and it is cooperative — it binds calfkit's
#: own clients, not the broker.
DEFAULT_MAX_MESSAGE_BYTES = 5 * 1024 * 1024

#: aiokafka's own ``fetch_max_bytes`` default. ``consumer_fetch_kwargs`` only ever RAISES
#: the cross-partition fetch cap above this — lowering it would throttle multi-partition
#: fetch throughput for no benefit (it is not a per-record bound).
_AIOKAFKA_FETCH_MAX_BYTES_DEFAULT = 52_428_800  # 50 MiB


@dataclass(frozen=True)
class ConnectionProfile:
    """The client's Kafka connection facts, threaded to every client calfkit builds."""

    bootstrap_servers: str
    """The resolved bootstrap address(es), comma-joined."""

    security_opts: Mapping[str, object]
    """aiokafka security kwargs (``parse_security`` output; may be empty)."""

    max_message_bytes: int
    """The client-wide size knob: producer guard + consumer floor (design §1)."""

    def __post_init__(self) -> None:
        # Defensive copy + read-only view: a caller mutating the dict they passed in must
        # not mutate the profile (frozen= alone is a shallow freeze over a live mapping).
        object.__setattr__(self, "security_opts", MappingProxyType(dict(self.security_opts)))

    def __repr__(self) -> str:
        # security_opts carries credentials (sasl_plain_password); render keys only.
        keys = ", ".join(sorted(self.security_opts))
        return (
            f"ConnectionProfile(bootstrap_servers={self.bootstrap_servers!r}, "
            f"security_opts=<keys: {keys}>, max_message_bytes={self.max_message_bytes})"
        )

    def producer_size_kwargs(self) -> dict[str, Any]:
        """The producer-side **guard**: splat into any producer calfkit configures."""
        return {"max_request_size": self.max_message_bytes}

    def consumer_fetch_kwargs(self) -> dict[str, Any]:
        """The consumer-side **floor**: splat into any consumer/subscriber calfkit configures."""
        return {
            "max_partition_fetch_bytes": self.max_message_bytes,
            "fetch_max_bytes": max(_AIOKAFKA_FETCH_MAX_BYTES_DEFAULT, self.max_message_bytes),
        }

    def ktables_connection(self) -> KafkaConnectionConfig:
        """The profile as a ktables ``KafkaConnectionConfig`` — Leg 4's carrier (design §5).

        Security applies to ALL ktables clients (``common_opts``: readers, writers, and the
        ``ensure_topic`` admin), the guard to its writers, the floor to its readers.
        ``enable_idempotence`` deliberately stays OUTSIDE this mapping: it remains a
        first-class per-writer ktables kwarg (its one home), and ktables 2.0 *reserves* it
        in ``producer_opts`` — the two-knob collision is structurally impossible.
        """
        from ktables import KafkaConnectionConfig  # lazy — ktables stays off the offline import path

        return KafkaConnectionConfig(
            bootstrap_servers=self.bootstrap_servers,
            common_opts=dict(self.security_opts),
            producer_opts=self.producer_size_kwargs(),
            consumer_opts=self.consumer_fetch_kwargs(),
        )
