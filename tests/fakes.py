"""Shared test doubles for the broker ``publish`` surface.

One ``CaptureBroker`` replaces the per-file publish-recording fakes that had drifted apart
(``_CaptureBroker`` / ``_FakeBroker`` / ``_StubConn`` / ``_SpyBroker`` and the raising
variants ``_RaisingBroker`` / ``_NonKafkaRaisingBroker`` / ``_FailThenCaptureBroker`` /
``_AlwaysFailBroker``). Those recorded publishes in **conflicting** tuple shapes
(``(topic, headers, msg)`` vs ``(topic, msg, headers)`` vs ``(topic, msg)`` vs a dict), so
this records each call as a :class:`PublishCall` with **named fields** — access
``.message`` / ``.topic`` / ``.key`` / ``.headers`` / ``.correlation_id``, never by
position. Failure injection folds into the constructor.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class PublishCall:
    """One recorded ``broker.publish(...)`` call, accessed by name.

    ``message`` is the first positional arg — an ``Envelope`` on node/client paths, a
    ``StepMessage`` on the step-ledger path.
    """

    message: Any
    topic: str | None
    correlation_id: str | None
    key: bytes | None
    headers: dict[str, str]


class CaptureBroker:
    """Duck-typed stand-in for the broker's (and client connection's) ``publish``.

    Records every publish as a :class:`PublishCall`. Optionally injects failures:

    - ``CaptureBroker(raises=exc)`` — every publish raises ``exc`` (the always-failing fakes).
    - ``CaptureBroker(raises=exc, fail_on={1})`` — raises on those 1-based call numbers and
      records the rest (the fail-first-then-capture fakes).

    ``_connection = True`` so the client's ``_publish_call`` path treats the swapped-in
    connection as already connected (the old ``_StubConn`` role).
    """

    _connection = True  # truthy -> client skips start()

    def __init__(self, *, raises: BaseException | None = None, fail_on: set[int] | None = None) -> None:
        self.published: list[PublishCall] = []
        self._raises = raises
        self._fail_on = fail_on
        self._calls = 0

    @property
    def call_count(self) -> int:
        """Total publish ATTEMPTS, including ones that raised (``published`` holds only the
        successful calls). Lets a fail-injecting test assert the failed attempt happened."""
        return self._calls

    @property
    def keys(self) -> list[bytes | None]:
        """The recorded partition keys, in publish order (some suites assert on these alone)."""
        return [call.key for call in self.published]

    @property
    def topics(self) -> list[str | None]:
        """The recorded topics, in publish order."""
        return [call.topic for call in self.published]

    async def publish(
        self,
        message: Any,
        *,
        topic: str | None = None,
        correlation_id: str | None = None,
        key: bytes | None = None,
        headers: dict[str, str] | None = None,
        **_extra: Any,
    ) -> None:
        self._calls += 1
        if self._raises is not None and (self._fail_on is None or self._calls in self._fail_on):
            raise self._raises
        self.published.append(PublishCall(message, topic, correlation_id, key, headers or {}))
