"""PR-2: ``Client.connect()`` hardens the shared producer.

aiokafka defaults the producer to ``acks=1`` with no idempotence — under which a
broker-acked publish can vanish on leader failover and retries can duplicate. The
fault rail's escalation hops and the in-node fan-out re-entry self-publish both need
durable, non-duplicating publishes, so calfkit defaults the shared producer to
``acks=all`` + ``enable_idempotence=True`` (fault-rail spec §13). A user may still
override via ``Client.connect(**broker_kwargs)`` (document-don't-police).
"""

from calfkit.client import Client
from calfkit.client._broker import _PreStartHookBroker


def _spy_broker_kwargs(monkeypatch) -> dict:  # noqa: ANN001
    """Capture the kwargs ``Client.connect`` passes to ``_PreStartHookBroker``."""
    captured: dict = {}
    orig_init = _PreStartHookBroker.__init__

    def spy_init(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        captured.update(kwargs)
        orig_init(self, *args, **kwargs)

    monkeypatch.setattr(_PreStartHookBroker, "__init__", spy_init)
    return captured


def test_connect_defaults_to_acks_all_and_idempotence(monkeypatch) -> None:  # noqa: ANN001
    captured = _spy_broker_kwargs(monkeypatch)

    Client.connect(server_urls="localhost:9092")

    assert captured.get("acks") == "all"
    assert captured.get("enable_idempotence") is True


def test_connect_lets_user_override_producer_posture(monkeypatch) -> None:  # noqa: ANN001
    captured = _spy_broker_kwargs(monkeypatch)

    Client.connect(server_urls="localhost:9092", enable_idempotence=False)

    assert captured.get("enable_idempotence") is False
