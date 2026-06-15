"""PR-2: ``Client.connect()`` hardens the shared producer.

aiokafka defaults the producer to ``acks=1`` with no idempotence — under which a
broker-acked publish can vanish on leader failover and retries can duplicate or reorder.
calfkit defaults the shared producer to ``acks=all`` + ``enable_idempotence=True`` for
durable, non-duplicating publishes. A user may still override via
``Client.connect(**broker_kwargs)`` (document-don't-police).
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


def test_connect_user_can_relax_acks_with_idempotence_off(monkeypatch) -> None:  # noqa: ANN001
    # Both posture keys are overridable. Relaxing acks below "all" requires turning
    # idempotence off too (aiokafka rejects acks<all with idempotence on).
    captured = _spy_broker_kwargs(monkeypatch)

    Client.connect(server_urls="localhost:9092", acks=1, enable_idempotence=False)

    assert captured.get("acks") == 1
    assert captured.get("enable_idempotence") is False
