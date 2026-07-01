"""``Client.connect()`` producer posture: calfkit imposes **no** ``acks`` / ``enable_idempotence``
value by default, but exposes a unified opt-in override.

The libraries supply the defaults (ktables writers and aiokafka both default to no idempotence;
aiokafka defaults ``acks=1``). Passing ``enable_idempotence=True`` (or ``False``) via
``Client.connect`` — or ``ck run --enable-idempotence`` — turns it on (off) consistently across the
shared broker producer *and* the co-located worker's control-plane / fan-out writers. The resolved
tri-state (``None`` = unset, ``True``, ``False``) is stored on the client so those writers wire to
the same posture. ``acks`` stays overridable via ``broker_kwargs`` (document-don't-police).
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


def test_connect_sets_no_producer_posture_by_default(monkeypatch) -> None:  # noqa: ANN001
    captured = _spy_broker_kwargs(monkeypatch)

    Client.connect(server_urls="localhost:9092")

    # calfkit imposes nothing — the libraries' defaults apply (ktables/aiokafka: no idempotence).
    assert "acks" not in captured
    assert "enable_idempotence" not in captured


def test_connect_opt_in_threads_idempotence_true(monkeypatch) -> None:  # noqa: ANN001
    captured = _spy_broker_kwargs(monkeypatch)

    Client.connect(server_urls="localhost:9092", enable_idempotence=True)

    assert captured.get("enable_idempotence") is True


def test_connect_explicit_false_threads_idempotence_false(monkeypatch) -> None:  # noqa: ANN001
    captured = _spy_broker_kwargs(monkeypatch)

    Client.connect(server_urls="localhost:9092", enable_idempotence=False)

    assert captured.get("enable_idempotence") is False


def test_connect_stores_resolved_idempotence_for_colocated_workers() -> None:
    # A co-located worker/nodes read the resolved tri-state off the client
    # (self._client._enable_idempotence) to wire their writers to the same posture — one knob.
    assert Client.connect(server_urls="localhost:9092")._enable_idempotence is None
    assert Client.connect(server_urls="localhost:9092", enable_idempotence=True)._enable_idempotence is True
    assert Client.connect(server_urls="localhost:9092", enable_idempotence=False)._enable_idempotence is False


def test_connect_acks_override_via_broker_kwargs(monkeypatch) -> None:  # noqa: ANN001
    captured = _spy_broker_kwargs(monkeypatch)

    Client.connect(server_urls="localhost:9092", acks="all")

    assert captured.get("acks") == "all"
    # acks alone does not turn idempotence on.
    assert "enable_idempotence" not in captured
