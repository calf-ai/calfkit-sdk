"""Shared fake admin/broker doubles for the provisioning unit tests.

The ``EnsurerBroker`` mirrors FastStream's ``broker.config.broker_config.admin_client``
seam (and tracks admin-client access), so both ``test_startup_provisioning`` and
``test_provisioning_worker`` can run a ``StartupTopicEnsurer`` against a fake admin
without redefining the three-layer broker stub. The per-test *admin* fakes (an
error-plan-driven one vs a create-all one) stay local to each file — they encode
different test intents.
"""

from typing import Any


class FakeResponse:
    """Stand-in for an admin ``create_topics`` response."""

    def __init__(self, topic_errors: list[tuple]) -> None:
        self.topic_errors = topic_errors


class _BrokerConfig:
    """Mimics FastStream's ``KafkaBrokerConfig.admin_client`` property."""

    def __init__(self, admin: Any, raise_incorrect_state: bool) -> None:
        self._admin = admin
        self._raise = raise_incorrect_state
        self.admin_access_count = 0

    @property
    def admin_client(self) -> Any:
        self.admin_access_count += 1
        if self._raise:
            from faststream.exceptions import IncorrectState

            raise IncorrectState("Admin client is not initialized. Call connect() first.")
        return self._admin


class _Config:
    def __init__(self, broker_config: _BrokerConfig) -> None:
        self.broker_config = broker_config


class EnsurerBroker:
    """Minimal broker stand-in exposing ``broker.config.broker_config.admin_client``.

    ``admin_access_count`` records how many times the admin client was reached
    (so a test can assert the disabled path never touches it). Pass
    ``raise_incorrect_state=True`` to simulate a ``consumer_only`` / not-connected
    broker whose admin accessor raises ``IncorrectState``.
    """

    def __init__(self, admin: Any = None, *, raise_incorrect_state: bool = False) -> None:
        self._bc = _BrokerConfig(admin, raise_incorrect_state)
        self.config = _Config(self._bc)

    @property
    def admin_access_count(self) -> int:
        return self._bc.admin_access_count
