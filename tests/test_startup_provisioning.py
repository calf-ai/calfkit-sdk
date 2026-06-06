"""Unit tests for startup topic provisioning (issue #180).

Covers the new `MissingTopicsError`, the stateless `provision_topics(admin, ...)`
executor, and `StartupTopicEnsurer` — all exercised with a fake admin client
(no live Kafka broker required).
"""

import asyncio

import pytest
from faststream.exceptions import IncorrectState

from calfkit.exceptions import MissingTopicsError
from calfkit.provisioning import (
    ProvisioningConfig,
    ProvisionReport,
    StartupTopicEnsurer,
    TopicProvisioningError,
    provision_topics,
)
from calfkit.provisioning import provisioner as provisioner_mod

# ---------------------------------------------------------------------------
# Fake admin (already "started" — provision_topics receives a live admin client
# and must NOT own its lifecycle; FastStream's broker owns it).
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, topic_errors: list[tuple]) -> None:
        self.topic_errors = topic_errors


class _Admin:
    """Per-attempt ``(topic, code)`` plan; tracks create/close calls."""

    def __init__(self, error_plan: list[list[tuple]]) -> None:
        self._error_plan = error_plan
        self.create_calls: list[list] = []  # each = list[NewTopic]
        self.close_calls = 0

    async def create_topics(self, new_topics, *args, **kwargs):  # noqa: ANN001
        idx = len(self.create_calls)
        self.create_calls.append(list(new_topics))
        rows = self._error_plan[min(idx, len(self._error_plan) - 1)]
        return _Resp([(t, c) for (t, c) in rows])

    async def close(self) -> None:
        self.close_calls += 1


def _cfg(**kw) -> ProvisioningConfig:  # noqa: ANN003
    return ProvisioningConfig(enabled=True, **kw)


# ---------------------------------------------------------------------------
# MissingTopicsError
# ---------------------------------------------------------------------------


def test_missing_topics_error_carries_topics_and_names_them() -> None:
    err = MissingTopicsError(["calf-client-reply-abc", "some.node.in"])

    assert err.topics == ["calf-client-reply-abc", "some.node.in"]
    msg = str(err)
    assert "calf-client-reply-abc" in msg
    assert "some.node.in" in msg


# ---------------------------------------------------------------------------
# provision_topics(admin, ...) — stateless executor on a live admin client
# ---------------------------------------------------------------------------


def test_provision_topics_classifies_created_existing_unauthorized() -> None:
    admin = _Admin([[("new.t", 0), ("dup.t", 36), ("denied.t", 29)]])

    report = asyncio.run(provision_topics(admin, ["new.t", "dup.t", "denied.t"], framework_topics=set(), config=_cfg()))

    assert isinstance(report, ProvisionReport)
    assert "new.t" in report.created
    assert "dup.t" in report.existing
    assert "denied.t" in report.unauthorized


def test_provision_topics_does_not_close_the_admin() -> None:
    # The admin client is owned by FastStream's broker; the executor must not
    # close it (that would tear down the broker's admin connection).
    admin = _Admin([[("a.t", 0)]])

    asyncio.run(provision_topics(admin, ["a.t"], framework_topics=set(), config=_cfg()))

    assert admin.close_calls == 0


def test_provision_topics_empty_is_a_noop() -> None:
    admin = _Admin([[]])

    report = asyncio.run(provision_topics(admin, [], framework_topics=set(), config=_cfg()))

    assert report == ProvisionReport()
    assert admin.create_calls == []


def test_provision_topics_framework_topic_skips_user_topic_configs() -> None:
    admin = _Admin([[("data.t", 0), ("fw.t", 0)]])

    asyncio.run(
        provision_topics(
            admin,
            ["data.t", "fw.t"],
            framework_topics={"fw.t"},
            config=_cfg(topic_configs={"retention.ms": "123"}),
        )
    )

    by_name = {nt.name: nt for nt in admin.create_calls[0]}
    assert by_name["data.t"].topic_configs == {"retention.ms": "123"}
    assert not by_name["fw.t"].topic_configs  # framework inbox: never gets user configs


def test_provision_topics_uses_config_partitions_and_rf() -> None:
    admin = _Admin([[("a.t", 0)]])

    asyncio.run(provision_topics(admin, ["a.t"], framework_topics=set(), config=_cfg(num_partitions=4, replication_factor=2)))

    nt = admin.create_calls[0][0]
    assert nt.num_partitions == 4
    assert nt.replication_factor == 2


def test_provision_topics_non_retriable_raises() -> None:
    admin = _Admin([[("bad.t", 37)]])

    with pytest.raises(TopicProvisioningError):
        asyncio.run(provision_topics(admin, ["bad.t"], framework_topics=set(), config=_cfg()))


def test_provision_topics_retries_retriable_until_resolved(monkeypatch) -> None:
    monkeypatch.setattr(provisioner_mod, "_RETRY_BACKOFF_S", 0)
    # code 5 = LEADER_NOT_AVAILABLE (retriable) on attempt 1, then created.
    admin = _Admin([[("t", 5)], [("t", 0)]])

    report = asyncio.run(provision_topics(admin, ["t"], framework_topics=set(), config=_cfg()))

    assert "t" in report.created
    assert len(admin.create_calls) == 2


def test_provision_topics_timeout_raises_naming_pending(monkeypatch) -> None:
    monkeypatch.setattr(provisioner_mod, "_RETRY_BACKOFF_S", 0)
    # Always retriable -> never resolves -> the create_timeout budget fires.
    admin = _Admin([[("stuck.t", 5)]])

    with pytest.raises(TopicProvisioningError, match="stuck.t"):
        asyncio.run(provision_topics(admin, ["stuck.t"], framework_topics=set(), config=_cfg(create_timeout_ms=50)))


# ---------------------------------------------------------------------------
# StartupTopicEnsurer — declares topics, provisions (when enabled) at start,
# reusing FastStream's broker-managed admin client.
# ---------------------------------------------------------------------------


class _BrokerConfig:
    """Mimics FastStream's ``KafkaBrokerConfig.admin_client`` property."""

    def __init__(self, admin, raise_incorrect_state: bool = False) -> None:
        self._admin = admin
        self._raise = raise_incorrect_state
        self.admin_access_count = 0

    @property
    def admin_client(self):  # noqa: ANN201
        self.admin_access_count += 1
        if self._raise:
            raise IncorrectState("Admin client is not initialized. Call connect() first.")
        return self._admin


class _ConfigComposition:
    def __init__(self, broker_config: _BrokerConfig) -> None:
        self.broker_config = broker_config


class _Broker:
    """Minimal stand-in exposing ``broker.config.broker_config.admin_client``."""

    def __init__(self, admin=None, raise_incorrect_state: bool = False) -> None:  # noqa: ANN001
        self._bc = _BrokerConfig(admin, raise_incorrect_state)
        self.config = _ConfigComposition(self._bc)

    @property
    def admin_access_count(self) -> int:
        return self._bc.admin_access_count


def test_ensurer_disabled_is_a_noop_admin_never_touched() -> None:
    admin = _Admin([[]])
    broker = _Broker(admin)
    ens = StartupTopicEnsurer(config=ProvisioningConfig(enabled=False))
    ens.declare(["calf-client-reply-x"], framework=True)

    asyncio.run(ens.run(broker))

    assert broker.admin_access_count == 0  # default path: admin client never reached
    assert admin.create_calls == []


def test_ensurer_enabled_provisions_declared_topics() -> None:
    admin = _Admin([[("calf-client-reply-x", 0), ("node.in", 0)]])
    broker = _Broker(admin)
    ens = StartupTopicEnsurer(config=ProvisioningConfig(enabled=True))
    ens.declare(["calf-client-reply-x"], framework=True)
    ens.declare(["node.in"])

    asyncio.run(ens.run(broker))

    names = sorted(nt.name for nt in admin.create_calls[0])
    assert names == ["calf-client-reply-x", "node.in"]


def test_ensurer_enabled_but_topic_uncreated_raises_missing() -> None:
    admin = _Admin([[("calf-client-reply-x", 29)]])  # 29 = authorization denied -> not created
    broker = _Broker(admin)
    ens = StartupTopicEnsurer(config=ProvisioningConfig(enabled=True))
    ens.declare(["calf-client-reply-x"], framework=True)

    with pytest.raises(MissingTopicsError) as excinfo:
        asyncio.run(ens.run(broker))

    assert excinfo.value.topics == ["calf-client-reply-x"]


def test_ensurer_enabled_with_nothing_declared_is_a_noop() -> None:
    admin = _Admin([[]])
    broker = _Broker(admin)
    ens = StartupTopicEnsurer(config=ProvisioningConfig(enabled=True))

    asyncio.run(ens.run(broker))

    assert broker.admin_access_count == 0
    assert admin.create_calls == []


def test_ensurer_skips_when_admin_client_unavailable() -> None:
    broker = _Broker(raise_incorrect_state=True)  # consumer_only / not connected
    ens = StartupTopicEnsurer(config=ProvisioningConfig(enabled=True))
    ens.declare(["calf-client-reply-x"], framework=True)

    # Skips gracefully — no raise.
    asyncio.run(ens.run(broker))


def test_ensurer_declare_marks_framework_topic_to_skip_user_configs() -> None:
    admin = _Admin([[("data.in", 0), ("fw.reply", 0)]])
    broker = _Broker(admin)
    ens = StartupTopicEnsurer(config=ProvisioningConfig(enabled=True, topic_configs={"retention.ms": "1"}))
    ens.declare(["data.in"])
    ens.declare(["fw.reply"], framework=True)

    asyncio.run(ens.run(broker))

    by_name = {nt.name: nt for nt in admin.create_calls[0]}
    assert by_name["data.in"].topic_configs == {"retention.ms": "1"}
    assert not by_name["fw.reply"].topic_configs


# ---------------------------------------------------------------------------
# Client.connect() wiring: the reply topic is declared into a StartupTopicEnsurer
# wired as the broker's pre-start hook (no live Kafka).
# ---------------------------------------------------------------------------


def test_connect_uses_a_pre_start_hook_broker() -> None:
    from calfkit.client import Client
    from calfkit.client._broker import _PreStartHookBroker

    client = Client.connect("localhost:9092", reply_topic="r.reply")

    assert isinstance(client.broker, _PreStartHookBroker)


def test_connect_enabled_declares_reply_topic_into_ensurer() -> None:
    from calfkit.client import Client

    client = Client.connect("localhost:9092", reply_topic="r.reply", provisioning=ProvisioningConfig(enabled=True))

    # Running the wired ensurer (the broker's pre-start hook) against a fake
    # broker provisions exactly the client's reply topic.
    admin = _Admin([[("r.reply", 0)]])
    asyncio.run(client._startup_ensurer.run(_Broker(admin)))

    assert [nt.name for nt in admin.create_calls[0]] == ["r.reply"]


def test_connect_disabled_ensurer_is_a_noop() -> None:
    from calfkit.client import Client

    client = Client.connect("localhost:9092", reply_topic="r.reply")  # provisioning off (default)

    admin = _Admin([[]])
    broker = _Broker(admin)
    asyncio.run(client._startup_ensurer.run(broker))

    assert broker.admin_access_count == 0  # default path: admin client never reached
    assert admin.create_calls == []
