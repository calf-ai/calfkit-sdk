"""Unit tests for ``calfkit.provisioning``.

These tests exercise the REAL provisioning logic (topic-set derivation, error
classification, retry, security-kwarg merge) with a fake admin client injected
via the ``_make_admin_client`` seam — no live Kafka broker required.
"""

import asyncio

import pytest

from calfkit.nodes.agent import StatelessAgent
from calfkit.nodes.tool import ToolNodeDef
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.provisioning import (
    ProvisioningConfig,
    ProvisionReport,
    TopicProvisioner,
    TopicProvisioningError,
    framework_topics_for_nodes,
    topics_for_nodes,
)
from calfkit.provisioning import provisioner as provisioner_mod


class _FakeModel(PydanticModelClient):
    """Minimal pydantic-ai ``Model`` so a real ``StatelessAgent`` node can be built
    without any network / API key. Never actually invoked by these tests."""

    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:  # noqa: D401
        raise NotImplementedError


def _tool_node(name: str, sub: str, pub: str) -> ToolNodeDef:
    def _fn(x: int) -> int:
        return x

    _fn.__name__ = name
    return ToolNodeDef.create_tool_node(func=_fn, subscribe_topics=sub, publish_topic=pub)


def test_config_defaults() -> None:
    cfg = ProvisioningConfig(enabled=True)
    assert cfg.enabled is True
    assert cfg.num_partitions == 1
    assert cfg.replication_factor == 1
    assert cfg.topic_configs == {}
    assert cfg.create_timeout_ms == 30000


def test_config_defaults_to_disabled() -> None:
    # ``ProvisioningConfig()`` is valid and means provisioning is OFF.
    cfg = ProvisioningConfig()
    assert cfg.enabled is False


def test_config_is_frozen() -> None:
    import dataclasses

    cfg = ProvisioningConfig(enabled=True)
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.enabled = False  # type: ignore[misc]


def test_config_valid_config_constructs() -> None:
    cfg = ProvisioningConfig(
        enabled=True,
        num_partitions=3,
        replication_factor=2,
        create_timeout_ms=10000,
    )
    assert cfg.num_partitions == 3
    assert cfg.replication_factor == 2
    assert cfg.create_timeout_ms == 10000


def test_config_invalid_num_partitions_raises() -> None:
    with pytest.raises(ValueError, match="num_partitions"):
        ProvisioningConfig(enabled=True, num_partitions=0)


def test_config_invalid_replication_factor_raises() -> None:
    with pytest.raises(ValueError, match="replication_factor"):
        ProvisioningConfig(enabled=True, replication_factor=0)


def test_config_invalid_create_timeout_ms_raises() -> None:
    with pytest.raises(ValueError, match="create_timeout_ms"):
        ProvisioningConfig(enabled=True, create_timeout_ms=0)


def test_topics_for_nodes_plain_node_includes_subscribe_publish_and_return() -> None:
    node = _tool_node("echo", sub="echo.in", pub="echo.out")

    topics = topics_for_nodes([node])

    assert "echo.in" in topics
    assert "echo.out" in topics
    assert node._return_topic in topics


def test_topics_for_nodes_includes_agent_tool_subscribe_topics() -> None:
    tool = _tool_node("weather_lookup", sub="weather_lookup.in", pub="weather_lookup.out")
    agent = StatelessAgent(
        "weather",
        subscribe_topics="weather.in",
        publish_topic="weather.out",
        tools=[tool],
        model_client=_FakeModel(),
    )

    topics = topics_for_nodes([agent])

    assert "weather.in" in topics
    assert "weather.out" in topics
    assert agent._return_topic in topics
    # The agent's tool input topic must be provisioned too — the agent
    # publishes Calls onto tools_registry[...].subscribe_topics[0].
    assert "weather_lookup.in" in topics


def test_topics_for_nodes_dedupes_order_preserving() -> None:
    a = _tool_node("a", sub="shared.in", pub="a.out")
    b = _tool_node("b", sub="shared.in", pub="b.out")

    topics = topics_for_nodes([a, b])

    assert topics.count("shared.in") == 1
    # Order-preserving: first-seen wins. "shared.in" precedes a.out which
    # precedes b.out.
    assert topics.index("shared.in") < topics.index("a.out") < topics.index("b.out")


def test_topics_for_nodes_includes_private_input_topic() -> None:
    # ADR-0017: every node's framework-owned inbound inbox is provisioned too.
    node = _tool_node("echo", sub="echo.in", pub="echo.out")

    topics = topics_for_nodes([node])

    assert node._private_input_topic in topics
    assert node._private_input_topic == "tool.echo.private.input"


def test_framework_topics_for_nodes_returns_return_and_input_inboxes() -> None:
    # The single authority for framework-owned topics (never user topic_configs):
    # each node's private return inbox + private input inbox (C1 fix; ADR-0017).
    node = _tool_node("echo", sub="echo.in", pub="echo.out")

    assert framework_topics_for_nodes([node]) == {node._return_topic, node._private_input_topic}
    assert framework_topics_for_nodes([node]) == {"echo.private.return", "tool.echo.private.input"}


def test_framework_topics_for_nodes_unions_across_nodes() -> None:
    a = _tool_node("a", sub="a.in", pub="a.out")
    b = _tool_node("b", sub="b.in", pub="b.out")

    assert framework_topics_for_nodes([a, b]) == {
        "a.private.return",
        "tool.a.private.input",
        "b.private.return",
        "tool.b.private.input",
    }


# ---------------------------------------------------------------------------
# Fake admin client (the _make_admin_client seam)
# ---------------------------------------------------------------------------


class _FakeAdmin:
    """Stand-in for ``AIOKafkaAdminClient``.

    ``error_plan`` maps an attempt index (0-based) to a list of per-topic
    ``(topic, code)`` (or ``(topic, code, message)``) tuples that
    ``create_topics`` should return for that attempt. The last entry is reused
    for any further attempts. This lets a single fake drive the real
    classification + retry logic with fabricated broker responses.
    """

    def __init__(
        self,
        *,
        error_plan: list[list[tuple]],
        kwargs: dict,
        tuple_arity: int = 2,
        start_error: Exception | None = None,
        create_error: Exception | None = None,
    ) -> None:
        self._error_plan = error_plan
        self.kwargs = kwargs
        self._tuple_arity = tuple_arity
        self._start_error = start_error
        self._create_error = create_error
        self.start_calls = 0
        self.close_calls = 0
        self.create_calls: list[list] = []  # each = list of NewTopic passed

    async def start(self) -> None:
        self.start_calls += 1
        if self._start_error is not None:
            raise self._start_error

    async def create_topics(self, new_topics, *args, **kwargs):  # noqa: ANN001
        attempt = len(self.create_calls)
        self.create_calls.append(list(new_topics))
        if self._create_error is not None:
            raise self._create_error
        plan_idx = min(attempt, len(self._error_plan) - 1)
        rows = self._error_plan[plan_idx]
        normalized = []
        for row in rows:
            if self._tuple_arity == 3:
                topic, code = row[0], row[1]
                msg = row[2] if len(row) > 2 else None
                normalized.append((topic, code, msg))
            else:
                normalized.append((row[0], row[1]))
        return _FakeResponse(normalized)

    async def close(self) -> None:
        self.close_calls += 1


class _FakeResponse:
    def __init__(self, topic_errors: list[tuple]) -> None:
        self.topic_errors = topic_errors


def _install_fake_admin(monkeypatch, fake_factory) -> list[_FakeAdmin]:
    """Patch the module seam; return a list that captures created fakes."""
    created: list[_FakeAdmin] = []

    def _factory(**kwargs):
        admin = fake_factory(kwargs)
        created.append(admin)
        return admin

    monkeypatch.setattr(provisioner_mod, "_make_admin_client", _factory)
    return created


def _provisioner(topic_configs: dict | None = None, create_timeout_ms: int = 30000) -> TopicProvisioner:
    cfg = ProvisioningConfig(
        enabled=True,
        num_partitions=3,
        replication_factor=2,
        topic_configs=topic_configs or {},
        create_timeout_ms=create_timeout_ms,
    )
    return TopicProvisioner(bootstrap_servers="localhost:9092", config=cfg)


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("arity", [2, 3])
def test_classification_created_existing_unauthorized(monkeypatch, arity) -> None:
    plan = [[("new.topic", 0), ("dup.topic", 36), ("denied.topic", 29)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw, tuple_arity=arity),
    )

    report = asyncio.run(_provisioner().provision(["new.topic", "dup.topic", "denied.topic"], framework_topics=set()))

    assert isinstance(report, ProvisionReport)
    assert "new.topic" in report.created
    assert "dup.topic" in report.existing
    assert "denied.topic" in report.unauthorized
    # admin must be closed exactly once even on the happy path.
    assert created[0].close_calls == 1


@pytest.mark.parametrize("arity", [2, 3])
@pytest.mark.parametrize("bad_code", [37, 38, 999])
def test_classification_non_retriable_raises(monkeypatch, arity, bad_code) -> None:
    plan = [[("bad.topic", bad_code)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw, tuple_arity=arity),
    )

    with pytest.raises(TopicProvisioningError) as excinfo:
        asyncio.run(_provisioner().provision(["bad.topic"], framework_topics=set()))

    assert excinfo.value.topic == "bad.topic"
    assert excinfo.value.code == bad_code
    # admin still closed despite the raise.
    assert created[0].close_calls == 1


# ---------------------------------------------------------------------------
# Retry / timeout
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("retriable_code", [5, 7, 14, 41])
def test_retriable_code_retries_then_succeeds(monkeypatch, retriable_code) -> None:
    monkeypatch.setattr(provisioner_mod, "_RETRY_BACKOFF_S", 0)
    # attempt 0: retriable; attempt 1: created.
    plan = [[("t.topic", retriable_code)], [("t.topic", 0)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw),
    )

    report = asyncio.run(_provisioner().provision(["t.topic"], framework_topics=set()))

    assert report.created == ["t.topic"]
    # Two create attempts: the retriable one, then the successful re-issue.
    assert len(created[0].create_calls) == 2
    assert created[0].close_calls == 1


def test_timeout_raises_naming_pending(monkeypatch) -> None:
    monkeypatch.setattr(provisioner_mod, "_RETRY_BACKOFF_S", 0)
    # Always retriable -> never resolves -> the wait_for budget must fire.
    plan = [[("stuck.topic", 5)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw),
    )

    with pytest.raises(TopicProvisioningError) as excinfo:
        asyncio.run(_provisioner(create_timeout_ms=50).provision(["stuck.topic"], framework_topics=set()))

    assert excinfo.value.code is None
    assert "stuck.topic" in str(excinfo.value)
    # admin closed even on the timeout path.
    assert created[0].close_calls == 1


def test_unaccounted_topic_in_response_raises(monkeypatch) -> None:
    # The broker response omits a topic that was in the request — we must NOT
    # silently treat it as done. Only "present.topic" comes back.
    plan = [[("present.topic", 0)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw),
    )

    with pytest.raises(TopicProvisioningError) as excinfo:
        asyncio.run(
            _provisioner().provision(
                ["present.topic", "missing.topic"],
                framework_topics=set(),
            )
        )

    assert excinfo.value.code is None
    assert "missing.topic" in str(excinfo.value)
    # admin still closed despite the raise.
    assert created[0].close_calls == 1


# ---------------------------------------------------------------------------
# Multi-topic mixed-batch retry ([7])
# ---------------------------------------------------------------------------


def test_multi_topic_mixed_batch_retries_only_unresolved(monkeypatch) -> None:
    monkeypatch.setattr(provisioner_mod, "_RETRY_BACKOFF_S", 0)
    # attempt 0: "a" created (0), "b" retriable (5); attempt 1: "b" created.
    plan = [[("a", 0), ("b", 5)], [("b", 0)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw),
    )

    report = asyncio.run(_provisioner().provision(["a", "b"], framework_topics=set()))

    assert report.created == ["a", "b"]
    # Two create calls; the second re-issued ONLY the unresolved "b".
    assert len(created[0].create_calls) == 2
    second_names = [nt.name for nt in created[0].create_calls[1]]
    assert second_names == ["b"]


# ---------------------------------------------------------------------------
# Security kwarg merge
# ---------------------------------------------------------------------------


def test_security_object_parsed_and_passed_to_admin(monkeypatch) -> None:
    from faststream.security import SASLPlaintext

    plan = [[("s.topic", 0)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw),
    )

    cfg = ProvisioningConfig(enabled=True)
    prov = TopicProvisioner(
        bootstrap_servers="localhost:9092",
        config=cfg,
        security=SASLPlaintext(username="u", password="p"),
    )
    asyncio.run(prov.provision(["s.topic"], framework_topics=set()))

    kw = created[0].kwargs
    assert kw["security_protocol"] == "SASL_PLAINTEXT"
    assert kw["sasl_plain_username"] == "u"
    assert kw["sasl_plain_password"] == "p"
    assert kw["bootstrap_servers"] == "localhost:9092"


def test_security_merges_raw_kwargs_not_emitted_by_parse(monkeypatch) -> None:
    from faststream.security import SASLPlaintext

    plan = [[("s.topic", 0)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw),
    )

    cfg = ProvisioningConfig(enabled=True)
    # sasl_kerberos_service_name is NOT emitted by parse_security for
    # SASLPlaintext; the raw kwarg must be merged through.
    prov = TopicProvisioner(
        bootstrap_servers="localhost:9092",
        config=cfg,
        security=SASLPlaintext(username="u", password="p"),
        sasl_kerberos_service_name="custom",
    )
    asyncio.run(prov.provision(["s.topic"], framework_topics=set()))

    kw = created[0].kwargs
    assert kw["sasl_kerberos_service_name"] == "custom"
    # The security= object still wins for its own keys.
    assert kw["sasl_plain_username"] == "u"


def test_security_protocol_conflict_raises() -> None:
    from faststream.security import SASLPlaintext

    cfg = ProvisioningConfig(enabled=True)
    with pytest.raises(ValueError, match="security_protocol"):
        TopicProvisioner(
            bootstrap_servers="localhost:9092",
            config=cfg,
            security=SASLPlaintext(username="u", password="p"),  # -> SASL_PLAINTEXT
            security_protocol="SSL",  # conflicts
        )


def test_security_protocol_agreement_does_not_raise() -> None:
    from faststream.security import SASLPlaintext

    cfg = ProvisioningConfig(enabled=True)
    # Same protocol from both sources is not a conflict.
    prov = TopicProvisioner(
        bootstrap_servers="localhost:9092",
        config=cfg,
        security=SASLPlaintext(username="u", password="p"),
        security_protocol="SASL_PLAINTEXT",
    )
    assert prov is not None


# ---------------------------------------------------------------------------
# NewTopic construction invariants
# ---------------------------------------------------------------------------


def test_new_topic_uses_concrete_positive_partitions_and_rf(monkeypatch) -> None:
    plan = [[("d.topic", 0)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw),
    )

    # _provisioner() uses num_partitions=3, replication_factor=2.
    asyncio.run(_provisioner().provision(["d.topic"], framework_topics=set()))

    new_topics = created[0].create_calls[0]
    nt = new_topics[0]
    assert nt.num_partitions == 3
    assert nt.replication_factor == 2
    # Never the broker-default sentinel that the aiokafka XOR validator rejects.
    assert nt.num_partitions != -1
    assert nt.replication_factor != -1


def test_admin_request_timeout_matches_create_timeout(monkeypatch) -> None:
    plan = [[("d.topic", 0)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw),
    )

    # _provisioner() uses create_timeout_ms=30000 by default below.
    asyncio.run(_provisioner(create_timeout_ms=12345).provision(["d.topic"], framework_topics=set()))

    assert created[0].kwargs["request_timeout_ms"] == 12345


def test_topic_configs_applied_to_data_topics_not_framework_topics(monkeypatch) -> None:
    plan = [[("data.topic", 0), ("reply.private.return", 0)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw),
    )

    prov = _provisioner(topic_configs={"retention.ms": "604800000"})
    asyncio.run(
        prov.provision(
            ["data.topic", "reply.private.return"],
            framework_topics={"reply.private.return"},
        )
    )

    by_name = {nt.name: nt for nt in created[0].create_calls[0]}
    assert by_name["data.topic"].topic_configs == {"retention.ms": "604800000"}
    # Framework topic must NOT carry the data topic_configs (None -> {} in
    # aiokafka). cleanup.policy/retention on task-keyed reply inboxes is
    # semantically wrong.
    assert by_name["reply.private.return"].topic_configs == {}


# ---------------------------------------------------------------------------
# from_connection / _normalize_bootstrap ([3c])
# ---------------------------------------------------------------------------


def test_normalize_bootstrap_shapes() -> None:
    from calfkit.provisioning.provisioner import _normalize_bootstrap

    assert _normalize_bootstrap(None) == "localhost"
    assert _normalize_bootstrap("broker:9092") == "broker:9092"
    assert _normalize_bootstrap(["a:9092", "b:9092"]) == ["a:9092", "b:9092"]
    # Any (non-str) iterable is materialized to a list.
    assert _normalize_bootstrap(("a:9092", "b:9092")) == ["a:9092", "b:9092"]


def test_from_connection_normalizes_and_splits_security(monkeypatch) -> None:
    from faststream.security import SASLPlaintext

    plan = [[("fc.topic", 0)]]
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=plan, kwargs=kw),
    )

    cfg = ProvisioningConfig(enabled=True)
    prov = TopicProvisioner.from_connection(
        server_urls=None,  # -> "localhost"
        config=cfg,
        security_kwargs={
            "security": SASLPlaintext(username="u", password="p"),
            "sasl_kerberos_service_name": "custom",
        },
    )
    asyncio.run(prov.provision(["fc.topic"], framework_topics=set()))

    kw = created[0].kwargs
    assert kw["bootstrap_servers"] == "localhost"
    assert kw["security_protocol"] == "SASL_PLAINTEXT"
    assert kw["sasl_plain_username"] == "u"
    assert kw["sasl_kerberos_service_name"] == "custom"


def test_from_connection_does_not_mutate_security_kwargs() -> None:
    sk = {"security": None, "security_protocol": "PLAINTEXT"}
    TopicProvisioner.from_connection(
        server_urls="localhost:9092",
        config=ProvisioningConfig(enabled=True),
        security_kwargs=sk,
    )
    # The caller's dict must be untouched (popped from a copy).
    assert sk == {"security": None, "security_protocol": "PLAINTEXT"}


# ---------------------------------------------------------------------------
# Resource cleanup / no I/O on construction
# ---------------------------------------------------------------------------


def test_admin_closed_in_finally_when_create_raises(monkeypatch) -> None:
    boom = RuntimeError("create blew up")
    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _FakeAdmin(error_plan=[[]], kwargs=kw, create_error=boom),
    )

    with pytest.raises(RuntimeError, match="create blew up"):
        asyncio.run(_provisioner().provision(["x.topic"], framework_topics=set()))

    assert created[0].close_calls == 1


def test_close_error_does_not_mask_original_failure(monkeypatch) -> None:
    boom = RuntimeError("create blew up")

    class _CloseRaises(_FakeAdmin):
        async def close(self) -> None:
            self.close_calls += 1
            raise RuntimeError("close also blew up")

    created = _install_fake_admin(
        monkeypatch,
        lambda kw: _CloseRaises(error_plan=[[]], kwargs=kw, create_error=boom),
    )

    # The original create failure must surface, not the close failure.
    with pytest.raises(RuntimeError, match="create blew up"):
        asyncio.run(_provisioner().provision(["x.topic"], framework_topics=set()))

    assert created[0].close_calls == 1


def test_construction_does_no_network(monkeypatch) -> None:
    # If construction tried to build/connect an admin client, this seam would
    # be hit. Make it explode so any construction-time use is caught.
    def _explode(**kwargs):
        raise AssertionError("admin client must not be built at construction time")

    monkeypatch.setattr(provisioner_mod, "_make_admin_client", _explode)

    cfg = ProvisioningConfig(enabled=True)
    # Must not raise — no admin client is created until provision() runs.
    TopicProvisioner(bootstrap_servers="localhost:9092", config=cfg)


def test_provision_empty_topics_is_a_noop_no_admin_built(monkeypatch) -> None:
    # An empty request returns an empty report WITHOUT constructing an admin
    # client (the create/connect work is skipped entirely).
    created = _install_fake_admin(monkeypatch, lambda kw: _FakeAdmin(error_plan=[[]], kwargs=kw))

    report = asyncio.run(_provisioner().provision([], framework_topics=set()))

    assert report == ProvisionReport()
    assert created == []
