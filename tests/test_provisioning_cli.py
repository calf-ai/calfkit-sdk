"""Tests for the ``calfkit topics provision`` CLI subcommand and the
top-level ``ProvisioningConfig`` export.

Uses typer's ``CliRunner`` + a small in-repo nodes module so the topic-set
resolution is exercised end to end. The admin-client seam
(``calfkit.provisioning.provisioner._make_admin_client``) is patched so no
live Kafka broker is required; ``--dry-run`` must never construct it.
"""

from __future__ import annotations

import re


def test_provisioning_config_exported_at_top_level() -> None:
    """``ProvisioningConfig`` is importable directly off the package root."""
    import calfkit

    assert calfkit.ProvisioningConfig is not None
    cfg = calfkit.ProvisioningConfig(enabled=True)
    assert cfg.enabled is True


_ANSI_SGR = re.compile(r"\x1b\[[0-9;]*m")


def _plain(s: str) -> str:
    """Strip ANSI SGR escape codes so substring assertions survive
    typer/rich styling in CI."""
    return _ANSI_SGR.sub("", s)


def test_provision_help_loads() -> None:
    """``topics provision --help`` loads and documents the core flags."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app

    result = CliRunner().invoke(app, ["provision", "--help"])
    assert result.exit_code == 0, result.stdout
    stdout = _plain(result.stdout)
    assert "--nodes" in stdout
    assert "--bootstrap-servers" in stdout
    assert "--partitions" in stdout
    assert "--replication-factor" in stdout
    assert "--timeout-ms" in stdout
    assert "--dry-run" in stdout


def test_dry_run_resolves_topics_without_admin_client(monkeypatch) -> None:  # noqa: ANN001
    """--dry-run prints the resolved topic set and never builds an admin client."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app
    from calfkit.provisioning import provisioner as provisioner_mod

    def _explode(**kwargs):  # noqa: ANN003, ANN202
        raise AssertionError("--dry-run must NOT construct an admin client")

    monkeypatch.setattr(provisioner_mod, "_make_admin_client", _explode)

    result = CliRunner().invoke(
        app,
        ["provision", "--nodes", "tests.provisioning_cli_nodes:nodes", "--dry-run"],
    )
    assert result.exit_code == 0, result.stdout
    out = _plain(result.stdout)
    # The two tool nodes contribute their subscribe + publish topics.
    assert "alpha.in" in out
    assert "alpha.out" in out
    assert "beta.in" in out
    assert "beta.out" in out
    # Framework return inboxes are part of the resolved set too.
    assert ".private.return" in out
    assert "Dry run: no topics created." in out


def test_dry_run_resolves_single_node(monkeypatch) -> None:  # noqa: ANN001
    """A --nodes attr that is a single node (not a collection) resolves."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app
    from calfkit.provisioning import provisioner as provisioner_mod

    monkeypatch.setattr(
        provisioner_mod,
        "_make_admin_client",
        lambda **kw: (_ for _ in ()).throw(AssertionError("no admin in dry-run")),
    )

    result = CliRunner().invoke(
        app,
        ["provision", "--nodes", "tests.provisioning_cli_nodes:single", "--dry-run"],
    )
    assert result.exit_code == 0, result.stdout
    out = _plain(result.stdout)
    assert "echo.in" in out
    assert "echo.out" in out


# ---------------------------------------------------------------------------
# Live provisioning path (fake admin via the _make_admin_client seam)
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, topic_errors) -> None:  # noqa: ANN001
        self.topic_errors = topic_errors


class _FakeAdmin:
    """Stand-in for AIOKafkaAdminClient that returns fabricated topic_errors."""

    def __init__(self, *, code_for, kwargs) -> None:  # noqa: ANN001
        self._code_for = code_for
        self.kwargs = kwargs
        self.create_calls = 0

    async def start(self) -> None:
        return None

    async def create_topics(self, new_topics, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        self.create_calls += 1
        return _FakeResponse([(nt.name, self._code_for(nt.name)) for nt in new_topics])

    async def close(self) -> None:
        return None


def test_live_provision_prints_report(monkeypatch) -> None:  # noqa: ANN001
    """Without --dry-run, the admin seam runs and the ProvisionReport prints."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app
    from calfkit.provisioning import provisioner as provisioner_mod

    # Every topic reports code 0 (created).
    monkeypatch.setattr(
        provisioner_mod,
        "_make_admin_client",
        lambda **kw: _FakeAdmin(code_for=lambda name: 0, kwargs=kw),
    )

    result = CliRunner().invoke(
        app,
        [
            "provision",
            "--nodes",
            "tests.provisioning_cli_nodes:nodes",
            "--bootstrap-servers",
            "localhost:9092",
        ],
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    out = _plain(result.stdout)
    assert "created" in out
    assert "alpha.in" in out


def test_live_provision_error_exits_2(monkeypatch) -> None:  # noqa: ANN001
    """A non-retriable per-topic error becomes TopicProvisioningError -> exit 2."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app
    from calfkit.provisioning import provisioner as provisioner_mod

    # Code 38 (INVALID_REPLICATION_FACTOR) is non-retriable -> raises.
    monkeypatch.setattr(
        provisioner_mod,
        "_make_admin_client",
        lambda **kw: _FakeAdmin(code_for=lambda name: 38, kwargs=kw),
    )

    result = CliRunner().invoke(
        app,
        ["provision", "--nodes", "tests.provisioning_cli_nodes:nodes"],
    )
    assert result.exit_code == 2
    assert "provisioning failed" in _plain(result.stderr)


# ---------------------------------------------------------------------------
# --nodes spec validation
# ---------------------------------------------------------------------------


def test_bad_nodes_spec_exits_2() -> None:
    """A --nodes value without 'module:attr' form errors with exit 2."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app

    result = CliRunner().invoke(app, ["provision", "--nodes", "no_colon_here", "--dry-run"])
    assert result.exit_code == 2
    assert "module:attr" in _plain(result.stderr)


def test_topics_mounted_on_top_level_cli() -> None:
    """The top-level ``calfkit`` app mounts the ``topics`` sub-app."""
    from typer.testing import CliRunner

    from calfkit.cli import _build_app

    result = CliRunner().invoke(_build_app(), ["topics", "--help"])
    assert result.exit_code == 0, result.stdout
    assert "provision" in _plain(result.stdout)


def test_unimportable_module_exits_2() -> None:
    """A --nodes module that can't be imported errors with exit 2."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app

    result = CliRunner().invoke(
        app,
        ["provision", "--nodes", "tests.does_not_exist_xyz:thing", "--dry-run"],
    )
    assert result.exit_code == 2
    assert "cannot import module" in _plain(result.stderr)


def test_module_import_side_effect_failure_exits_2_with_distinct_message() -> None:
    """A module that raises during import (not ImportError) is reported as a
    side-effect failure, distinct from a missing module, and still exits 2."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app

    result = CliRunner().invoke(
        app,
        ["provision", "--nodes", "tests.provisioning_cli_import_boom:thing", "--dry-run"],
    )
    assert result.exit_code == 2
    err = _plain(result.stderr)
    assert "raised during import" in err
    assert "side-effect boom" in err
    # Not misreported as a missing module.
    assert "cannot import module" not in err


def test_non_node_resolved_object_exits_2_no_traceback() -> None:
    """A --nodes attr resolving to a non-node object is rejected with an
    actionable error (exit 2), not an AttributeError traceback (exit 1)."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app

    result = CliRunner().invoke(
        app,
        ["provision", "--nodes", "tests.provisioning_cli_nodes:not_a_node", "--dry-run"],
    )
    assert result.exit_code == 2
    err = _plain(result.stderr)
    assert "subscribe_topics" in err or "not a node" in err.lower()


def test_empty_bootstrap_servers_exits_2() -> None:
    """An empty --bootstrap-servers (after split/strip) errors with exit 2
    rather than an IndexError traceback."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app

    result = CliRunner().invoke(
        app,
        [
            "provision",
            "--nodes",
            "tests.provisioning_cli_nodes:nodes",
            "--bootstrap-servers",
            "  ,  ",
        ],
    )
    assert result.exit_code == 2
    assert "bootstrap" in _plain(result.stderr).lower()


def test_partitions_and_replication_factor_flow_to_new_topic(monkeypatch) -> None:  # noqa: ANN001
    """--partitions / --replication-factor reach the NewTopic the admin gets."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app
    from calfkit.provisioning import provisioner as provisioner_mod

    seen: list = []

    class _CapturingAdmin(_FakeAdmin):
        async def create_topics(self, new_topics, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            seen.extend(new_topics)
            return await super().create_topics(new_topics, *args, **kwargs)

    monkeypatch.setattr(
        provisioner_mod,
        "_make_admin_client",
        lambda **kw: _CapturingAdmin(code_for=lambda name: 0, kwargs=kw),
    )

    result = CliRunner().invoke(
        app,
        [
            "provision",
            "--nodes",
            "tests.provisioning_cli_nodes:nodes",
            "--partitions",
            "3",
            "--replication-factor",
            "2",
        ],
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    assert seen, "admin.create_topics was never called"
    assert all(nt.num_partitions == 3 for nt in seen)
    assert all(nt.replication_factor == 2 for nt in seen)


def test_unauthorized_report_branch_prints_line(monkeypatch) -> None:  # noqa: ANN001
    """An unauthorized (code 29) topic prints the unauthorized summary line."""
    from typer.testing import CliRunner

    from calfkit.cli.topics import app
    from calfkit.provisioning import provisioner as provisioner_mod

    # "alpha.in" is denied (29); everything else created (0). 29 is a
    # warn-and-continue outcome, so provisioning still returns a report.
    monkeypatch.setattr(
        provisioner_mod,
        "_make_admin_client",
        lambda **kw: _FakeAdmin(code_for=lambda name: 29 if name == "alpha.in" else 0, kwargs=kw),
    )

    result = CliRunner().invoke(
        app,
        ["provision", "--nodes", "tests.provisioning_cli_nodes:nodes"],
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    out = _plain(result.stdout)
    assert "unauthorized" in out
    assert "alpha.in" in out
