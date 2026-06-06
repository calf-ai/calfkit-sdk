"""Tests for the shared CLI node loader (``calfkit.cli._loader``).

The loader resolves ``module:attr`` specs into a flat list of node objects and
is shared by ``calfkit topics provision`` and ``calfkit run``. These tests
exercise it directly (no CliRunner) against the small in-repo
``tests.provisioning_cli_nodes`` fixture module.
"""

from __future__ import annotations

import sys

import pytest
import typer

_FIXTURE = "tests.provisioning_cli_nodes"


def test_resolve_specs_single_node() -> None:
    """An attr that is one node resolves to a one-element list."""
    from calfkit.cli._loader import resolve_specs

    objs = resolve_specs([f"{_FIXTURE}:single"])
    assert len(objs) == 1
    assert objs[0].node_id == "tool_echo"


def test_resolve_specs_expands_iterable_attr() -> None:
    """An attr that is a list of nodes is expanded into the result."""
    from calfkit.cli._loader import resolve_specs

    objs = resolve_specs([f"{_FIXTURE}:nodes"])
    assert [o.node_id for o in objs] == ["tool_alpha", "tool_beta"]


def test_resolve_specs_multiple_specs_concatenate_in_order() -> None:
    """Multiple specs resolve and concatenate in the order supplied."""
    from calfkit.cli._loader import resolve_specs

    objs = resolve_specs([f"{_FIXTURE}:single", f"{_FIXTURE}:nodes"])
    assert [o.node_id for o in objs] == ["tool_echo", "tool_alpha", "tool_beta"]


def test_resolve_specs_preserves_directly_resolved_mcp_server() -> None:
    """A directly-resolved McpServer is returned as-is, not splatted into its
    tool defs (McpServer is iterable, so a naive expand would break it)."""
    from calfkit.cli._loader import resolve_specs
    from calfkit.mcp._server import McpServer

    objs = resolve_specs([f"{_FIXTURE}:solo_mcp"])
    assert len(objs) == 1
    assert isinstance(objs[0], McpServer)


def test_resolve_specs_bad_spec_exits_2() -> None:
    """A spec without a ':' raises typer.Exit(2) mentioning the required form."""
    from calfkit.cli._loader import resolve_specs

    with pytest.raises(typer.Exit) as exc:
        resolve_specs(["no_colon_here"])
    assert exc.value.exit_code == 2


def test_resolve_specs_missing_module_exits_2() -> None:
    """An unimportable module raises typer.Exit(2)."""
    from calfkit.cli._loader import resolve_specs

    with pytest.raises(typer.Exit) as exc:
        resolve_specs(["tests.does_not_exist_xyz:thing"])
    assert exc.value.exit_code == 2


def test_resolve_specs_missing_attr_exits_2() -> None:
    """A module that lacks the requested attr raises typer.Exit(2)."""
    from calfkit.cli._loader import resolve_specs

    with pytest.raises(typer.Exit) as exc:
        resolve_specs([f"{_FIXTURE}:nonexistent_attr"])
    assert exc.value.exit_code == 2


def test_resolve_specs_app_dir_enables_nested_dotted_import(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    """``app_dir`` puts a directory on sys.path so a nested dotted module that
    is otherwise unimportable resolves — incl. PEP-420 namespace packages
    (no __init__.py), matching ``calfkit run subdir.agent_file:agent``."""
    from calfkit.cli._loader import resolve_specs

    subdir = tmp_path / "subpkg"  # type: ignore[operator]
    subdir.mkdir()
    (subdir / "agent_file.py").write_text("from calfkit.nodes import agent_tool\n\n@agent_tool\ndef my_tool(x: int) -> int:\n    return x\n")

    # Isolate sys.path mutation (monkeypatch restores the original list object).
    monkeypatch.setattr(sys, "path", list(sys.path))
    before = set(sys.modules)
    try:
        objs = resolve_specs(["subpkg.agent_file:my_tool"], app_dir=str(tmp_path))
    finally:
        for name in set(sys.modules) - before:
            sys.modules.pop(name, None)

    assert len(objs) == 1
    assert objs[0].node_id == "tool_my_tool"


def test_validate_nodes_accepts_nodes_and_mcp_servers() -> None:
    """A mix of real nodes and McpServer instances passes validation."""
    from calfkit.cli._loader import resolve_specs, validate_nodes

    objs = resolve_specs([f"{_FIXTURE}:mixed"])
    validate_nodes(objs)  # must not raise


def test_validate_nodes_rejects_non_node_exits_2() -> None:
    """An object that is neither a node nor an McpServer is rejected (exit 2)."""
    from calfkit.cli._loader import resolve_specs, validate_nodes

    objs = resolve_specs([f"{_FIXTURE}:not_a_node"])
    with pytest.raises(typer.Exit) as exc:
        validate_nodes(objs)
    assert exc.value.exit_code == 2


def test_dedupe_by_node_id_drops_repeats_preserving_order() -> None:
    """Duplicate node_ids collapse to the first occurrence, order preserved."""
    from calfkit.cli._loader import dedupe_by_node_id, resolve_specs

    objs = resolve_specs([f"{_FIXTURE}:single", f"{_FIXTURE}:single", f"{_FIXTURE}:nodes"])
    deduped = dedupe_by_node_id(objs)
    assert [o.node_id for o in deduped] == ["tool_echo", "tool_alpha", "tool_beta"]


def test_dedupe_keeps_distinct_mcp_servers() -> None:
    """McpServer instances have no node_id, so dedupe keys them by identity and
    always retains them (it must not collapse all node_id-less objects to one)."""
    from calfkit.cli._loader import dedupe_by_node_id, resolve_specs
    from calfkit.mcp._server import McpServer

    # solo_mcp + the McpServer inside `mixed` are two distinct instances.
    objs = resolve_specs([f"{_FIXTURE}:solo_mcp", f"{_FIXTURE}:mixed"])
    deduped = dedupe_by_node_id(objs)
    mcp_servers = [o for o in deduped if isinstance(o, McpServer)]
    assert len(mcp_servers) == 2


def test_resolve_specs_later_bad_spec_exits_2() -> None:
    """A malformed spec is caught even when it follows a valid one."""
    from calfkit.cli._loader import resolve_specs

    with pytest.raises(typer.Exit) as exc:
        resolve_specs([f"{_FIXTURE}:single", "no_colon_here"])
    assert exc.value.exit_code == 2


def test_resolve_specs_source_label_appears_in_error(capsys: pytest.CaptureFixture[str]) -> None:
    """source_label names the spec source in the malformed-spec message so
    `topics provision` can say "--nodes" while `run` says "target"."""
    from calfkit.cli._loader import resolve_specs

    with pytest.raises(typer.Exit):
        resolve_specs(["no_colon"], source_label="--nodes")
    err = capsys.readouterr().err
    assert "--nodes must be in 'module:attr' form" in err


def test_load_nodes_resolves_validates_and_dedupes() -> None:
    """load_nodes returns the deduped node list for valid targets."""
    from calfkit.cli._loader import load_nodes

    nodes = load_nodes([f"{_FIXTURE}:single", f"{_FIXTURE}:single", f"{_FIXTURE}:nodes"])
    assert [n.node_id for n in nodes] == ["tool_echo", "tool_alpha", "tool_beta"]


def test_load_nodes_empty_resolution_exits_2() -> None:
    """load_nodes raises exit 2 when targets resolve to zero nodes."""
    from calfkit.cli._loader import load_nodes

    with pytest.raises(typer.Exit) as exc:
        load_nodes([f"{_FIXTURE}:empty_list"])
    assert exc.value.exit_code == 2
