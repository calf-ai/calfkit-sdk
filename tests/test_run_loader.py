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
    assert objs[0].node_id == "echo"


def test_resolve_specs_expands_iterable_attr() -> None:
    """An attr that is a list of nodes is expanded into the result."""
    from calfkit.cli._loader import resolve_specs

    objs = resolve_specs([f"{_FIXTURE}:nodes"])
    assert [o.node_id for o in objs] == ["alpha", "beta"]


def test_resolve_specs_multiple_specs_concatenate_in_order() -> None:
    """Multiple specs resolve and concatenate in the order supplied."""
    from calfkit.cli._loader import resolve_specs

    objs = resolve_specs([f"{_FIXTURE}:single", f"{_FIXTURE}:nodes"])
    assert [o.node_id for o in objs] == ["echo", "alpha", "beta"]


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
    assert objs[0].node_id == "my_tool"


def test_validate_nodes_rejects_non_node_exits_2() -> None:
    """An object that is not a node is rejected (exit 2)."""
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
    assert [o.node_id for o in deduped] == ["echo", "alpha", "beta"]


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
    assert [n.node_id for n in nodes] == ["echo", "alpha", "beta"]


def test_load_nodes_empty_resolution_exits_2() -> None:
    """load_nodes raises exit 2 when targets resolve to zero nodes."""
    from calfkit.cli._loader import load_nodes

    with pytest.raises(typer.Exit) as exc:
        load_nodes([f"{_FIXTURE}:empty_list"])
    assert exc.value.exit_code == 2
