"""CG-1 packaging + import-hygiene guards for the ``ck dev`` / ``[mesh]`` work.

Protects two contracts from the dev-mesh-broker plan:

- the opt-in ``[mesh]`` extra replaces the dead empty ``dev`` extra (the ``calfkit-mesh`` binary dep is
  added once the upstream package publishes — ADR-0031); and
- a *core* ``pip install calfkit`` (no ``[mesh]``) can still run every ``ck`` command:
  ``calfkit.cli._build_app()`` mounts every sub-app on each invocation, so nothing it imports may pull
  in ``psutil`` or ``calfkit_mesh`` (both belong to ``[mesh]``). The ``ck dev`` bodies + the broker
  binary locator import those lazily.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_PYPROJECT = Path(__file__).parents[1] / "pyproject.toml"


def test_mesh_extra_replaces_dead_dev_extra() -> None:
    text = _PYPROJECT.read_text()
    assert "dev = []" not in text, "the dead empty `dev` extra must be removed"
    assert 'mesh = ["calfkit-mesh>=0.1", "psutil>=5.9"]' in text


def test_python_dash_m_calfkit_cli_runs_the_ck_app() -> None:
    """The ``-d`` daemon spawn re-invokes the CLI as ``python -m calfkit.cli run ...``
    (dev-agent-lifecycle plan §3.1): the module must be executable without the ``ck`` console
    script being on PATH."""
    result = subprocess.run([sys.executable, "-m", "calfkit.cli", "--help"], capture_output=True, text=True)
    assert result.returncode == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "dev" in result.stdout


def test_ck_app_builds_without_mesh_deps() -> None:
    """Building the ``ck`` app must not import ``psutil``/``calfkit_mesh`` (the ``[mesh]`` deps).

    Runs in a fresh interpreter with both modules forced unimportable, so the guard holds regardless
    of whether the current env happens to have them installed.
    """
    script = (
        "import sys\n"
        "sys.modules['psutil'] = None\n"  # make `import psutil` raise ModuleNotFoundError
        "sys.modules['calfkit_mesh'] = None\n"
        "from calfkit.cli import _build_app\n"
        "_build_app()\n"  # mounts topics/run/chat (+ dev, once it exists)
        "import calfkit.cli._chat\n"  # the chat session path imports _dev_agents — psutil must stay scan-lazy there too
        "import calfkit.cli._dev_agents\n"
        "print('HYGIENE_OK')\n"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "HYGIENE_OK" in result.stdout
