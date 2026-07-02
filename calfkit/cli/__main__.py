"""``python -m calfkit.cli`` — the module entry point.

The ``ck dev run -d`` daemon spawn re-invokes the CLI as
``[sys.executable, "-m", "calfkit.cli", "run", ...]`` (dev-agent-lifecycle plan §3.1): a child
process must not depend on the ``ck`` console script being on PATH — ``sys.executable`` plus this
module is resolvable from whatever environment the parent runs in.
"""

from calfkit.cli import main

main()
