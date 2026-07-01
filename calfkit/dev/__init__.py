"""``ck dev`` broker supervisor — the connect-or-spawn local-mesh daemon.

Manages a local Tansu broker for ``ck dev`` (probe / spawn / ownership / teardown). Modules in this
package import ``psutil`` and the ``calfkit-mesh`` binary locator **lazily** (only when a spawn is
actually warranted), so a core ``pip install calfkit`` — without the ``[mesh]`` extra — never needs
them. See ADR-0031 / ADR-0032 and ``docs/designs/dev-mesh-broker-spec.md``.
"""
