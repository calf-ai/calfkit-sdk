"""Key-ordered concurrent dispatch — a standalone FastStream extension.

This package adds what FastStream 0.7.x lacks: a Kafka subscriber that runs up to
``max_workers`` handler coroutines in parallel while processing messages that share a
partition key **serially, in partition order** ("parallel across keys, serial within a
key"). FastStream's own ``ConcurrentDefaultSubscriber`` dispatches with no key affinity,
so per-key ordering is lost the moment ``max_workers > 1``; its ordering-preserving
alternative (``ConcurrentBetweenPartitionsSubscriber``) is non-ACK_FIRST-only and silently
truncates multi-topic subscriptions to a single topic.

Independence contract
=====================
This package is a **pure FastStream extension**. It imports only ``faststream``, ``anyio``,
and the stdlib — never ``calfkit`` — and encodes no calfkit concepts. Errors are
FastStream's own ``SetupError``; logging rides the subscriber's FastStream logger state.
The dependency direction is strictly one-way (calfkit consumes this package), enforced by
an independence canary in ``tests/unit/faststream_ext/test_upstream_seams.py``. The package
is extraction-ready: it can be lifted into its own distribution or an upstream PR with only
a directory move.

Internals-coupling contract
===========================
FastStream has no injection seam for custom subscriber classes, so this package subclasses
``faststream._internal`` subscriber machinery. Every ``faststream._internal`` import in the
codebase lives HERE and nowhere else, the dependency is pinned ``>=0.7.1,<0.8`` in
``pyproject.toml``, and every touched seam is asserted by the canary tests so an upstream
change trips CI at lock-upgrade time rather than production at runtime.

Design
======
Spec: ``docs/designs/key-ordered-dispatch-spec.md`` (local working doc, not shipped).
Dispatch: ``crc32(raw_key) % max_workers`` lanes, one bounded stream + one serial worker
task per lane; a single ``anyio.Semaphore(2 * max_workers)`` is the only blocking
primitive (wait-never-drop backpressure); graceful stop drains every accepted message.

Tombstone
=========
This package exists to be deleted. Upstream feature request: (link TBD — drafted with this
package, filed separately). When FastStream ships an equivalent (e.g. ``ordering="key"``),
delete this package and flip call sites to the native option.
"""

from ._broker import KeyOrderedRegistratorMixin
from ._subscriber import KeyOrderedSubscriber

__all__ = ["KeyOrderedRegistratorMixin", "KeyOrderedSubscriber"]
