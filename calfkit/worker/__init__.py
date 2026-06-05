from typing import TYPE_CHECKING, Any

# The lifecycle context types are safe to import eagerly: ``lifecycle`` is a leaf
# module (it imports only faststream + calfkit.exceptions, never ``nodes`` or
# ``worker.worker``), so it can't re-enter the circular graph that forces the
# lazy ``Worker``/``WorkerConfig`` re-exports below.
from calfkit.exceptions import LifecycleConfigError
from calfkit.worker.lifecycle import LifecycleContext, ResourceSetupContext, ServingContext

if TYPE_CHECKING:
    from calfkit.worker.worker import Worker
    from calfkit.worker.worker_config import WorkerConfig

__all__ = [
    "LifecycleConfigError",
    "LifecycleContext",
    "ResourceSetupContext",
    "ServingContext",
    "Worker",
    "WorkerConfig",
]


def __getattr__(name: str) -> Any:
    # Lazy re-exports (PEP 562): ``Worker``/``WorkerConfig`` import
    # ``calfkit.nodes`` at module load, and ``calfkit.nodes.base`` imports
    # ``calfkit.worker.lifecycle`` for ``LifecycleHookMixin``. Eagerly importing
    # them here would re-enter ``calfkit.nodes`` while it is still initializing
    # (circular import). Deferring the import to first attribute access keeps the
    # public ``from calfkit.worker import Worker`` API intact while letting
    # ``calfkit.worker.lifecycle`` be imported without dragging in the nodes
    # graph.
    if name == "Worker":
        from calfkit.worker.worker import Worker

        return Worker
    if name == "WorkerConfig":
        from calfkit.worker.worker_config import WorkerConfig

        return WorkerConfig
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
