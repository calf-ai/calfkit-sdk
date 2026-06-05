from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from calfkit.worker.worker import Worker
    from calfkit.worker.worker_config import WorkerConfig

__all__ = [
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
