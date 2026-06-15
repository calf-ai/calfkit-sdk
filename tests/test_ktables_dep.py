"""PR-1: the ktables dependency must expose the ``barrier()`` RYOW primitive.

The upcoming in-node durable fan-out fold reads batch state with read-your-own-writes
freshness via ``KafkaTable.barrier()``, which ships in ktables ``>=0.2.0``. This is the
dependency-contract gate for the bump (and for retiring the ``ktables.*`` mypy override,
since 0.2.0 ships ``py.typed``).
"""

import inspect

from ktables.kafka_table import KafkaTable


def test_kafka_table_exposes_async_barrier() -> None:
    assert hasattr(KafkaTable, "barrier"), "ktables>=0.2.0 must expose KafkaTable.barrier()"
    assert inspect.iscoroutinefunction(KafkaTable.barrier), "KafkaTable.barrier() must be an async method"
