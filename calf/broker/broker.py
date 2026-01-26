import os
from collections.abc import Iterable

from faststream.kafka import KafkaBroker

from calf.broker.middleware import ContextInjectionMiddleware


class Broker(KafkaBroker):
    """Lightweight client wrapper connecting to Calf brokers"""

    def __init__(self, bootstrap_servers: str | Iterable[str] | None = None, **broker_kwargs):
        if not bootstrap_servers:
            bootstrap_servers = os.getenv("CALF_HOST_URL")
        super().__init__(
            bootstrap_servers or "localhost",
            middlewares=[ContextInjectionMiddleware],
            **broker_kwargs,
        )

    def __getattr__(self, name):
        return getattr(super(), name)
