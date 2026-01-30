import os
from collections.abc import Iterable
from typing import Literal

from faststream import FastStream
from faststream.kafka import KafkaBroker

from calf.broker.deployable import Deployable
from calf.broker.middleware import ContextInjectionMiddleware


class Broker(KafkaBroker, Deployable):
    """Lightweight client wrapper connecting to Calf brokers"""

    mode = Literal["kafka_mode"]

    def __init__(self, bootstrap_servers: str | Iterable[str] | None = None, **broker_kwargs):
        if not bootstrap_servers:
            bootstrap_servers = os.getenv("CALF_HOST_URL")
        super().__init__(
            bootstrap_servers or "localhost",
            middlewares=[ContextInjectionMiddleware],
            **broker_kwargs,
        )

    @property
    def app(self):
        return FastStream(self)

    async def run_app(self):
        await self.app.run()

    def __getattr__(self, name):
        return getattr(self, name)
