import os
from collections.abc import Iterable
from typing import Any, Literal

from faststream import FastStream
from faststream.kafka import KafkaBroker

from calf.broker.deployable import Deployable
from calf.broker.middleware import ContextInjectionMiddleware


class Broker(KafkaBroker, Deployable):
    """Lightweight client wrapper connecting to Calf brokers"""

    mode = Literal["kafka_mode"]

    def __init__(self, bootstrap_servers: str | Iterable[str] | None = None, **broker_kwargs: Any):
        if not bootstrap_servers:
            bootstrap_servers = os.getenv("CALF_HOST_URL")
        super().__init__(
            bootstrap_servers or "localhost",
            middlewares=[ContextInjectionMiddleware],
            **broker_kwargs,
        )

    @property
    def app(self) -> FastStream:
        return FastStream(self)

    async def run_app(self) -> None:
        await self.app.run()

    def __getattr__(self, name: str) -> Any:
        return getattr(self, name)
