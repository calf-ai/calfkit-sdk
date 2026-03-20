from asyncio import Future, wait_for
from uuid import uuid4

from faststream.kafka import KafkaBroker, KafkaMessage
from faststream.types import SendableMessage

# via faststream docs: https://faststream.ag2.ai/0.6/howto/kafka/rpc/#reusable-class


class RPCWorker:
    responses: dict[str, Future[bytes]]

    def __init__(self, broker: KafkaBroker, reply_topic: str) -> None:
        self.responses = {}
        self.broker = broker
        self.reply_topic = reply_topic

        self.subscriber = broker.subscriber(reply_topic)
        self.subscriber(self._handle_responses)

    async def start(self) -> None:
        await self.subscriber.start()

    async def stop(self) -> None:
        await self.subscriber.stop()

    def _handle_responses(self, msg: KafkaMessage) -> None:
        if future := self.responses.pop(msg.correlation_id, None):
            future.set_result(msg.body)

    async def request(
        self,
        data: SendableMessage,
        topic: str,
        timeout: float = 10.0,
    ) -> bytes:
        correlation_id = str(uuid4())
        future = self.responses[correlation_id] = Future[bytes]()

        await self.broker.publish(
            data,
            topic,
            reply_to=self.reply_topic,
            correlation_id=correlation_id,
        )

        try:
            response: bytes = await wait_for(future, timeout=timeout)
        except TimeoutError:
            self.responses.pop(correlation_id, None)
            raise
        else:
            return response
