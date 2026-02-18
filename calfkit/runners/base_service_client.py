from typing import Any

import uuid_utils
from pydantic import BaseModel

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.base_node import BaseNode


class BaseServiceClient:
    def __init__(self, broker: BrokerClient):
        self._broker = broker

    async def message_manual(
        self,
        topic_names: list[str],
        message_payload: BaseModel,
        *,
        correlation_id: str | None = None,
    ):
        if not self._broker._connection:
            await self._broker.start()
        correlation_id = uuid_utils.uuid7().hex if correlation_id is None else correlation_id
        for topic in topic_names:
            if not topic:
                continue
            await self._broker.publish(
                message_payload,
                topic=topic,
                correlation_id=correlation_id,
            )

    async def message_to_node(
        self, node: BaseNode, payload_kwargs: dict[str, Any], *, correlation_id: str | None = None
    ):
        if node.subscribed_topic is None:
            raise RuntimeError(f"Node's subscribed topic can't be None: {node}")
        schema = node.input_message_schema
        input_msg = schema.model_validate(payload_kwargs)
        topics = [node.subscribed_topic]
        await self.message_manual(topics, input_msg, correlation_id=correlation_id)
