from abc import ABC
from typing import Annotated, Any

from faststream import Context
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)

from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.base_node import BaseNode, publish_to, subscribe_to


class RoundRobinGroupchatNode(BaseNode, ABC):
    """Node defining the llm chat node internal wiring.
    Separate from any logic for LLM persona or behaviour."""

    _on_enter_topic_name = "groupchat_prompted"
    _post_to_topic_name = "groupchat_generated"

    def __init__(
        self,
        agent_nodes: list[AgentRouterNode],
        *,
        shared_system_prompt_addition: str | None = None,
        **kwargs: Any,
    ):
        self._agent_node_topics = [
            node.private_subscribed_topic
            for node in agent_nodes
            if node.private_subscribed_topic is not None
        ]
        self._shared_system_prompt_addition = shared_system_prompt_addition
        super().__init__(**kwargs)

    @subscribe_to(_on_enter_topic_name)
    @publish_to(_post_to_topic_name)
    async def _route_groupchat(
        self,
        event_envelope: EventEnvelope,
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ) -> EventEnvelope:
        if event_envelope.groupchat_data is None:
            raise RuntimeError("No groupchat data/config provided in groupchat node")

        event_envelope.groupchat_data.ensure_defaults(
            self._agent_node_topics, self._shared_system_prompt_addition
        )

        event_envelope.groupchat_data.commit_turn()
        # Snapshot taken after the completed turn is committed but before
        # forward-looking routing mutations (turn index, skip counter, context swap).
        # This is the semantically correct observation state: the turn index
        # still identifies the agent that just finished, and uncommitted_messages
        # still contain that agent's response.
        observer_snapshot = event_envelope.model_copy(deep=True)

        event_envelope.replace_uncommitted_with_turn_context()
        all_skipped = event_envelope.groupchat_data.advance_to_next_turn()

        if all_skipped:
            event_envelope.mark_as_end_of_turn()
            return event_envelope

        await self._call_agent(event_envelope, correlation_id=correlation_id, broker=broker)
        return observer_snapshot

    async def _call_agent(
        self, event_envelope: EventEnvelope, correlation_id: str, broker: BrokerAnnotation
    ) -> None:
        if event_envelope.groupchat_data is None:
            raise RuntimeError("Groupchat data is None for a call to a groupchat")

        event_envelope.final_response_topic = self.subscribed_topic
        event_envelope.mark_as_start_of_turn()
        await broker.publish(
            event_envelope,
            topic=event_envelope.groupchat_data.current_agent_topic,
            correlation_id=correlation_id,
        )
