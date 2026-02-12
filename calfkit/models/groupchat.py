from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from pydantic import Field

from calfkit._vendor.pydantic_ai import ModelMessage
from calfkit.gates.groupchat import GroupchatGate
from calfkit.models.bounded_queue import BoundedQueue
from calfkit.models.types import CompactBaseModel


@runtime_checkable
class AgentNodeLike(Protocol):
    """Structural type for objects that expose an agent's topic and name.

    Satisfied by AgentRouterNode (and any BaseNode subclass).
    Used to decouple the models layer from the nodes layer.
    """

    @property
    def subscribed_topic(self) -> str | None: ...

    @property
    def name(self) -> str | None: ...


class Turn(CompactBaseModel):
    messages: list[ModelMessage] = Field(default_factory=list)
    skipped: bool

    def add_new_message(self, message: ModelMessage):
        self.messages.append(message)

    def extend_new_messages(self, messages: list[ModelMessage]):
        self.messages.extend(messages)

    @classmethod
    def create_new_turn(
        cls, messages: list[ModelMessage] | None = None, skipped: bool = False
    ) -> "Turn":
        return cls(messages=messages if messages is not None else [], skipped=skipped)


class GroupchatDataModel(CompactBaseModel):
    # Groupchat schema
    groupchat_agent_topics: list[str] | None
    groupchat_turn_index: int = -1
    consecutive_skips: int = 0
    system_prompt_addition: str | None = "\n\nYou are in a groupchat with other agents.\n\n"

    # Trailing list of the last N - 1 turns for a N-sized groupchat
    turns_queue: BoundedQueue[Turn]
    uncommitted_turn: Turn = Field(default_factory=Turn.create_new_turn)

    gate_kind: str

    def increment_turn_index(self):
        self.groupchat_turn_index += 1

    def increment_skip(self, reset: bool = False):
        if reset:
            self.consecutive_skips = 0
        else:
            self.consecutive_skips += 1

    def mark_skip_current_turn(self):
        self.uncommitted_turn.skipped = True

    def patch_groupchat_agent_topics(self, agent_topics: list[str]):
        self.groupchat_agent_topics = agent_topics

    def set_system_prompt_addition(self, system_prompt_addition: str):
        self.system_prompt_addition = system_prompt_addition

    def commit_turn(self):
        self.turns_queue.push(self.uncommitted_turn)
        self.uncommitted_turn = Turn.create_new_turn()

    def add_uncommitted_message_to_turn(self, message: ModelMessage):
        self.uncommitted_turn.add_new_message(message)

    def is_all_skipped(self):
        return self.consecutive_skips >= len(self.groupchat_agent_topics or [])

    def advance_to_next_turn(self) -> bool:
        """Advance the turn index and update skip tracking.

        Returns True if all agents have consecutively skipped (termination condition).
        """
        self.groupchat_turn_index += 1
        if self.just_skipped:
            self.consecutive_skips += 1
        else:
            self.consecutive_skips = 0
        return self.is_all_skipped()

    @property
    def current_agent_topic(self) -> str:
        """The topic for the agent whose turn it is, based on round-robin index."""
        topics = self.groupchat_agent_topics or []
        return topics[self.groupchat_turn_index % len(topics)]

    def ensure_defaults(
        self, agent_topics: list[str], system_prompt_addition: str | None
    ) -> None:
        """Populate agent topics and system prompt from deployed node config if not already set."""
        if self.groupchat_agent_topics is None:
            self.groupchat_agent_topics = agent_topics
        if self.system_prompt_addition is None and system_prompt_addition is not None:
            self.system_prompt_addition = system_prompt_addition

    @property
    def just_skipped(self):
        if self.turns_queue:
            return self.turns_queue[-1].skipped
        return False

    @property
    def turn_index(self):
        return self.groupchat_turn_index

    @property
    def flat_messages_from_turns_queue(self) -> list[ModelMessage]:
        messages = []
        for turn in self.turns_queue.iter_items():
            messages.extend(turn.messages)
        return messages

    @property
    def is_current_turn_empty(self):
        return not self.uncommitted_turn.messages

    @classmethod
    def create_new_groupchat(
        cls,
        agent_nodes: Sequence[AgentNodeLike],
        *,
        system_prompt_addition: str | None = None,
        gate_kind: str = GroupchatGate.kind,
    ):
        agent_topics: list[str] = []
        agent_names: list[str] = []
        for node in agent_nodes:
            if node.subscribed_topic is None:
                continue
            agent_topics.append(node.subscribed_topic)
            agent_names.append(node.name or "general-agent (no name)")

        if len(agent_topics) < 2:
            raise ValueError(
                f"A groupchat requires at least 2 agents with valid topics, got {len(agent_topics)}"
            )

        if system_prompt_addition is None:
            system_prompt_addition = (
                "\n\nYou are in a groupchat with other agents. The agents in the groupchat are:\n"
            )
            system_prompt_addition += "\n".join("- " + name for name in agent_names)
        return cls(
            groupchat_agent_topics=agent_topics,
            system_prompt_addition=system_prompt_addition,
            turns_queue=BoundedQueue[Turn](maxlen=len(agent_topics) - 1),
            uncommitted_turn=Turn.create_new_turn(),
            gate_kind=gate_kind,
        )
