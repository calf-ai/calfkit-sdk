import pytest

from calfkit._vendor.pydantic_ai import ModelRequest, ModelResponse, TextPart, models
from calfkit.gates.groupchat import GroupchatGate
from calfkit.models.groupchat import GroupchatDataModel, Turn


@pytest.fixture(autouse=True)
def block_model_requests():
    """Block actual model requests during unit tests."""
    original_value = models.ALLOW_MODEL_REQUESTS
    models.ALLOW_MODEL_REQUESTS = False
    yield
    models.ALLOW_MODEL_REQUESTS = original_value


# --- Helpers ---


class FakeAgentNode:
    """Minimal AgentNodeLike for testing GroupchatDataModel creation."""

    def __init__(self, topic: str, name: str):
        self._topic = topic
        self._name = name

    @property
    def subscribed_topic(self) -> str | None:
        return self._topic

    @property
    def name(self) -> str | None:
        return self._name


def _make_agents(count: int) -> list[FakeAgentNode]:
    names = ["Alice", "Bob", "Charlie", "Diana"]
    return [FakeAgentNode(f"topic_{i}", names[i]) for i in range(count)]


# --- GroupchatGate tests ---


def test_gate_allows_normal_response():
    gate = GroupchatGate()
    result = gate.gate("Hello everyone, great discussion!")
    assert result.skip is False


def test_gate_skips_ignore_response():
    gate = GroupchatGate()
    result = gate.gate("IGNORE: not relevant to me")
    assert result.skip is True


def test_gate_allows_none_response():
    gate = GroupchatGate()
    result = gate.gate(None)
    assert result.skip is False


def test_gate_ignore_is_case_sensitive():
    gate = GroupchatGate()
    assert gate.gate("ignore: lowercase").skip is False
    assert gate.gate("Ignore: titlecase").skip is False


def test_gate_prompt_contains_ignore_keyword():
    gate = GroupchatGate()
    prompt = gate.prompt()
    assert "IGNORE:" in prompt


# --- GroupchatDataModel creation tests ---


def test_create_groupchat_requires_at_least_two_agents():
    with pytest.raises(ValueError, match="at least 2 agents"):
        GroupchatDataModel.create_new_groupchat(_make_agents(1))


def test_create_groupchat_with_zero_agents():
    with pytest.raises(ValueError, match="at least 2 agents"):
        GroupchatDataModel.create_new_groupchat([])


def test_create_groupchat_with_two_agents():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(2))
    assert gc.groupchat_agent_topics == ["topic_0", "topic_1"]
    assert gc.groupchat_turn_index == -1
    assert gc.consecutive_skips == 0
    assert gc.turns_queue.maxlen == 1  # N-1 where N=2
    assert gc.is_current_turn_empty is True


def test_create_groupchat_with_three_agents():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(3))
    assert gc.groupchat_agent_topics == ["topic_0", "topic_1", "topic_2"]
    assert gc.turns_queue.maxlen == 2  # N-1 where N=3


def test_create_groupchat_default_system_prompt_includes_agent_names():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(2))
    assert "Alice" in gc.system_prompt_addition
    assert "Bob" in gc.system_prompt_addition


def test_create_groupchat_custom_system_prompt():
    gc = GroupchatDataModel.create_new_groupchat(
        _make_agents(2), system_prompt_addition="Custom prompt"
    )
    assert gc.system_prompt_addition == "Custom prompt"


# --- Round-robin and turn management tests ---


def test_current_agent_topic_round_robin():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(3))

    gc.groupchat_turn_index = 0
    assert gc.current_agent_topic == "topic_0"

    gc.groupchat_turn_index = 1
    assert gc.current_agent_topic == "topic_1"

    gc.groupchat_turn_index = 2
    assert gc.current_agent_topic == "topic_2"

    # Wraps around
    gc.groupchat_turn_index = 3
    assert gc.current_agent_topic == "topic_0"


def test_advance_resets_skip_counter_on_non_skip():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(2))
    gc.consecutive_skips = 1

    # Push a non-skipped turn so just_skipped returns False
    gc.turns_queue.push(Turn.create_new_turn(skipped=False))

    all_skipped = gc.advance_to_next_turn()
    assert all_skipped is False
    assert gc.consecutive_skips == 0


def test_advance_increments_skip_counter():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(2))

    gc.turns_queue.push(Turn.create_new_turn(skipped=True))

    all_skipped = gc.advance_to_next_turn()
    assert gc.consecutive_skips == 1
    assert all_skipped is False  # 1 < 2 agents


def test_all_agents_skip_terminates():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(2))
    gc.consecutive_skips = 1  # One agent already skipped

    gc.turns_queue.push(Turn.create_new_turn(skipped=True))
    all_skipped = gc.advance_to_next_turn()

    assert gc.consecutive_skips == 2
    assert all_skipped is True


def test_advance_increments_turn_index():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(3))
    assert gc.groupchat_turn_index == -1

    gc.turns_queue.push(Turn.create_new_turn(skipped=False))
    gc.advance_to_next_turn()
    assert gc.groupchat_turn_index == 0

    gc.turns_queue.push(Turn.create_new_turn(skipped=False))
    gc.advance_to_next_turn()
    assert gc.groupchat_turn_index == 1


# --- Commit and turn queue tests ---


def test_commit_turn_moves_to_queue():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(3))

    msg = ModelResponse(parts=[TextPart("Hello from agent")])
    gc.add_uncommitted_message_to_turn(msg)
    assert gc.is_current_turn_empty is False

    gc.commit_turn()
    assert gc.is_current_turn_empty is True  # Fresh uncommitted turn
    assert len(gc.turns_queue) == 1
    assert gc.turns_queue[0].messages[0] == msg


def test_turns_queue_evicts_oldest_when_full():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(2))
    # maxlen = 1 for 2 agents

    turn1 = Turn.create_new_turn()
    turn1.add_new_message(ModelResponse(parts=[TextPart("Turn 1")]))
    gc.turns_queue.push(turn1)

    turn2 = Turn.create_new_turn()
    turn2.add_new_message(ModelResponse(parts=[TextPart("Turn 2")]))
    gc.turns_queue.push(turn2)

    assert len(gc.turns_queue) == 1
    assert "Turn 2" in str(gc.turns_queue[0].messages[0].parts[0])


def test_flat_messages_from_turns_queue():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(3))

    msg1 = ModelResponse(parts=[TextPart("Hello")])
    msg2 = ModelRequest.user_text_prompt("World")

    turn = Turn.create_new_turn()
    turn.add_new_message(msg1)
    turn.add_new_message(msg2)
    gc.turns_queue.push(turn)

    flat = gc.flat_messages_from_turns_queue
    assert len(flat) == 2
    assert flat[0] == msg1
    assert flat[1] == msg2


# --- ensure_defaults tests ---


def test_ensure_defaults_fills_none_values():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(2))
    gc.groupchat_agent_topics = None
    gc.system_prompt_addition = None

    gc.ensure_defaults(["new_t1", "new_t2"], "Injected prompt")
    assert gc.groupchat_agent_topics == ["new_t1", "new_t2"]
    assert gc.system_prompt_addition == "Injected prompt"


def test_ensure_defaults_does_not_overwrite_existing():
    gc = GroupchatDataModel.create_new_groupchat(_make_agents(2))
    original_topics = gc.groupchat_agent_topics
    original_prompt = gc.system_prompt_addition

    gc.ensure_defaults(["overwrite_t1"], "Overwrite prompt")
    assert gc.groupchat_agent_topics == original_topics
    assert gc.system_prompt_addition == original_prompt
