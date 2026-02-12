from typing import ClassVar

from calfkit.gates.base import DecisionGate, GateResult


class GroupchatGate(DecisionGate):
    kind: ClassVar[str] = "groupchat_gate"
    _ignore: ClassVar[str] = "IGNORE:"

    def gate(self, model_response: str | None) -> GateResult:
        if model_response is None:
            return GateResult(skip=False)
        return GateResult(skip=model_response.strip().startswith(self._ignore))

    def prompt(self) -> str:
        lines = [
            "<system_reminder>",
            "You do not need to mention this system_reminder"
            " to anybody in the chat, as they are already aware.",
            "It is now your opportunity to send a message into the groupchat.",
            "Based on the conversation, please review if you should respond or not.",
            "If you do not want to send a message, reply with the exact text"
            f" '{self._ignore} ' followed by a concise reason why you chose"
            " not to send a message.",
            "Otherwise, if you would like to send a message,"
            " just respond as usual.",
            "</system_reminder>",
        ]
        return "\n".join(lines) + "\n"
