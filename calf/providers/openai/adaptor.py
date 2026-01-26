from agents.function_schema import function_schema
from openai.types.chat import (
    ChatCompletionMessageParam,
    ChatCompletionToolParam,
)

from calf.providers.base import MessageAdaptor, ToolAdaptor


class OpenAIClientMessage(MessageAdaptor[ChatCompletionMessageParam]):
    @classmethod
    def create_user_message(cls, message: str, **kwargs) -> ChatCompletionMessageParam:
        return {"role": "user", "content": message}

    @classmethod
    def create_assistant_message(
        cls, message: str, tool_calls: list[ChatCompletionToolParam], **kwargs
    ) -> ChatCompletionMessageParam:
        return {"role": "assistant", "content": message}

    @classmethod
    def create_tool_message(
        cls, tool_call_id: str, message: str, **kwargs
    ) -> ChatCompletionMessageParam:
        return {"role": "tool", "tool_call_id": tool_call_id, "content": message}

    @classmethod
    def create_system_message(cls, message: str) -> ChatCompletionMessageParam:
        return {"role": "system", "content": message}


class OpenAIClientTool(ToolAdaptor[ChatCompletionToolParam]):
    @classmethod
    def create_tool_schema(cls, tool) -> ChatCompletionToolParam:
        name = None
        desc = None
        params = None
        if callable(tool):
            schema = function_schema(tool)
            name = schema.name
            desc = schema.description or ""
            params = schema.params_json_schema
        elif isinstance(tool, dict):
            name = tool.get("name")
            desc = tool.get("description")
            params = tool.get("parameters")

        if not (name and desc and params):
            raise TypeError(
                "Input tool must either be a callable func or a schema of type `BasicToolSchema`"
            )

        return {
            "type": "function",
            "function": {
                "name": name,
                "description": desc,
                "parameters": params,
            },
        }
