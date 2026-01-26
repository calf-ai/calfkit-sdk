from pydantic_ai.models.openai import Model, OpenAIChatModel

from calf.providers.base import ProviderClient


class TestPydantic(ProviderClient):
    def __init__(self):
        self.model = OpenAIChatModel()
    async def generate(self):
        
