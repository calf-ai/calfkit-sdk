from abc import ABC, abstractmethod


class Deployable(ABC):
    @abstractmethod
    async def run_app(self): ...
