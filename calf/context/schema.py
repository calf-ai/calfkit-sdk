from calf.types import CompactBaseModel


class EventContext(CompactBaseModel):
    text: str
    trace_id: str | None = None
