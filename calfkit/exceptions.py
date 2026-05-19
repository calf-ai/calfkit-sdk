"""Exception hierarchy for the calfkit SDK.

All SDK-raised exceptions inherit from :class:`CalfkitError` so users can catch
the whole family with a single ``except`` clause. Concrete subtypes carry the
specific semantics (deserialization failure, aggregator state-store failure,
etc.).
"""


class CalfkitError(Exception):
    """Base class for all exceptions raised by the calfkit SDK."""


class DeserializationError(CalfkitError):
    """Raised when client-side output deserialization fails."""
