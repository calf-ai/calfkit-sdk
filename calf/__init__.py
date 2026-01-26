from importlib.metadata import version

from calf.message import Message

__version__ = version("calf-sdk")
__all__ = ["Message", "__version__"]
