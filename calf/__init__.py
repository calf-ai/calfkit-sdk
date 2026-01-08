from importlib.metadata import version

from calf.client import Calf
from calf.message import Message

__version__ = version("calf-sdk")
__all__ = ["Calf", "Message", "__version__"]
