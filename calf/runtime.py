from abc import ABC
from typing import Callable, Iterable, Optional

from faststream import FastStream
from calf.broker.broker import Broker
from boltons.typeutils import classproperty
from faststream.kafka import TestKafkaBroker

class CalfRuntime(ABC):
    _calf: Broker | TestKafkaBroker | None = None
    _initialized = False
    
    @classmethod
    def initialize(cls, bootstrap_servers: Optional[str | Iterable[str]] = None, **broker_kwargs):
        if cls._initialized:
            raise RuntimeError("Calf runtime already initialized")
        cls._calf = Broker(bootstrap_servers, **broker_kwargs)
        cls._initialized = True
        
    @classmethod
    def initialize_in_memory(cls, bootstrap_servers: Optional[str | Iterable[str]] = None, **broker_kwargs):
        if cls._initialized:
            raise RuntimeError("Calf runtime already initialized")
        broker = Broker(bootstrap_servers, **broker_kwargs)
        cls._calf = TestKafkaBroker(broker)
        cls._initialized = True
    
    @classproperty
    def initialized(cls):
        return cls._initialized
    
    @classproperty  
    def calf(cls) -> Broker | TestKafkaBroker:
        if not cls._calf:
            raise RuntimeError("Calf runtime not initialized. Run `initialize()`")
        return cls._calf
    
    @classproperty  
    def runnable(cls):
        return FastStream(CalfRuntime.calf)
    
    @classproperty  
    def start(cls):
        return cls.runnable.run()