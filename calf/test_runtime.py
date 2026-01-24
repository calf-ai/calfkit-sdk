from typing import Callable, Iterable, Optional

from faststream import FastStream
from calf.broker.broker import Broker
from boltons.typeutils import classproperty
from faststream.kafka import TestKafkaBroker

from calf.runtime import CalfRuntime


class InMemoryCalfRuntime(CalfRuntime):        
    @classmethod
    def initialize(cls, bootstrap_servers: Optional[str | Iterable[str]] = None, **broker_kwargs):
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