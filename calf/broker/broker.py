import os
from typing import Iterable, Optional
from faststream.kafka import KafkaBroker

class Broker(KafkaBroker):
    """Lightweight client wrapper connecting to Calf brokers"""
    
    def __init__(self, bootstrap_servers: Optional[str | Iterable[str]] = None, **broker_kwargs):
        if not bootstrap_servers:
            bootstrap_servers = os.getenv('CALF_HOST_URL')
        super().__init__(bootstrap_servers or 'localhost', **broker_kwargs)
        
        

def create_broker(bootstrap_servers: Optional[str | Iterable[str]] = None, **broker_kwargs) -> KafkaBroker:
    return KafkaBroker(bootstrap_servers or 'localhost', **broker_kwargs)