import asyncio
import os

from dotenv import load_dotenv
from faststream import FastStream
from calf.broker.broker import CalfConnection

async def listen():
    host = os.getenv('CALF_HOST_URL')
    gateway = None
    if host:
        gateway = CalfConnection(host)
    else:
        gateway = CalfConnection()
    
    runnable = FastStream(gateway)
    
    await runnable.run()

if __name__ == "__main__":
    load_dotenv()
    
    asyncio.run(listen())
    

    