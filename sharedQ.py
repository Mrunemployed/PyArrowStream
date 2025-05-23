from asyncio import Queue, AbstractEventLoop
import asyncio

class Pipeline:

    def __init__(self):
        self._pipeline : Queue = None
        self.loop : AbstractEventLoop = None
        self.syncEvent : asyncio.Event = asyncio.Event()

    async def stream(self,incomingData):
        await self.syncEvent.wait()
        print(f"[Pipeline] recieved data chunk {len(incomingData)} bytes")
        await self._pipeline.put(incomingData)
    
    async def stream_recv(self):
        chunk = await self._pipeline.get()
        return chunk

    async def stream_complete(self):
        print(f"[Pipeline] pushing chunk EOF")
        await self._pipeline.put("<EOF>")
    