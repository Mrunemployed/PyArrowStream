import pyarrow as pa
from pyarrow import ipc
import asyncio
from typing import Callable
from asyncio import AbstractEventLoop, Queue
from streamhub import stream_pipeline
from threading import Thread
import uuid


class Consumer:

    def __init__(self):
        self.loop : AbstractEventLoop = None
        self.syncEvent : asyncio.Event = asyncio.Event()
        self.buffer : bytes = None
        self.reader = None

    async def _start_(self):
        print("[CONSUMER] Initializing...")
        stream_pipeline._pipeline = Queue()
        stream_pipeline.syncEvent.set()
        stream_pipeline.loop = self.loop
        consumer_task = self.consume()
        await asyncio.gather(
            consumer_task
        )

    async def process(self,chunk):
        if not self.buffer:
            self.buffer : bytes = chunk
        else:
            self.buffer= self.buffer + chunk

        if self.reader is None:
            self.reader = ipc.RecordBatchStreamReader(pa.py_buffer(self.buffer))
        while True:
            print("[CONSUMER] Reading next batch...")
            try:
                batch = self.reader.read_next_batch()
            except StopIteration:
                print("[CONSUMER] finished reading batches")
                break
            if batch is None:
                print("[CONSUMER] finished reading batches")
                break
            table = pa.Table.from_batches([batch])
            try:
                self.callback(table)
            except Exception as err:
                print("[CONSUMER - EXCEPTION]", err)
        

    async def consume(self):
        print("[CONSUMER] waiting for new tasks...")
        await stream_pipeline.syncEvent.wait()
        while True:
            chunk = await stream_pipeline.stream_recv()
            print("[CONSUMER] recieved new task processing...")
            if chunk == "<EOF>":
                print("[Consumer] End of stream")
                # break
            else:
                await self.process(chunk)
    
    def start_consumer(self):
        print("[CONSUMER] starting up...") 
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.create_task(
            self._start_()
        )
        if not self.loop.is_running():
            self.loop.run_forever()

    def spawn(self,callback:Callable):
        # convenience to start in a thread
        self.callback = callback
        Thread(target=self.start_consumer, daemon=True).start()
    
    def close_consuming(self):
        """Cancel the asyncio loop and free buffer."""
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
        self.buffer = b""
        self.reader = None