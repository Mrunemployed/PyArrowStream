import pyarrow as pa
from pyarrow import ipc
import asyncio
from asyncio import Future
from typing import Callable
from asyncio import AbstractEventLoop, Queue
from streamhub import stream_pipeline
from threading import Thread
import uuid


class Consumer:

    def __init__(
        self,
        # QUEUE: Queue
        ):
        # self.queue = QUEUE
        self.loop : AbstractEventLoop = None
        self.active : asyncio.Event = None
        self.buffer : bytes = None
        self.reader = None
        self.thread : Thread = None
        self.offset = 0

    async def process(self, chunk):
        print(f"[CONSUMER] processing chunk: {chunk}")
        if not self.buffer:
            self.buffer = chunk
        else:
            self.buffer += chunk

        while True:
            if chunk == "EOF":
                print("[CONSUMER] Got EOF, shutting down")
                self.active.clear()
                break
            try:
                reader = ipc.RecordBatchStreamReader(pa.py_buffer(self.buffer[self.offset:]))
            except pa.ArrowInvalid:
                # not enough bytes yet to form a valid stream
                return

            print("[CONSUMER] Reading next batch...")

            try:
                batch = reader.read_next_batch()
            except (StopIteration, pa.ArrowInvalid):
                print("[CONSUMER] finished reading batches")
                break
            except Exception as err:
                print(f"[CONSUMER] Exception: {err}")
                break

            if batch is None:
                print("[CONSUMER] finished reading batches")
                break

            table = pa.Table.from_batches([batch])
            try:
                self.callback(table)
            except Exception as err:
                print("[CONSUMER - EXCEPTION]", err)

            # Update offset
            new_offset = len(self.buffer)
            self.offset = new_offset


        
    async def consume(self):
        print("[CONSUMER] waiting for new tasks...")
        stream_pipeline.syncEvent.set()
        while True:
            chunk = await stream_pipeline._pipeline.get()
            if str(chunk) == "EOF":
                break
            print("[CONSUMER] recieved new task processing...")
                # break
            await self.process(chunk)
    
    async def _start_(self):
        print("[CONSUMER] Initializing...")
        stream_pipeline._pipeline = Queue()
        stream_pipeline.syncEvent = asyncio.Event()
        stream_pipeline.loop = self.loop
        await asyncio.gather(
            self.consume()
        )
        print(f"[DEBUG] Queue ID: {id(stream_pipeline._pipeline)}")


    def start_consumer(self):
        print("[CONSUMER] starting up...")
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.future = Future()
        async def wrapper():
            self.active = asyncio.Event()
            stream_pipeline.loop = self.loop
            await self._start_()
            self.loop.stop()  # 🧼 shut down loop *only after* _start_ finishes

        self.loop.create_task(wrapper())
        self.loop.run_until_complete(self.future)


    def spawn(self,callback:Callable):
        # convenience to start in a thread
        self.callback = callback
        self.thread = Thread(target=self.start_consumer, daemon=True).start()
    
    def start_streaming(self):
        if self.loop and self.active:
            self.loop.call_soon_threadsafe(self.active.set)
        if not stream_pipeline._pipeline:
            stream_pipeline._pipeline = Queue()
    
    def stop_streaming(self): 
        self.active.clear()
        if self.loop and self.active:
            self.loop.call_soon_threadsafe(lambda: stream_pipeline._pipeline.put_nowait("EOF"))
        stream_pipeline.stream_complete()
        self.thread.join() if self.thread else None
        self.future.set_result(1) if self.future else None


    def stop_consuming(self):
        """Cancel the asyncio loop and free buffer."""
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
        self.buffer = b""
        self.reader = None
        stream_pipeline._pipeline = None
