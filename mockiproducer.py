import pyarrow as pa
from pyarrow import ipc
import asyncio
from asyncio import AbstractEventLoop, Queue
from typing import Any
from streamhub import stream_pipeline
from threading import Thread
import datetime
from datetime import timezone
import time

class Producer:

    def __init__(self, schema: pa.Schema):
        self.loop : AbstractEventLoop = None
        self.queue : Queue = None
        self.thread_id : int = None
        self.event : asyncio.Event = asyncio.Event()
        self.syncEvent : asyncio.Event = asyncio.Event()
        self.schema = schema
        self.sink   = pa.BufferOutputStream()
        self.writer = ipc.RecordBatchStreamWriter(self.sink, self.schema)
        self._offset = 0 

    async def _start_(self):
        self.queue = Queue()
        self.syncEvent.set()
        # keepalive = self.event.wait()
    
    async def push_task(self, arr):
        # Serialize to IPC bytes and stream
        await stream_pipeline.syncEvent.wait()
        print(f"[PRODUCER] Pushing data for processing")
        batch = pa.RecordBatch.from_arrays([arr], names=['value'])
        self.writer.write_batch(batch)
        full = self.sink.getvalue().to_pybytes()
        new  = full[self._offset:]
        self._offset = len(full)
        await stream_pipeline.stream(new)

    async def end_stream(self):
        self.writer.close()
        await stream_pipeline.stream_complete()

        

    def input_anchor(self,conversion_type:str=None, user_input:Any=None):
        if not (conversion_type and user_input):
            return
        # Build an Arrow Array of one element, according to the chosen type
        print(f"[PRODUCER] recieved input data batch: {user_input}")
        if conversion_type == "Integer":
            arr = pa.array([int(user_input)], type=pa.int64())

        elif conversion_type == "Float":
            arr = pa.array([float(user_input)], type=pa.float64())

        elif conversion_type == "Uppercase String":
            arr = pa.array([user_input.upper()], type=pa.string())

        elif conversion_type == "Lowercase String":
            arr = pa.array([user_input.lower()], type=pa.string())

        elif conversion_type == "Datetime":
            ns = datetime.datetime.now()
            # ns = time.time()
            # str_time = str(ns)
            print(f"[PRODUCER] generated time string: {str(user_input)}")
            arr = pa.array([user_input], type=pa.timestamp('ns'))
            # arr = pa.array([user_input], type=pa.string())


        else:
            # fallback to raw string if the user_input doesnt match
            arr = pa.array([user_input], type=pa.string())

        # Pack into a Table
        # table = pa.Table.from_arrays([arr], names=['value'])
        asyncio.run_coroutine_threadsafe(
            self.push_task(arr=arr),
            stream_pipeline.loop
        )
    
    def end_stream(self):
        """
        End Stream and close writer
        """
        try:
            self.writer.close()
        except Exception:
            pass


    def start_producer(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.create_task(
            self._start_()
        )
        if not self.loop.is_running():
            self.loop.run_forever()
        # self.loop.run_forever()