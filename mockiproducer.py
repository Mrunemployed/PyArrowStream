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

    def __init__(
        self,
        schema: pa.Schema,
    ):
        self.loop : AbstractEventLoop = None
        self.schema = schema
        self.sink   = pa.BufferOutputStream()
        self.writer = ipc.new_stream(self.sink, self.schema)
        self._offset = 0 

    async def _start_(self):
        await stream_pipeline.syncEvent.wait()
        # keepalive = self.event.wait()
    
    async def push_task(self, arr):
        print(f"[DEBUG] Queue ID: {id(stream_pipeline._pipeline)}")
        await asyncio.sleep(1)
        # Serialize to IPC bytes and stream
        print(f"[PRODUCER] Pushing data for processing")
        batch = pa.RecordBatch.from_arrays([arr], names=['value'])
        self.writer.write_batch(batch)
        full = self.sink.getvalue().to_pybytes()
        new  = full[self._offset:]
        self._offset = len(full)
        await stream_pipeline._pipeline.put(new)

    def end_stream(self):
        stream_pipeline.stream_complete()
        try:
            self.writer.close()
        except: pass
        # await stream_pipeline.stream_complete()

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
            print(f"[PRODUCER] generated time string: {str(user_input)}")
            arr = pa.array([user_input], type=pa.timestamp('ns'))

        else:
            # fallback to raw string if the user_input doesnt match
            arr = pa.array([user_input], type=pa.string())

        asyncio.run_coroutine_threadsafe(self.push_task(arr=arr),self.loop)

    def _set_loop_(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.create_task(
            self._start_()
        )
        if not self.loop.is_running():
            self.loop.run_forever()
        self.loop.run_forever()

    def start_streaming(self, schema:pa.Schema=None):
        if schema is not None:
            self.schema = schema
        if self.loop is None:                   # first ever start
            Thread(target=self._set_loop_, daemon=True).start()
        else:
            self.loop.call_soon_threadsafe(self.stop_streaming)


    def stop_streaming(self): self.loop.call_soon_threadsafe(self.end_stream) if self.loop else None

    def destroy(self):
        self.stop_streaming()
        self.loop = None
        self.active.clear()