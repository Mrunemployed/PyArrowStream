from app.core import arrowstream_pb2
from app.core import arrowstream_pb2_grpc
from app.utils.pprint import pprint
import asyncio
from asyncio import AbstractEventLoop
import grpc
from grpc import aio
import sys
import signal
import pyarrow as pa
import pyarrow.ipc as ipc
import random
import datetime
import io

class StreamService(arrowstream_pb2_grpc.StreamService):

    async def StartStream(self, request, context):
        for i in range(5):
            # Simulate rows
            num_rows = 5

            # Generate sample data
            ids = list(range(i * num_rows, (i + 1) * num_rows))
            names = [random.choice(["Alice", "Bob", "Charlie", "Diana"]) for _ in range(num_rows)]
            scores = [round(random.uniform(50.0, 100.0), 2) for _ in range(num_rows)]
            timestamps = [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") for _ in range(num_rows)]

            # Create Arrow table
            batch = pa.table({
                "id": pa.array(ids, type=pa.int32()),
                "name": pa.array(names, type=pa.string()),
                "score": pa.array(scores, type=pa.float64()),
                # "timestamp": pa.array(timestamps, type=pa.timestamp("us")),
                "timestamp": pa.array(timestamps, type=pa.string()),


            })

            # Serialize to Arrow IPC format
            sink = io.BytesIO()
            with ipc.new_stream(sink, batch.schema) as writer:
                writer.write_table(batch)
            serialized = sink.getvalue()

            # Send through gRPC
            yield arrowstream_pb2.StreamResponse(arrow_table=serialized)
            await asyncio.sleep(1)

    async def StopStream(self, request, context):
        # Could set a shared flag here to cancel streaming
        return arrowstream_pb2.StopResponse(success=True)


class ServerManager:
    def __init__(self):
        self.StreamingService : StreamService = None
        self._server : aio.Server = None
        self._loop : AbstractEventLoop = None
    
    async def stop(self,*args):
        await self._server.stop(3)
        await self._server.wait_for_termination(5)
        self._loop.stop()
        # await self._server.stop(grace=3)

    def start_serving(self):
        self._loop = asyncio.new_event_loop()
        signal.signal(signal.SIGINT, lambda s,y: self._loop.create_task(self.stop(s,y)))
        signal.signal(signal.SIGTERM, lambda s,y: self._loop.create_task(self.stop(s,y)))
        try:
            self._loop.create_task(self.serve())
            self._loop.run_forever()
        finally:
            self._loop.close()
        
    async def serve(self):
        self.StreamingService = StreamService()
        self._server = aio.server()
        arrowstream_pb2_grpc.add_StreamServiceServicer_to_server(self.StreamingService, self._server)
        self._server.add_insecure_port("[::]:50051")
        await self._server.start()
        pprint("[gRPC] Async Server started on port 50051")