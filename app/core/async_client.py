# async_client.py
import asyncio
import grpc
from grpc import aio
from app.core import arrowstream_pb2
from app.core import arrowstream_pb2_grpc
from app.core.startup import StreamingQ
import pyarrow.ipc as ipc
import io


async def run():
    async with aio.insecure_channel("localhost:50051") as channel:
        stub = arrowstream_pb2_grpc.StreamServiceStub(channel)
        try:
                
            print("[CLIENT] Starting stream...")
            async for response in stub.StartStream(arrowstream_pb2.StreamRequest()):
                deserialized_table = ipc.open_stream(io.BytesIO(response.arrow_table)).read_all()
                await StreamingQ.put(deserialized_table.to_pydict())

            print("[CLIENT] Sending stop...")
            resp = await stub.StopStream(arrowstream_pb2.StopRequest())
            await StreamingQ.put("__EOF__")
            print("[CLIENT] Stop status:", resp.success)
        except Exception as err:
            print(f"[WEBSOCKET] Connection closed error: {err}")

if __name__ == "__main__":
    asyncio.run(run())
