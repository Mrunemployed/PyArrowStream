from fastapi import APIRouter, WebSocket
from app.core.async_client import run
from app.core.startup import StreamingQ
import asyncio
import datetime
import pyarrow.compute as pc
import pyarrow as pa

router = APIRouter(prefix="/server")

@router.get("/start_client")
async def start_client_stream():
    asyncio.create_task(run())

@router.websocket("/ws/stream")
async def websocket_stream(websocket:WebSocket):
    await websocket.accept()
    asyncio.create_task(run())
    try:
        while True:
            data = await StreamingQ.get()
            if data == "__EOF__":
                await websocket.close(code=1000)
                break
            for keys,arrowColumn in data.items():
                if isinstance(arrowColumn[0], str):
                    # Skip
                    continue
                if isinstance(arrowColumn[0], datetime.datetime):
                    arr = pa.array(arrowColumn, type=pa.timestamp("ns"))
                    print(f"[DEBUG] Field '{keys}' → Arrow type: {arr.type}")
                    print(f"[DEBUG] Arrow logical type: {repr(arr.type)}")

                    # droptz = arr.cast(pa.timestamp('us'))
                    out = pc.strftime(arr, format="%Y-%m-%d %H:%M:%S %f")
                    data[keys] = out.to_pylist()
            print("Type of data recieved: ", type(data))
            await websocket.send_json(data)
    except Exception as err:
        await websocket.close()
        print(f"[Websocket] Closed due to error: {err}")