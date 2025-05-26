from multiprocessing import Process
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from app.endpoints.streaming_apis import router
from app.core.startup import tear_down, spawn_server
from contextlib import asynccontextmanager
from pathlib import Path
import pyarrow.util
pyarrow.util.download_tzdata_on_windows()


@asynccontextmanager
async def lifespan(app:FastAPI):
    """
    The stub for the startup callables and coros, 
    currently this is being used as the control for spawning processes
    simulating the IPC GRPC Connections

    Args:
        app(FastAPI): The dependency injection of FastAPI app
    """
    await spawn_server()
    yield
    await tear_down()

app = FastAPI(
    title="Arrows to Athens",
    debug=True,
    description="Mockup IPC Application to simulate RPC streaming of Apache Arrow tables",
    version="1.0.0",
    openapi_tags=[
        {"name": "Auth", "description": "Starts a Grpc Server and starts randomized streaming of pyarrow tables"},
    ],
    lifespan=lifespan
)

app.mount("/static", StaticFiles(directory="static", html=True), name="static")

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    print("[FRONTEND] Rendering UI")
    path_to_ui = Path(__file__).parent / "static" / "render.html"
    return HTMLResponse(content=str(path_to_ui.read_text(encoding='utf-8')), status_code=200)

app.include_router(router=router)