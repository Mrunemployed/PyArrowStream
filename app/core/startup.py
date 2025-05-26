from app.core.async_serve import ServerManager
from multiprocessing import Process
from typing import Union,List,Dict
from dataclasses import dataclass
import asyncio
from app.utils.pprint import pprint
from asyncio import Queue


@dataclass
class SpawnedProcesses:
    gRPC_process : Process = None
    Server_Obj : ServerManager = None

class ServerCollections:
    servers: List[SpawnedProcesses] = []
    
server_collections = ServerCollections()

StreamingQ = Queue()

async def spawn_server():

    pprint("[SPAWNER] Trying to spwan GRPC SERVER")
    Manager = ServerManager()
    process = Process(target=Manager.start_serving)
    spawned_process = SpawnedProcesses(
        gRPC_process=process,
        Server_Obj=Manager
    )
    server_collections.servers.append(spawned_process)
    process.start()
    pprint("[SPAWNER] Spawned GRPC SERVER Successfully")

async def tear_down():
    pprint("[SPAWNER] Shutting down all GRPC servers.")
    for process in server_collections.servers:
        pprint("[SHUTDOWN] Trying to stop GRPC Server",color="yellow")
        # await process.Server_Obj.stop()
        await asyncio.sleep(1)
        process.gRPC_process.terminate()
        process.gRPC_process.join()
        pprint("[SHUTDOWN] TearDown Complete GRPC SERVER has been deactivated successfully",color="yellow")

    
