# Mockup IPC Arrow Streaming Server

from the root of the Project run,

```bash
 py -m uvicorn fastapp:app --port 5919
```

compile grpc files:
``
python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/stream.proto
```
