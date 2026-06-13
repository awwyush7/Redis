import asyncio
import json
import os
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.servers.transport import Endpoint, cleanup_endpoint, shard_endpoint

SHARD_COUNT = 3
REGULAR_ORCH_PORT = 6379
POOLED_ORCH_PORT = 6380

# Prevents two benchmark runs from racing on the same ports
_bench_lock = asyncio.Lock()

app = FastAPI()

_static = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_static)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(_static / "index.html"))


@app.get("/health")
async def health():
    return {"status": "ok", "shards": SHARD_COUNT}


async def _start_servers() -> list[asyncio.Task]:
    """Start all shards + both orchestrators as asyncio tasks in the current loop."""
    from app.servers.server import Server
    from app.servers.orchestrator import Orchestrator
    from app.servers.orchestrator_pooled import OrchestratorPooled

    os.makedirs("/tmp/pyredis-aof", exist_ok=True)
    tasks: list[asyncio.Task] = []

    for i in range(SHARD_COUNT):
        ep = shard_endpoint(i)
        cleanup_endpoint(ep)
        tasks.append(asyncio.create_task(
            Server(host="127.0.0.1", port=9090 + i, node=i,
                   endpoint=ep, aof_dir="/tmp/pyredis-aof").run(),
            name=f"shard-{i}",
        ))

    # Let shards bind their sockets before the orchestrators connect
    await asyncio.sleep(0.7)

    reg_ep = Endpoint(kind="tcp", host="127.0.0.1", port=REGULAR_ORCH_PORT)
    cleanup_endpoint(reg_ep)
    tasks.append(asyncio.create_task(
        Orchestrator(shard_count=SHARD_COUNT, endpoint=reg_ep).run(),
        name="orchestrator",
    ))

    pooled_ep = Endpoint(kind="tcp", host="127.0.0.1", port=POOLED_ORCH_PORT)
    cleanup_endpoint(pooled_ep)
    tasks.append(asyncio.create_task(
        OrchestratorPooled(shard_count=SHARD_COUNT, endpoint=pooled_ep, pool_size=10).run(),
        name="orchestrator-pooled",
    ))

    await asyncio.sleep(0.6)
    return tasks


async def _stop_servers(tasks: list[asyncio.Task]) -> None:
    for t in tasks:
        if not t.done():
            t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@app.get("/api/benchmark")
async def benchmark_sse(n: int = 300, concurrency: int = 20):
    """Server-Sent Events endpoint — streams benchmark progress in real time."""
    from web.benchmark import run_all_scenarios

    if _bench_lock.locked():
        async def _busy():
            yield 'data: {"type":"error","message":"A benchmark is already running — try again in a moment."}\n\n'
        return StreamingResponse(_busy(), media_type="text/event-stream")

    queue: asyncio.Queue = asyncio.Queue()

    async def _send(msg: dict) -> None:
        await queue.put(msg)

    async def _produce() -> None:
        async with _bench_lock:
            server_tasks: list[asyncio.Task] = []
            try:
                server_tasks = await _start_servers()
                await run_all_scenarios(_send, n_requests=n, concurrency=concurrency)
            except Exception as exc:
                await _send({"type": "error", "message": str(exc)})
            finally:
                await _stop_servers(server_tasks)
                await queue.put(None)  # sentinel

    async def _stream():
        producer = asyncio.create_task(_produce())
        try:
            while True:
                msg = await queue.get()
                if msg is None:
                    break
                yield f"data: {json.dumps(msg)}\n\n"
        finally:
            if not producer.done():
                producer.cancel()
                await asyncio.gather(producer, return_exceptions=True)

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


if __name__ == "__main__":
    uvicorn.run(
        "web.app:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        log_level="warning",
    )
