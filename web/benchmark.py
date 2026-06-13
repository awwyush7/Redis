import asyncio
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.protocol_handler.protocol_handler import Error, ProtocolHandler
from app.servers.transport import Endpoint

REGULAR_EP = Endpoint(kind="tcp", host="127.0.0.1", port=6379)
POOLED_EP = Endpoint(kind="tcp", host="127.0.0.1", port=6380)

SCENARIOS = [
    {
        "id": "A",
        "name": "Sync · Sequential",
        "badge": "Stage 1 baseline",
        "detail": "Blocking socket · one req at a time · no async",
        "color": "#ef4444",
    },
    {
        "id": "B",
        "name": "Async · Sequential",
        "badge": "asyncio streams",
        "detail": "Cooperative I/O · single connection · still serial",
        "color": "#f59e0b",
    },
    {
        "id": "C",
        "name": "Async · Concurrent",
        "badge": "N workers · regular orch",
        "detail": "N async clients in parallel · open-close per shard cmd",
        "color": "#10b981",
    },
    {
        "id": "D",
        "name": "Async · Pooled + Concurrent",
        "badge": "N workers · LIFO pool",
        "detail": "N async clients · pooled shard connections · least overhead",
        "color": "#8b5cf6",
    },
]


class _AsyncClient:
    def __init__(self, endpoint: Endpoint):
        self._ep = endpoint
        self._r = self._w = None
        self._ph = ProtocolHandler()

    async def connect(self):
        if self._ep.kind == "unix":
            self._r, self._w = await asyncio.open_unix_connection(self._ep.path)
        else:
            self._r, self._w = await asyncio.open_connection(self._ep.host, self._ep.port)

    async def close(self):
        if self._w and not self._w.is_closing():
            self._w.close()
            try:
                await asyncio.wait_for(self._w.wait_closed(), timeout=1.0)
            except Exception:
                pass

    async def execute(self, *args):
        await self._ph.write_response(self._w, list(args))
        resp = await self._ph.handle_request(self._r)
        if isinstance(resp, Error):
            raise RuntimeError(f"Server: {resp.msg}")
        return resp


def _sync_bench(endpoint: Endpoint, n: int, prefix: str) -> list[float]:
    import socket as _socket

    ph = ProtocolHandler()
    lats: list[float] = []

    if endpoint.kind == "unix":
        sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
        sock.connect(endpoint.path)
    else:
        sock = _socket.create_connection((endpoint.host, endpoint.port))

    fh = sock.makefile("rwb")
    try:
        for i in range(n):
            t0 = time.perf_counter()
            ph.write_response_sync(fh, ["SET", f"{prefix}:{i}", f"v{i}"])
            ph.handle_request_sync(fh)
            lats.append((time.perf_counter() - t0) * 1000)
    finally:
        fh.close()
        sock.close()
    return lats


async def _seq_async_bench(endpoint: Endpoint, n: int, prefix: str) -> list[float]:
    c = _AsyncClient(endpoint)
    await c.connect()
    lats: list[float] = []
    try:
        for i in range(n):
            t0 = time.perf_counter()
            await c.execute("SET", f"{prefix}:{i}", f"v{i}")
            lats.append((time.perf_counter() - t0) * 1000)
    finally:
        await c.close()
    return lats


async def _concurrent_bench(
    endpoint: Endpoint, n: int, concurrency: int, prefix: str
) -> list[float]:
    actual = min(concurrency, n)
    clients = []
    for _ in range(actual):
        c = _AsyncClient(endpoint)
        await c.connect()
        clients.append(c)

    queue: asyncio.Queue[int] = asyncio.Queue()
    for i in range(n):
        await queue.put(i)

    all_lats: list[float] = []
    lock = asyncio.Lock()

    async def worker(client: _AsyncClient):
        lats: list[float] = []
        while True:
            try:
                i = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            t0 = time.perf_counter()
            await client.execute("SET", f"{prefix}:{i}", f"v{i}")
            lats.append((time.perf_counter() - t0) * 1000)
            queue.task_done()
        async with lock:
            all_lats.extend(lats)

    await asyncio.gather(*[worker(c) for c in clients])

    for c in clients:
        await c.close()
    return all_lats


def _metrics(lats: list[float], wall: float) -> dict:
    if not lats:
        return {"rps": 0, "p50": 0, "p95": 0, "p99": 0, "mean": 0, "total": 0}
    s = sorted(lats)
    n = len(s)

    def pct(p: float) -> float:
        return round(s[max(0, int(n * p) - 1)], 2)

    return {
        "rps": round(n / wall),
        "p50": pct(0.50),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "mean": round(statistics.mean(s), 2),
        "total": n,
    }


async def run_all_scenarios(
    send: Callable, n_requests: int = 500, concurrency: int = 20
) -> None:
    # Unique prefix per run so repeated benchmark runs don't hit "key already exists"
    rid = int(time.time_ns() // 1_000_000) % 1_000_000

    await send(
        {
            "type": "start",
            "scenarios": SCENARIOS,
            "n_requests": n_requests,
            "concurrency": concurrency,
        }
    )

    # A — sync sequential
    await send({"type": "scenario_start", "id": "A"})
    t0 = time.perf_counter()
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=1) as pool:
        lats = await loop.run_in_executor(pool, _sync_bench, REGULAR_EP, n_requests, f"{rid}A")
    await send({"type": "scenario_done", "id": "A", "metrics": _metrics(lats, time.perf_counter() - t0)})

    # B — async sequential
    await send({"type": "scenario_start", "id": "B"})
    t0 = time.perf_counter()
    lats = await _seq_async_bench(REGULAR_EP, n_requests, f"{rid}B")
    await send({"type": "scenario_done", "id": "B", "metrics": _metrics(lats, time.perf_counter() - t0)})

    # C — async concurrent, regular orchestrator
    await send({"type": "scenario_start", "id": "C"})
    t0 = time.perf_counter()
    lats = await _concurrent_bench(REGULAR_EP, n_requests, concurrency, f"{rid}C")
    await send({"type": "scenario_done", "id": "C", "metrics": _metrics(lats, time.perf_counter() - t0)})

    # D — async concurrent, pooled orchestrator
    await send({"type": "scenario_start", "id": "D"})
    t0 = time.perf_counter()
    lats = await _concurrent_bench(POOLED_EP, n_requests, concurrency, f"{rid}D")
    await send({"type": "scenario_done", "id": "D", "metrics": _metrics(lats, time.perf_counter() - t0)})

    await send({"type": "complete"})
