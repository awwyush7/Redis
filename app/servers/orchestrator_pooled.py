import asyncio
import contextlib

from app.hash.hash_slot import HashSlotManager
from app.protocol_handler.protocol_handler import CommandError, ProtocolHandler, Error, Disconnect
from app.servers.transport import Endpoint, cleanup_endpoint, orchestrator_endpoint, shard_endpoint


class ShardConnectionPool:
    """
    A per-shard async connection pool.

    Keeps up to `max_size` live (reader, writer) pairs open to one shard.
    Each connection() call leases exactly one pair for the duration of one
    request, then returns it to the pool (or discards it if it is broken).

    Why a LIFO queue?
    -----------------
    A recently-returned connection is at the top. Reusing it immediately
    keeps that TCP socket warm in the kernel's send/receive buffers, which
    is marginally cheaper than a connection that has been idle at the bottom
    of the queue.

    Thread safety note
    ------------------
    asyncio is single-threaded and cooperative. There is no await between
    the `_total < _max_size` check and the `_total += 1` increment inside
    `_acquire`, so no other coroutine can interleave — no lock is needed.
    """

    def __init__(self, endpoint: Endpoint, max_size: int = 5):
        self._endpoint = endpoint
        self._max_size = max_size
        # LifoQueue: most-recently-returned connection is served first.
        self._available: asyncio.LifoQueue = asyncio.LifoQueue()
        self._total = 0  # connections created (idle + currently leased)

    @contextlib.asynccontextmanager
    async def connection(self):
        """
        Lease one connection from the pool.

        Usage:
            async with pool.connection() as (reader, writer):
                ...

        On a clean exit the connection is returned to the pool.
        On any exception the connection is discarded (it may be in an
        inconsistent protocol state) and the pool size decrements.
        """
        reader, writer = await self._acquire()
        try:
            yield reader, writer
        except Exception:
            # Discard — we don't know what state the stream is in.
            if not writer.is_closing():
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
            self._total -= 1
            raise
        else:
            if writer.is_closing():
                # Shard closed the connection on its end.
                self._total -= 1
            else:
                # Healthy — return to pool for reuse.
                self._available.put_nowait((reader, writer))

    async def _acquire(self):
        # 1. Try an idle connection already in the pool.
        try:
            reader, writer = self._available.get_nowait()
            if not writer.is_closing():
                return reader, writer
            # Stale — the shard closed it while it was idle.
            self._total -= 1
        except asyncio.QueueEmpty:
            pass

        # 2. Create a new connection if we haven't hit the cap.
        #    No await between check and increment → safe in asyncio.
        if self._total < self._max_size:
            self._total += 1
            try:
                return await self._open()
            except Exception:
                self._total -= 1
                raise

        # 3. Pool is at capacity — wait for a connection to be returned.
        reader, writer = await self._available.get()
        if writer.is_closing():
            self._total -= 1
            return await self._acquire()  # retry; _total dropped so step 2 fires
        return reader, writer

    async def _open(self):
        if self._endpoint.kind == "unix":
            return await asyncio.open_unix_connection(self._endpoint.path)
        return await asyncio.open_connection(self._endpoint.host, self._endpoint.port)

    async def close_all(self):
        """Drain and close every idle connection in the pool."""
        while True:
            try:
                _reader, writer = self._available.get_nowait()
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
            except asyncio.QueueEmpty:
                break
        self._total = 0


class PooledShardMessageBus:
    """
    Same execute() interface as ShardMessageBus, but backed by a
    ShardConnectionPool per shard.

    What changes vs. the original:
    - Connections are NOT opened and closed on every forwarded command.
    - Each shard has a pool of up to `pool_size` persistent connections.
    - Under load, concurrent requests to the same shard each get their own
      leased connection from the pool, so they never wait on each other
      unless the pool is exhausted.
    """

    def __init__(self, shard_count: int, pool_size: int = 5):
        self._protocol = ProtocolHandler()
        self._pools = {
            node: ShardConnectionPool(shard_endpoint(node), max_size=pool_size)
            for node in range(shard_count)
        }

    async def execute(self, shard_index: int, *command_parts):
        pool = self._pools[shard_index]
        async with pool.connection() as (reader, writer):
            await self._protocol.write_response(writer, list(command_parts))
            response = await self._protocol.handle_request(reader)
            if isinstance(response, Error):
                message = response.msg.decode("utf-8") if isinstance(response.msg, bytes) else str(response.msg)
                raise CommandError(message)
            return response

    async def close_all(self):
        for pool in self._pools.values():
            await pool.close_all()


class OrchestratorPooled:
    """
    Drop-in replacement for Orchestrator that uses PooledShardMessageBus.

    To use instead of the original, swap the import in start_servers.py:

        from app.servers.orchestrator_pooled import OrchestratorPooled
        asyncio.run(OrchestratorPooled(shard_count=len(server_processes)).run())

    pool_size controls how many connections per shard are kept alive.
    With 3 shards and pool_size=5 you have at most 15 open TCP connections
    at any time between the orchestrator and the shards.
    """

    def __init__(self, shard_count: int, endpoint: Endpoint | None = None, pool_size: int = 5):
        self._shard_count = shard_count
        self._endpoint = endpoint or orchestrator_endpoint()
        self._message_bus = PooledShardMessageBus(shard_count, pool_size=pool_size)
        self._protocol = ProtocolHandler()
        self._slot_manager = HashSlotManager([(f"shard-{node}", node) for node in range(shard_count)])
        self._server: asyncio.base_events.Server | None = None

    async def run(self):
        cleanup_endpoint(self._endpoint)
        self._server = await self._start_server()
        print(f"Orchestrator (pooled, pool_size={self._message_bus._pools[0]._max_size}) "
              f"serving on {self._endpoint.describe()}")

        try:
            async with self._server:
                await self._server.serve_forever()
        finally:
            cleanup_endpoint(self._endpoint)
            await self._message_bus.close_all()

    async def _start_server(self):
        if self._endpoint.kind == "unix":
            return await asyncio.start_unix_server(self._handle, path=self._endpoint.path)
        return await asyncio.start_server(self._handle, self._endpoint.host, self._endpoint.port)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        address = writer.get_extra_info("peername") or self._endpoint.describe()
        print(f"Orchestrator (pooled) accepted connection from {address}")

        try:
            while True:
                try:
                    request = await self._protocol.handle_request(reader)
                except Disconnect:
                    break
                except CommandError as exc:
                    response = Error(str(exc).encode("utf-8"))
                else:
                    try:
                        response = await self._dispatch(request)
                    except CommandError as exc:
                        response = Error(str(exc).encode("utf-8"))

                await self._protocol.write_response(writer, response)
        finally:
            writer.close()
            await writer.wait_closed()

    async def _dispatch(self, request):
        if not isinstance(request, list):
            raise CommandError("Request must be a RESP array.")

        if not request:
            raise CommandError("Missing command")

        command = str(request[0]).upper()
        args = [str(item) for item in request[1:]]

        if command in {"GET", "SET", "DELETE"}:
            return await self._route_single_key(command, args)
        if command == "MGET":
            return await self._route_mget(args)
        if command == "MSET":
            return await self._route_mset(args)
        if command == "INFO":
            return await self._route_info()

        raise CommandError(f"Unsupported command at orchestrator: {command}")

    async def _route_single_key(self, command: str, args: list[str]):
        if not args:
            raise CommandError(f"{command} requires a key")

        shard_index = self._slot_manager.get_server_index_by_key(args[0])
        return await self._message_bus.execute(shard_index, command, *args)

    async def _route_mget(self, keys: list[str]):
        if not keys:
            return []

        shard_to_keys: dict[int, list[str]] = {}
        for key in keys:
            shard_index = self._slot_manager.get_server_index_by_key(key)
            shard_to_keys.setdefault(shard_index, []).append(key)

        shard_results = await asyncio.gather(
            *(self._message_bus.execute(shard_index, "MGET", *shard_keys)
              for shard_index, shard_keys in shard_to_keys.items())
        )

        merged = {}
        for (shard_index, shard_keys), values in zip(shard_to_keys.items(), shard_results):
            if not isinstance(values, list):
                raise CommandError(f"Shard {shard_index} returned invalid MGET response")
            merged.update(zip(shard_keys, values))

        return [merged.get(key) for key in keys]

    async def _route_mset(self, items: list[str]):
        if len(items) % 2 != 0:
            raise CommandError("MSET requires an even number of arguments")

        if not items:
            return 0

        shard_to_items: dict[int, list[str]] = {}
        for key, value in zip(items[::2], items[1::2]):
            shard_index = self._slot_manager.get_server_index_by_key(key)
            shard_to_items.setdefault(shard_index, []).extend([key, value])

        shard_results = await asyncio.gather(
            *(self._message_bus.execute(shard_index, "MSET", *payload)
              for shard_index, payload in shard_to_items.items())
        )
        return sum(int(result) for result in shard_results)

    async def _route_info(self):
        shard_metrics = await asyncio.gather(
            *(self._message_bus.execute(shard_index, "INFO")
              for shard_index in range(self._shard_count))
        )
        return {
            "role": "orchestrator-pooled",
            "shard_count": self._shard_count,
            "client_endpoint": self._endpoint.describe(),
            "internal_transport": shard_endpoint(0).kind if self._shard_count else "unknown",
            "shards": shard_metrics,
        }