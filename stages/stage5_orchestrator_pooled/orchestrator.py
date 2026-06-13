import asyncio
from app.hash.hash_slot import HashSlotManager
from app.protocol_handler.protocol_handler import CommandError, ProtocolHandler, Error, Disconnect
from app.servers.transport import Endpoint, cleanup_endpoint, orchestrator_endpoint, shard_endpoint


class ShardMessageBus:
    def __init__(self, shard_count: int):
        self._protocol = ProtocolHandler()
        self._endpoints = {node: shard_endpoint(node) for node in range(shard_count)}

    async def execute(self, shard_index: int, *command_parts):
        endpoint = self._endpoints[shard_index]
        reader, writer = await self._open_connection(endpoint)

        try:
            await self._protocol.write_response(writer, list(command_parts))
            response = await self._protocol.handle_request(reader)
            if isinstance(response, Error):
                message = response.msg.decode("utf-8") if isinstance(response.msg, bytes) else str(response.msg)
                raise CommandError(message)
            return response
        finally:
            writer.close()
            await writer.wait_closed()

    async def _open_connection(self, endpoint: Endpoint):
        if endpoint.kind == "unix":
            return await asyncio.open_unix_connection(endpoint.path)
        return await asyncio.open_connection(endpoint.host, endpoint.port)


class Orchestrator:
    def __init__(self, shard_count: int, endpoint: Endpoint | None = None):
        self._shard_count = shard_count
        self._endpoint = endpoint or orchestrator_endpoint()
        self._message_bus = ShardMessageBus(shard_count)
        self._protocol = ProtocolHandler()
        self._slot_manager = HashSlotManager([(f"shard-{node}", node) for node in range(shard_count)])
        self._server: asyncio.base_events.Server | None = None

    async def run(self):
        cleanup_endpoint(self._endpoint)
        self._server = await self._start_server()
        print(f"Orchestrator serving on {self._endpoint.describe()}")

        try:
            async with self._server:
                await self._server.serve_forever()
        finally:
            cleanup_endpoint(self._endpoint)

    async def _start_server(self):
        if self._endpoint.kind == "unix":
            return await asyncio.start_unix_server(self._handle, path=self._endpoint.path)
        return await asyncio.start_server(self._handle, self._endpoint.host, self._endpoint.port)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        address = writer.get_extra_info("peername") or self._endpoint.describe()
        print(f"Orchestrator accepted connection from {address}")

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
            *(self._message_bus.execute(shard_index, "MGET", *shard_keys) for shard_index, shard_keys in shard_to_keys.items())
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
            *(self._message_bus.execute(shard_index, "MSET", *payload) for shard_index, payload in shard_to_items.items())
        )
        return sum(int(result) for result in shard_results)

    async def _route_info(self):
        shard_metrics = await asyncio.gather(
            *(self._message_bus.execute(shard_index, "INFO") for shard_index in range(self._shard_count))
        )
        return {
            "role": "orchestrator",
            "shard_count": self._shard_count,
            "client_endpoint": self._endpoint.describe(),
            "internal_transport": shard_endpoint(0).kind if self._shard_count else "unknown",
            "shards": shard_metrics,
        }