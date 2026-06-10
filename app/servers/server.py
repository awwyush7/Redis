import asyncio
import contextlib
from datetime import datetime

from app.persistence.aof_manager import AOFManager
from app.protocol_handler.protocol_handler import CommandError, ProtocolHandler, Error, Disconnect
from app.storage.storage import Storage


class Server:
    """
    One shard = one process = one asyncio event loop.
    - Handles client connections concurrently (asyncio tasks)
    - Buffers write commands in memory
    - Flushes buffered commands to disk every N seconds (background task)
    """

    def __init__(
        self,
        host: str,
        port: int,
        node: int,
        flush_interval: float = 1.0,
        aof_dir: str = "./data",
        fsync_every_flush: bool = False,
    ):
        self._host = host
        self._port = port
        self._node = node

        self._protocol = ProtocolHandler()
        self._kv = Storage(node)
        self._commands = self.get_commands()

        self._flush_interval = flush_interval
        self._aof = AOFManager(
            node=node,
            aof_dir=aof_dir,
            fsync_every_flush=fsync_every_flush,
        )

        # Runtime handles
        self._server: asyncio.base_events.Server | None = None
        self._flush_task: asyncio.Task | None = None
        self._stop_evt = asyncio.Event()

    # ----------------------------
    # Lifecycle
    # ----------------------------
    async def run(self):
        """
        Orchestrates background tasks + TCP server in the same event loop.
        """
        self._aof.load(self._kv)

        # Start periodic flush loop
        self._flush_task = asyncio.create_task(self._flush_loop(), name=f"flush-loop-shard-{self._node}")

        # Start TCP server
        self._server = await asyncio.start_server(self._handle, self._host, self._port)
        addr = self._server.sockets[0].getsockname()
        print(f"Shard {self._node} serving on {addr} (async event loop)")
        print(f"Shard {self._node} AOF => {self._aof.aof_path} | flush_interval={self._flush_interval}s")

        try:
            async with self._server:
                await self._server.serve_forever()
        except asyncio.CancelledError:
            # normal on shutdown
            raise
        finally:
            # Stop flusher and flush remaining commands
            self._stop_evt.set()

            if self._flush_task:
                self._flush_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._flush_task

            await self._aof.shutdown(self._kv)

    # ----------------------------
    # Client handling
    # ----------------------------
    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        address = writer.get_extra_info("peername")
        print(f"New connection from {address}")

        try:
            while True:
                try:
                    request = await self._protocol.handle_request(reader)

                except Disconnect:
                    break

                except CommandError as e:
                    response = Error(str(e).encode("utf-8"))

                else:
                    try:
                        response = self.get_response(request)
                    except CommandError as e:
                        response = Error(str(e).encode("utf-8"))

                await self._protocol.write_response(writer, response)

        except Exception as e:
            import traceback
            print(f"Error with {address}: {e}")
            traceback.print_exc()

        finally:
            writer.close()
            await writer.wait_closed()
            print(f"Connection with {address} closed.")

    # ----------------------------
    # Command dispatch
    # ----------------------------
    def get_commands(self):
        return {
            "GET": self.get,
            "SET": self.set,
            "DELETE": self.delete,
            "MGET": self.mget,
            "MSET": self.mset,
            "INFO": self.info,
        }

    def get_response(self, data):
        """
        Pure CPU / dict operations => sync is fine.
        IMPORTANT: keep it non-blocking (no disk, no heavy CPU).
        """
        if not isinstance(data, list):
            try:
                data = data.split()
            except Exception:
                raise CommandError("Request must be list or simple string.")

        if not data:
            raise CommandError("Missing command")

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError(f"Unrecognized command: {command}")

        response = self._commands[command](*data[1:])
        self._persist_command(command, data[1:], response)
        return response

    def _persist_command(self, command: str, args: list[str], response):
        if isinstance(response, dict) and "error" in response:
            return

        if command == "SET":
            key = args[0]
            expire_ts = self._kv.get_expire_ts(key)
            value = self._kv.get_value(key)
            if expire_ts is not None and value is not None:
                self._aof.append_set(key, value, expire_ts)
            return

        if command == "DELETE":
            self._aof.append_delete(args[0])
            return

        if command == "MSET":
            for key in args[::2]:
                expire_ts = self._kv.get_expire_ts(key)
                value = self._kv.get_value(key)
                if expire_ts is not None and value is not None:
                    self._aof.append_set(key, value, expire_ts)

    # ----------------------------
    # Storage operations
    # ----------------------------
    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        return self._kv.add(key, value, ttl_seconds=30000)

    def delete(self, key):
        return self._kv.delete(key)

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        data = list(zip(items[::2], items[1::2]))
        for key, value in data:
            self._kv.add(key, value, ttl_seconds=30000)
        return len(data)

    def info(self):
        return self._kv.get_metrics()

    # ----------------------------
    # Flushing logic
    # ----------------------------
    async def _flush_loop(self):
        """
        Wake up every flush_interval seconds and flush buffered AOF entries.
        """
        while not self._stop_evt.is_set():
            await asyncio.sleep(self._flush_interval)
            await self._flush_once()

    async def _flush_once(self):
        """
        Flush buffered entries to disk without blocking the event loop.

        Persistence manager owns write buffering and rewrite lifecycle.
        """
        await self._aof.flush_once(self._kv)


# Optional: direct run (single shard)
if __name__ == "__main__":
    # Example single-process run
    server = Server(host="127.0.0.1", port=9090, node=0)
    asyncio.run(server.run())