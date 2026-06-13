"""
Async Redis client — connects to the orchestrator using asyncio streams.

Key differences vs. client.py (the sync version):
--------------------------------------------------
1. `asyncio.open_connection` / `asyncio.open_unix_connection` instead of
   blocking `socket.create_connection`.  The event loop is never blocked
   waiting for the server to respond; other coroutines can run while we wait.

2. All command methods are `async def` and must be awaited.

3. The interactive loop uses `loop.run_in_executor(None, input, prompt)` to
   push the blocking `input()` call to a thread pool.  This keeps the event
   loop free between keystrokes — important when you want to fire concurrent
   requests from other tasks in the same loop.

4. The connection is kept alive across commands (same as the sync client),
   but if it drops the client reconnects automatically on the next execute().
"""

import asyncio
import sys
import logging

from app.protocol_handler.protocol_handler import Error, ProtocolHandler
from app.servers.transport import orchestrator_endpoint

ORCHESTRATOR_ENDPOINT = orchestrator_endpoint()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - ASYNC-CLIENT - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


class AsyncShardedClient:
    """
    Async version of ShardedClient.

    Maintains one persistent, non-blocking asyncio stream connection to the
    orchestrator.  All I/O is cooperative — awaiting a response yields
    control back to the event loop so other tasks can run concurrently.
    """

    def __init__(self):
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._ph = ProtocolHandler()

    async def connect(self):
        """Open the asyncio stream connection to the orchestrator."""
        try:
            if ORCHESTRATOR_ENDPOINT.kind == "unix":
                # Unix domain socket — only available on Linux/macOS.
                self._reader, self._writer = await asyncio.open_unix_connection(
                    ORCHESTRATOR_ENDPOINT.path
                )
            else:
                # TCP — used on Windows and any platform.
                # asyncio.open_connection wraps the raw socket in non-blocking
                # StreamReader / StreamWriter objects.
                self._reader, self._writer = await asyncio.open_connection(
                    ORCHESTRATOR_ENDPOINT.host, ORCHESTRATOR_ENDPOINT.port
                )
            logging.debug(f"Connected to orchestrator at {ORCHESTRATOR_ENDPOINT.describe()}")
        except Exception as e:
            logging.error(f"Connection failed: {e}")
            self._reader = self._writer = None

    async def close(self):
        """Gracefully close the stream."""
        if self._writer and not self._writer.is_closing():
            self._writer.close()
            await self._writer.wait_closed()
        self._reader = self._writer = None

    async def execute(self, *args):
        """
        Send a command to the orchestrator and await the response.

        If the connection is closed (e.g. orchestrator restarted) this method
        will attempt one reconnect before raising.
        """
        if not self._writer or self._writer.is_closing():
            await self.connect()

        if not self._writer:
            raise ConnectionError("Orchestrator is unavailable.")

        try:
            # Serialize the command as a RESP array and flush it.
            await self._ph.write_response(self._writer, list(args))
            # Await the response — the event loop is free until data arrives.
            resp = await self._ph.handle_request(self._reader)

            if isinstance(resp, Error):
                msg = resp.msg.decode() if isinstance(resp.msg, bytes) else str(resp.msg)
                raise Exception(f"Server Error: {msg}")

            return resp

        except Exception as e:
            logging.error(f"Execution error: {e}")
            await self.close()
            raise

    # ---- Command interface (mirrors the sync client) ----

    async def get(self, key):
        return await self.execute("GET", key)

    async def set(self, key, value):
        return await self.execute("SET", key, value)

    async def delete(self, key):
        return await self.execute("DELETE", key)

    async def mget(self, *keys):
        return await self.execute("MGET", *keys)

    async def mset(self, *items):
        return await self.execute("MSET", *items)

    async def info(self):
        return await self.execute("INFO")


async def main():
    client = AsyncShardedClient()
    await client.connect()

    if not client._writer:
        print("Could not connect to orchestrator. Is the server running?")
        return

    # loop.run_in_executor: runs blocking input() in a thread pool so the
    # event loop is not stuck waiting for the user to press Enter.
    loop = asyncio.get_event_loop()

    print(f"\n--- Async Redis Client (Front door: {ORCHESTRATOR_ENDPOINT.describe()}) ---")
    print("Type 'QUIT' or 'EXIT' to close. Commands: GET SET DELETE MGET MSET INFO")

    try:
        while True:
            try:
                user_input = await loop.run_in_executor(None, input, "redis-async> ")
                user_input = user_input.strip()
            except EOFError:
                break

            if not user_input or user_input.upper() in ("QUIT", "EXIT"):
                print("Exiting.")
                break

            parts = user_input.split()
            if not parts:
                continue

            command = parts[0].upper()
            args = parts[1:]

            try:
                if command == "SET" and len(args) == 2:
                    response = await client.set(args[0], args[1])

                elif command == "GET" and len(args) == 1:
                    response = await client.get(args[0])

                elif command in ("DEL", "DELETE") and len(args) == 1:
                    response = await client.delete(args[0])

                elif command == "MGET" and len(args) >= 1:
                    response = await client.mget(*args)

                elif command == "MSET" and len(args) >= 2 and len(args) % 2 == 0:
                    response = await client.mset(*args)

                elif command == "INFO" and not args:
                    response = await client.info()
                    if isinstance(response, dict) and isinstance(response.get("shards"), list):
                        print(f"\n=== Orchestrator Metrics ===")
                        print(f"   shard_count:        {response.get('shard_count')}")
                        print(f"   client_endpoint:    {response.get('client_endpoint')}")
                        print(f"   internal_transport: {response.get('internal_transport')}")
                        for metrics in response["shards"]:
                            print(f"\n=== Shard {metrics.get('shard_id', '?')} Metrics ===")
                            for k, v in metrics.items():
                                print(f"   {k}: {v}")
                        continue

                else:
                    print(f"(error) Unknown command or wrong number of arguments for: {command}")
                    continue

                print(f"<- Response: {response}")

            except Exception as e:
                print(f"(error) {e}")

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())