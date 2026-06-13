import asyncio
from app.protocol_handler.protocol_handler import CommandError, ProtocolHandler, Error, Disconnect
from app.storage.storage import Storage

class Server():
    def __init__(self, host, port, node):
        self._host = host
        self._port = port
        self._node = node

        self._protocol = ProtocolHandler()
        self._kv = Storage(node)

        self._commands = self.get_commands()

        # Start the async server
        asyncio.run(self.start())
    
    async def _handle(self, reader, writer):
        """
        Async coroutine that handles a single client connection.
        Multiple of these run concurrently in the event loop.
        """
        address = writer.get_extra_info('peername')
        print(f"New connection from {address}")

        try:
            while True:
                try:
                    # DESERIALIZATION: Read and parse the request
                    request = await self._protocol.handle_request(reader)
                
                except Disconnect:
                    # Client closed the connection gracefully
                    break
                
                except CommandError as e:
                    # Client sent a malformed request
                    response = Error(str(e).encode('utf-8'))
                
                else:
                    try:
                        # EXECUTION: Get the response by dispatching to Storage
                        response = self.get_response(request)
                    except CommandError as e:
                        # Command syntax error
                        response = Error(str(e).encode('utf-8'))
                    
                # SERIALIZATION: Write the response back to client
                await self._protocol.write_response(writer, response)

        except Exception as e:
            import traceback
            print(f"Error with {address}: {e}")
            print(f"Exception type: {type(e).__name__}")
            print(f"Traceback:")
            traceback.print_exc()
            
        finally:
            # Ensure the connection is closed
            writer.close()
            await writer.wait_closed()
            print(f"Connection with {address} closed.")

    async def start(self):
        """
        Start the async server. This method runs the event loop.
        Each client connection spawns a new coroutine (_handle).
        """
        server = await asyncio.start_server(
            self._handle, 
            self._host, 
            self._port
        )
        
        addr = server.sockets[0].getsockname()
        print(f"Shard {self._node} serving on {addr} (async event loop)")

        async with server:
            await server.serve_forever()

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'MGET': self.mget,
            'MSET': self.mset,
            'INFO': self.info}

    def get_response(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be list or simple string.')

        if not data:
            raise CommandError('Missing command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)

        return self._commands[command](*data[1:])
    
    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        return self._kv.add(key,value,ttl_seconds=30000) 

    def delete(self, key):
        return self._kv.delete(key)

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        data = list(zip(items[::2], items[1::2]))
        for key, value in data:
            self._kv.add(key,value,ttl_seconds=30000)
        return len(data)
    
    def info(self):
        """Return shard metrics"""
        return self._kv.get_metrics()
    
if __name__ == '__main__':
    server = Server()