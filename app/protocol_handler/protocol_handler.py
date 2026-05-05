from io import BytesIO
import asyncio

class Error:
    def __init__(self, msg):
        self.msg = msg

class CommandError(Exception): 
    """Raised when a client command is malformed or invalid."""
    pass

class Disconnect(Exception): 
    pass

class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict}

    async def handle_request(self, reader):
        """
        Async version: reads from asyncio.StreamReader
        """
        first_byte = await reader.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            # Delegate to the appropriate handler based on the first byte.
            return await self.handlers[first_byte](reader)
        except KeyError:
            raise CommandError('bad request')

    async def handle_simple_string(self, reader):
        simple_string = await reader.readline()
        simple_string = simple_string.rstrip(b'\r\n')
        return simple_string.decode('utf-8')

    async def handle_error(self, reader):
        line = await reader.readline()
        return Error(line.rstrip(b'\r\n'))

    async def handle_integer(self, reader):
        line = await reader.readline()
        return int(line.rstrip(b'\r\n'))

    async def handle_string(self, reader):
        line = await reader.readline()
        length = int(line.rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        data = await reader.read(length)
        return data[:-2].decode('utf-8')

    async def handle_array(self, reader):
        line = await reader.readline()
        num_elements = int(line.rstrip(b'\r\n'))
        return [await self.handle_request(reader) for _ in range(num_elements)]

    async def handle_dict(self, reader):
        line = await reader.readline()
        num_items = int(line.rstrip(b'\r\n'))
        elements = [await self.handle_request(reader)
                    for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))
    
    async def write_response(self, writer, data):
        """
        Async version: writes to asyncio.StreamWriter
        Serialize the *entire* command as one RESP array.
        """
        buf = BytesIO()
        self._write(buf, data)
        packet = buf.getvalue()
        writer.write(packet)
        await writer.drain()  # Ensure data is sent

    def _write(self, buf, data):
        # STRING → treat as bulk string
        if isinstance(data, str):
            data = data.encode()

        # BYTES → bulk string
        if isinstance(data, bytes):
            buf.write(b"$%d\r\n" % len(data))
            buf.write(data)
            buf.write(b"\r\n")
            return
        
        # INTEGER → RESP integer
        elif isinstance(data, int):
            buf.write(b":%d\r\n" % data)
        
        # LIST or TUPLE → RESP array
        elif isinstance(data, (list, tuple)):
            buf.write(b"*%d\r\n" % len(data))
            for item in data:
                self._write(buf, item)

        elif isinstance(data, dict):
            buf.write(b"%%%d\r\n" % len(data))
            for k, v in data.items():
                self._write(buf, k)
                self._write(buf, v)

        elif data is None:
            buf.write(b"$-1\r\n")

        elif isinstance(data, Error):
            msg = data.msg.encode() if isinstance(data.msg, str) else data.msg
            buf.write(b"-" + msg + b"\r\n")

        else:
            raise ValueError(f"Unserializable type: {type(data)}")
        
    # ===================================================================
    # SYNC METHODS FOR CLIENT USE (works with file-like socket objects)
    # ===================================================================
    
    def handle_request_sync(self, fh):
        """
        Synchronous version: reads from file-like object (socket.makefile())
        Used by the client.
        """
        first_byte = fh.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            # Map first byte to sync handler
            sync_handlers = {
                b'+': self.handle_simple_string_sync,
                b'-': self.handle_error_sync,
                b':': self.handle_integer_sync,
                b'$': self.handle_string_sync,
                b'*': self.handle_array_sync,
                b'%': self.handle_dict_sync
            }
            return sync_handlers[first_byte](fh)
        except KeyError:
            raise CommandError('bad request')

    def handle_simple_string_sync(self, fh):
        simple_string = fh.readline()
        simple_string = simple_string.rstrip(b'\r\n')
        return simple_string.decode('utf-8')

    def handle_error_sync(self, fh):
        line = fh.readline()
        return Error(line.rstrip(b'\r\n'))

    def handle_integer_sync(self, fh):
        line = fh.readline()
        return int(line.rstrip(b'\r\n'))

    def handle_string_sync(self, fh):
        line = fh.readline()
        length = int(line.rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        data = fh.read(length)
        return data[:-2].decode('utf-8')

    def handle_array_sync(self, fh):
        line = fh.readline()
        num_elements = int(line.rstrip(b'\r\n'))
        return [self.handle_request_sync(fh) for _ in range(num_elements)]

    def handle_dict_sync(self, fh):
        line = fh.readline()
        num_items = int(line.rstrip(b'\r\n'))
        elements = [self.handle_request_sync(fh) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))
    
    def write_response_sync(self, fh, data):
        """
        Synchronous version: writes to file-like object (socket.makefile())
        Used by the client.
        """
        buf = BytesIO()
        self._write(buf, data)
        packet = buf.getvalue()
        fh.write(packet)
        fh.flush()