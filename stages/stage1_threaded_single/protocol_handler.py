from io import BytesIO
from socket import error

class Error:
    pass

class CommandError(Exception): 
    """Raised when a client command is malformed or invalid."""
    pass

class Disconnect(Exception): 
    """Raised when a client disconnects."""
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

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            # Delegate to the appropriate handler based on the first byte.
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('bad request')

    def handle_simple_string(self, socket_file):
        print("Handling Simple String")
        simple_string = socket_file.readline().rstrip(b'\r\n')
        print(type(simple_string))
        return simple_string.decode('utf-8')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip(b'\r\n'))

    def handle_integer(self, socket_file):
        print("Handling Integer")
        return int(socket_file.readline().rstrip(b'\r\n'))

    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        # print(length)
        return socket_file.read(length)[:-2].decode('utf-8')

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        print("Handling Array")
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range(num_items * 2)]
        print("Handling Dict")
        return dict(zip(elements[::2], elements[1::2]))
    
    def write_response(self, socket_file, data):
        """
        Serialize the *entire* command as one RESP array.
        """
        buf = BytesIO()
        self._write(buf, data)  # <--- WRAP WHOLE COMMAND
        packet = buf.getvalue()
        print(packet)
        socket_file.write(packet)
        socket_file.flush()

    def _write(self, buf, data):
        # STRING → treat as bulk string
        if isinstance(data, str):
            print("Writing String")
            data = data.encode()

        # BYTES → bulk string
        if isinstance(data, bytes):
            print("Writing Bytes")
            buf.write(b"$%d\r\n" % len(data))
            buf.write(data)
            buf.write(b"\r\n")
            return
        
        # INTEGER → RESP integer
        elif isinstance(data, int):
            print("Writing Integer")
            buf.write(b":%d\r\n" % data)
        
        # LIST or TUPLE → RESP array
        elif isinstance(data, (list, tuple)):
            print("Writing List")
            buf.write(b"*%d\r\n" % len(data))
            for item in data:
                self._write(buf, item)

        elif isinstance(data, dict):
            print("Writing Dict")
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
        

