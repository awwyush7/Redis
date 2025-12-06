import socket
import ssl
import threading
from app.protocol_handler.protocol_handler import CommandError, ProtocolHandler
from storage.storage import Storage
import threading

class Error: pass
class Disconnect(Exception): pass

class Server(object):
    def __init__(self, host='127.0.0.1', port=9090, max_clients=64):
        self._pool = max_clients
        self._host = host
        self._port = port

        self._context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self._context.load_cert_chain(certfile="app/server/server.crt", keyfile="app/server/server.key")

        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.bind((self._host,self._port))
        self._server.listen(self._pool)

        self._protocol = ProtocolHandler()
        self._kv = Storage()

        self._commands = self.get_commands()

        self.start()
    
    # --- Add this method to your Server class ---

    def _handle(self, communication_socket, address):
        """
        Handles the client communication loop, reading requests and writing responses.
        """
        # 1. Wrap the socket for the ProtocolHandler
        # We use makefile() to get a file-like object from the socket.
        # This is crucial because ProtocolHandler uses readline() and read().
        socket_file = communication_socket.makefile('rwb')

        try:
            while True:
                try:
                    # DESERIALIZATION: ProtocolHandler reads bytes from the socket_file,
                    # determines the data type, and converts it into a Python list (the command).
                    request = self._protocol.handle_request(socket_file)
                
                except Disconnect:
                    # Client closed the connection
                    break
                
                except CommandError as e:
                    # Client sent a malformed request
                    response = Error(str(e).encode('utf-8'))
                
                else:
                    try:
                        # EXECUTION: Get the response by dispatching the command to the Storage layer
                        response = self.get_response(request)
                    except CommandError as e:
                        # Command syntax error (e.g., MGET with wrong number of args)
                        response = Error(str(e).encode('utf-8'))
                    
                # SERIALIZATION: ProtocolHandler takes the Python object (response)
                self._protocol.write_response(socket_file, response)

        except socket.error as e:
            # Catch socket level errors (e.g., connection reset)
            print(f"Socket error with {address}: {e}")
            
        finally:
            # Ensure the connection is closed
            communication_socket.close()
            print(f"Connection with {address} closed.")

    def start(self):
        while True:
            print("Server Listening")
            # 1. Accept the plain TCP connection
            plain_socket, address = self._server.accept()
            print(f"Communication from {address} accepted")
            
            # 2. WRAP the individual communication socket with SSL/TLS
            # This is where the TLS handshake happens, and the new secure socket is created.
            communication_socket = self._context.wrap_socket(plain_socket, server_side=True)
            
            # 3. Proceed with the secure socket
            thread = threading.Thread(
                target=self._handle, 
                args=(communication_socket, address), 
                daemon=True
            )
            thread.start()

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            # 'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset}

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
        data = zip(items[::2], items[1::2])
        for key, value in data:
            self._kv.add(key,value,ttl_seconds=30000)
        return len(data)
    
if __name__ == '__main__':
    server = Server()