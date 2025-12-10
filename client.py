import socket
import ssl
import sys
import logging
from app.protocol_handler.protocol_handler import Error, ProtocolHandler


HOST = '127.0.0.1'
PORT = 9090

# Setup basic logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - CLIENT - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class Client(object):
    def __init__(self, host=HOST, port=PORT):
        self._host = host
        self._port = port
        
        # Two Handlers: One for writing requests, one for reading responses
        self._ph = ProtocolHandler() 

        self._socket = None
        self._fh = None
        
        # SSL Context Setup (Insecure for local testing)
        self._context = ssl.create_default_context()
        self._context.check_hostname = False
        self._context.verify_mode = ssl.CERT_NONE

        self._connect()

    def _connect(self):
        try:
            plain_socket = socket.create_connection((self._host, self._port))
            logging.debug(f"TCP Connected to {self._host}:{self._port}")
            self._socket = self._context.wrap_socket(plain_socket, server_hostname=self._host)
            self._fh = self._socket.makefile('rwb')
            logging.debug(f"SSL Handshake Successful.")
            return True
        except Exception as e:
            logging.error(f"Connection Failed: {e}")
            self._socket = None
            return False

    def close(self):
        if self._socket:
            self._socket.close()
            logging.debug("Connection closed.")

    def execute(self, *args):
        """
        Generic command execution. Passes raw Python objects to the writer.
        """

        if not self._fh:
            if not self._connect():
                raise Exception("Client is not connected to server.")

        try:
            # 1. Serialize & Send (The writer figures out the types)
            resp = self._ph.write_response(self._fh,[*args])
            
            resp = self._ph.handle_request(self._fh)

            # 3. Check for Protocol Level Errors
            if isinstance(resp, Error):
                raise Exception(f"Server Error: {resp.msg.decode() if isinstance(resp.msg, bytes) else resp.msg}")

            print("Response:", resp)
            return resp
            
        except Exception as e:
            logging.error(f"Execution Error: {e}")
            self.close()
            raise e

    # --- Command Methods (User-friendly interface) ---

    def get(self, key):
        return self.execute('GET',key)

    def set(self, key, value):
        # Value can be int or str, the writer handles the binary type!
        return self.execute('SET',key, value)

    def delete(self, key):
        return self.execute('DELETE',key)

    def flush(self):
        return self.execute('FLUSH')

    def mget(self, *keys):
        return self.execute('MGET',*keys)

    def mset(self, *items):
        return self.execute('MSET',*items)


if __name__ == '__main__':
    client = Client()
    
    if client._socket:
        print("\n--- Redis Client Interactive Mode ---")
        print("Type 'QUIT' or 'EXIT' to close the connection.")
        
        while True:
            try:
                # 1. Get user input (e.g., "SET key value" or "GET key")
                user_input = input("redis-clone> ").strip()
                
                if not user_input:
                    continue
                
                # Check for exit commands
                if user_input.upper() in ("QUIT", "EXIT"):
                    print("Exiting interactive mode.")
                    break
                
                # 2. Parse the command and arguments
                # This assumes simple, space-separated arguments (like standard Redis CLI)
                parts = user_input.split()
                
                if not parts:
                    continue
                
                command = parts[0].upper()
                args = parts[1:]
                
                # 3. Dynamic Command Execution
                
                # For simplicity, we'll manually handle the common commands
                # A more robust solution would use Python's built-in reflection (getattr)
                
                response = None
                
                if command == "SET" and len(args) == 2:
                    response = client.set(args[0], args[1])
                
                elif command == "GET" and len(args) == 1:
                    response = client.get(args[0])
                
                elif command == "DEL" and len(args) == 1:
                    response = client.delete(args[0])

                elif command == "mget":
                    response = client.mget(*args)
                
                # --- Add other command handlers here (e.g., LPUSH, HGET) ---
                
                else:
                    # If command is not recognized or arguments are wrong
                    print(f"(error) Unknown command or wrong number of arguments for: {command}")
                    continue

                # 4. Display the response
                # Note: The output format (e.g., 'b'OK') depends on your client's return type.
                print(f"<- Response: {response}")
                
            except EOFError:
                # Handle Ctrl+D (end of file)
                print("\nExiting interactive mode.")
                break
            except Exception as e:
                # Handle connection errors, protocol errors, etc.
                print(f"(error) An exception occurred: {e}")
                
    client.close()