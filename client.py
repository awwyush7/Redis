import socket
import ssl
import sys
import logging
import hashlib
from app.hash.hash_slot import HashSlotManager
from app.protocol_handler.protocol_handler import Error, ProtocolHandler

# --- CONFIGURATION: Define the Shard Endpoints ---
# IMPORTANT: Ensure your servers are actually running on these ports (9090, 9091, 9092)
SERVER_ADDRESSES = [
    ('127.0.0.1', 9090),  # Shard 0
    ('127.0.0.1', 9091),  # Shard 1
    ('127.0.0.1', 9092)   # Shard 2
]
NUM_SERVERS = len(SERVER_ADDRESSES)
NUM_SLOTS = 16384 # Standard Redis Cluster slots

# Setup basic logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - CLIENT - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ====================================================================
# MODIFIED CLASS: ShardedClient
# ====================================================================

class ShardedClient(object):
    """
    Manages connections to multiple Redis-clone servers and routes commands
    using the HashSlotManager.
    """
    def __init__(self):
        self._connections = {}
        self._ph = ProtocolHandler() 
        
        # NEW: Initialize the Hash Slot Manager
        self.slot_manager = HashSlotManager(SERVER_ADDRESSES)

        # SSL Context Setup (Insecure for local testing)
        self._context = ssl.create_default_context()
        self._context.check_hostname = False
        self._context.verify_mode = ssl.CERT_NONE

        self._connect_all()

    def _connect_all(self):
        """Attempts to establish connections to all configured shards."""
        for i, (host, port) in enumerate(SERVER_ADDRESSES):
            self._connect_shard(i, host, port)

    def _connect_shard(self, shard_index, host, port):
        """Establishes a single SSL connection for a specific shard."""
        try:
            plain_socket = socket.create_connection((host, port))
            ssl_socket = self._context.wrap_socket(plain_socket, server_hostname=host)
            fh = ssl_socket.makefile('rwb')
            
            self._connections[shard_index] = {
                'socket': ssl_socket,
                'fh': fh,
                'host': host,
                'port': port,
                'ph': self._ph
            }
            logging.debug(f"Shard {shard_index} connected to {host}:{port}")
        except Exception as e:
            logging.error(f"Shard {shard_index} Connection Failed to {host}:{port}: {e}")
            self._connections[shard_index] = None

    def close(self):
        """Closes all active shard connections."""
        for conn_data in self._connections.values():
            if conn_data and conn_data['socket']:
                conn_data['socket'].close()
                
    def execute(self, *args):
        """
        Generic command execution. Routes command based on the first argument (the key).
        """
        if not args:
            raise ValueError("Command cannot be empty.")
        
        command = args[0].upper()
        
        # 1. Handle commands that go to ALL servers (e.g., FLUSH)
        if command in ('FLUSH'):
            results = []
            for i in range(NUM_SERVERS):
                results.append(self._execute_on_shard(i, *args))
            return results

        # 2. Handle commands that require a key (GET, SET, DEL, etc.)
        if len(args) < 2:
             raise ValueError(f"Command '{command}' requires a key.")
        
        key = args[1] # Key is the second argument
        
        # Use the HashSlotManager to find the correct server index
        shard_index = self.slot_manager.get_server_index_by_key(key)
        
        logging.debug(f"Key '{key}' mapped to Slot {self.slot_manager.get_slot(key)} on Shard {shard_index}")
        
        return self._execute_on_shard(shard_index, *args)

    def _execute_on_shard(self, shard_index, *args):
        """Executes the command on the specified shard."""
        conn_data = self._connections.get(shard_index)
        
        if not conn_data or not conn_data['socket']:
            # A real client would try to reconnect here
            raise Exception(f"Shard {shard_index} is unavailable.")

        fh = conn_data['fh']
        ph = conn_data['ph']
        
        try:
            # 1. Serialize & Send 
            ph.write_response(fh, list(args))
            
            # 2. Read Response
            resp = ph.handle_request(fh)

            # 3. Check for Protocol Level Errors
            if isinstance(resp, Error):
                raise Exception(f"Server Error: {resp.msg.decode() if isinstance(resp.msg, bytes) else resp.msg}")

            return resp
            
        except Exception as e:
            logging.error(f"Execution Error on Shard {shard_index}: {e}")
            # Ensure the broken connection is closed
            conn_data['socket'].close()
            self._connections[shard_index] = None # Mark connection as failed
            raise e

    # --- Command Methods (User-friendly interface) ---
    def get(self, key):
        return self.execute('GET', key)

    def set(self, key, value):
        return self.execute('SET', key, value)

    def delete(self, key):
        return self.execute('DELETE', key)

    def flush(self):
        return self.execute('FLUSH') 

    def mget(self, *keys):
        raise NotImplementedError("MGET requires splitting keys across shards and merging results.")

    def mset(self, *items):
        raise NotImplementedError("MSET requires splitting key/value pairs across shards.")


if __name__ == '__main__':
    client = ShardedClient()
    
    # Check if any connections were established before continuing
    if any(client._connections.values()): 
        print(f"\n--- Redis Client Interactive Mode (Shards: {NUM_SERVERS}, Slots: {NUM_SLOTS}) ---")
        print("Type 'QUIT' or 'EXIT' to close the connection. Use `SET key value`.")
        
        while True:
            try:
                user_input = input("redis-clone> ").strip()
                
                if not user_input or user_input.upper() in ("QUIT", "EXIT"):
                    print("Exiting interactive mode.")
                    break
                
                parts = user_input.split()
                if not parts: continue
                
                command = parts[0].upper()
                args = parts[1:]
                
                response = None
                
                # --- COMMAND EXECUTION LOGIC ---
                if command == "SET" and len(args) == 2:
                    response = client.set(args[0], args[1])
                
                elif command == "GET" and len(args) == 1:
                    response = client.get(args[0])
                
                elif command in ("DEL", "DELETE") and len(args) == 1:
                    response = client.delete(args[0])
                
                elif command == "FLUSH" and len(args) == 0:
                    response = client.flush()

                elif command in ("MGET", "MSET"):
                    print(f"(error) {command} is not implemented for sharding.")
                    continue
                
                else:
                    print(f"(error) Unknown command or wrong number of arguments for: {command}")
                    continue

                # 4. Display the response
                print(f"<- Response: {response}")
                
            except Exception as e:
                print(f"(error) An exception occurred: {e}")
                
    client.close()