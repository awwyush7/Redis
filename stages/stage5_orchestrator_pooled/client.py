import socket
import sys
import logging
from app.protocol_handler.protocol_handler import Error, ProtocolHandler
from app.servers.transport import orchestrator_endpoint

ORCHESTRATOR_ENDPOINT = orchestrator_endpoint()

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
      Connects to a single orchestrator endpoint. The orchestrator owns shard
      routing so clients stay unaware of shard topology.
      """
      def __init__(self):
            self._socket = None
            self._fh = None
            self._ph = ProtocolHandler() 
            self._connect()

      def _connect(self):
            """Establishes the client connection to the orchestrator."""
            try:
                  if ORCHESTRATOR_ENDPOINT.kind == 'unix':
                        plain_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                        plain_socket.connect(ORCHESTRATOR_ENDPOINT.path)
                  else:
                        plain_socket = socket.create_connection((ORCHESTRATOR_ENDPOINT.host, ORCHESTRATOR_ENDPOINT.port))

                  self._socket = plain_socket
                  self._fh = plain_socket.makefile('rwb')
                  logging.debug(f"Connected to orchestrator at {ORCHESTRATOR_ENDPOINT.describe()}")
            except Exception as e:
                  logging.error(f"Orchestrator connection failed: {e}")
                  self._socket = None
                  self._fh = None

      def close(self):
            """Closes the active orchestrator connection."""
            if self._fh:
                  self._fh.close()
                  self._fh = None
            if self._socket:
                  self._socket.close()
                  self._socket = None
                        
      def execute(self, *args):
            """
            Generic command execution through the orchestrator.
            """
            if not args:
                  raise ValueError("Command cannot be empty.")

            if not self._socket or not self._fh:
                  self._connect()

            if not self._socket or not self._fh:
                  raise Exception("Orchestrator is unavailable.")
            
            try:
                  self._ph.write_response_sync(self._fh, list(args))
                  
                  resp = self._ph.handle_request_sync(self._fh)

                  if isinstance(resp, Error):
                        raise Exception(f"Server Error: {resp.msg.decode() if isinstance(resp.msg, bytes) else resp.msg}")

                  return resp
                  
            except Exception as e:
                  logging.error(f"Execution Error via orchestrator: {e}")
                  self.close()
                  raise e

      # --- Command Methods (User-friendly interface) ---
      def get(self, key):
            return self.execute('GET', key)

      def set(self, key, value):
            return self.execute('SET', key, value)

      def delete(self, key):
            return self.execute('DELETE', key)

      def mget(self, *keys):
            return self.execute('MGET', *keys)

      def mset(self, *items):
            return self.execute('MSET', *items)

      def info(self):
            return self.execute('INFO')


if __name__ == '__main__':
      client = ShardedClient()
      
      if client._socket:
            print(f"\n--- Redis Client Interactive Mode (Front door: {ORCHESTRATOR_ENDPOINT.describe()}) ---")
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
                              print(f"Val: ${args[0]} Type: ${type(args[0])}")
                              print(f"Val: ${args[1]} Type: ${type(args[1])}")
                              response = client.set(args[0], args[1])
                        
                        elif command == "GET" and len(args) == 1:
                              print(f"Val: ${args[0]} Type: ${type(args[0])}")
                              response = client.get(args[0])
                        
                        elif command in ("DEL", "DELETE") and len(args) == 1:
                              print(f"Val: ${args[0]} Type: ${type(args[0])}")
                              response = client.delete(args[0])

                        elif command == "MGET" and len(args) >= 1:
                              print(f"MGET keys: {args}")
                              response = client.mget(*args)
                        
                        elif command == "MSET" and len(args) >= 2 and len(args) % 2 == 0:
                              print(f"MSET pairs: {args}")
                              response = client.mset(*args)
                        
                        elif command == "INFO" and len(args) == 0:
                              response = client.info()
                              if isinstance(response, dict) and isinstance(response.get('shards'), list):
                                    print(f"\n=== Orchestrator Metrics ===")
                                    print(f"   shard_count: {response.get('shard_count')}")
                                    print(f"   client_endpoint: {response.get('client_endpoint')}")
                                    print(f"   internal_transport: {response.get('internal_transport')}")
                                    for metrics in response['shards']:
                                          print(f"\n=== Shard {metrics.get('shard_id', '?')} Metrics ===")
                                          for key, value in metrics.items():
                                                print(f"   {key}: {value}")
                                    continue
                        
                        else:
                              print(f"(error) Unknown command or wrong number of arguments for: {command}")
                              continue

                        # 4. Display the response
                        print(f"<- Response: {response}")
                        
                  except Exception as e:
                        print(f"(error) An exception occurred: {e}")
                        
      client.close()
