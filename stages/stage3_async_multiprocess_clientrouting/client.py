import socket
import sys
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.hash.hash_slot import HashSlotManager
from app.protocol_handler.protocol_handler import Error, ProtocolHandler

# --- CONFIGURATION: Define the Shard Endpoints ---
# IMPORTANT: Ensure your servers are actually running on these ports (9090, 9091, 9092)
SERVER_ADDRESSES = [
      ('127.0.0.1', 9090),   # Shard 0
      ('127.0.0.1', 9091),   # Shard 1
      ('127.0.0.1', 9092)    # Shard 2
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
            
            # Initialize the Hash Slot Manager
            self.slot_manager = HashSlotManager(SERVER_ADDRESSES)

            # Removed SSL - using plain TCP for async simplicity
            self._connect_all()

      def _connect_all(self):
            """Attempts to establish connections to all configured shards."""
            for i, (host, port) in enumerate(SERVER_ADDRESSES):
                  self._connect_shard(i, host, port)

      def _connect_shard(self, shard_index, host, port):
            """Establishes a single TCP connection for a specific shard (no SSL)."""
            try:
                  plain_socket = socket.create_connection((host, port))
                  fh = plain_socket.makefile('rwb')
                  
                  self._connections[shard_index] = {
                        'socket': plain_socket,
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
                  ph.write_response_sync(fh, list(args))
                  
                  # 2. Read Response
                  resp = ph.handle_request_sync(fh)

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
      
      def info(self, shard_index=None):
            """
            Get metrics from a specific shard or all shards.
            If shard_index is None, query all shards.
            """
            if shard_index is not None:
                  return self._execute_on_shard(shard_index, 'INFO')
            else:
                  # Get metrics from all shards
                  all_metrics = []
                  for i in range(NUM_SERVERS):
                        try:
                              metrics = self._execute_on_shard(i, 'INFO')
                              all_metrics.append(metrics)
                        except Exception as e:
                              logging.error(f"Failed to get INFO from shard {i}: {e}")
                  return all_metrics 

      def mget(self, *keys):
            """
            Multi-GET across shards using parallel execution.
            Partitions keys by shard, dispatches in parallel, merges results.
            """
            if not keys:
                  return []
            
            # 1. Group keys by shard
            shard_to_keys = {}
            key_to_shard = {}
            for key in keys:
                  shard_idx = self.slot_manager.get_server_index_by_key(key)
                  key_to_shard[key] = shard_idx
                  if shard_idx not in shard_to_keys:
                        shard_to_keys[shard_idx] = []
                  shard_to_keys[shard_idx].append(key)
            
            logging.debug(f"MGET partitioned across {len(shard_to_keys)} shards")
            
            # 2. Dispatch GET requests to each shard in parallel
            results = {}
            with ThreadPoolExecutor(max_workers=len(shard_to_keys)) as executor:
                  future_to_shard = {
                        executor.submit(self._mget_from_shard, shard, shard_keys): shard
                        for shard, shard_keys in shard_to_keys.items()
                  }
                  
                  for future in as_completed(future_to_shard):
                        try:
                              shard_results = future.result()
                              results.update(shard_results)
                        except Exception as e:
                              logging.error(f"MGET shard error: {e}")
            
            # 3. Return results in original key order
            return [results.get(key, None) for key in keys]

      def _mget_from_shard(self, shard_idx, keys):
            """Execute MGET on a single shard and return dict of key:value"""
            try:
                  response = self._execute_on_shard(shard_idx, 'MGET', *keys)
                  # Server returns list of values in same order as keys
                  if isinstance(response, list):
                        return dict(zip(keys, response))
                  else:
                        # Fallback: single key GET
                        return {keys[0]: response}
            except Exception as e:
                  logging.error(f"_mget_from_shard error: {e}")
                  return {key: None for key in keys}

      def mset(self, *items):
            """
            Multi-SET across shards using parallel execution.
            Takes alternating key, value pairs: mset(key1, val1, key2, val2, ...)
            """
            if len(items) % 2 != 0:
                  raise ValueError("MSET requires an even number of arguments (key-value pairs)")
            
            # 1. Parse key-value pairs and group by shard
            pairs = list(zip(items[::2], items[1::2]))
            shard_to_pairs = {}
            
            for key, value in pairs:
                  shard_idx = self.slot_manager.get_server_index_by_key(key)
                  if shard_idx not in shard_to_pairs:
                        shard_to_pairs[shard_idx] = []
                  shard_to_pairs[shard_idx].append((key, value))
            
            logging.debug(f"MSET partitioned across {len(shard_to_pairs)} shards")
            
            # 2. Dispatch SET requests to each shard in parallel
            success_count = 0
            with ThreadPoolExecutor(max_workers=len(shard_to_pairs)) as executor:
                  future_to_shard = {
                        executor.submit(self._mset_to_shard, shard, shard_pairs): shard
                        for shard, shard_pairs in shard_to_pairs.items()
                  }
                  
                  for future in as_completed(future_to_shard):
                        try:
                              count = future.result()
                              success_count += count
                        except Exception as e:
                              logging.error(f"MSET shard error: {e}")
            
            return success_count

      def _mset_to_shard(self, shard_idx, pairs):
            """Execute MSET on a single shard and return count of successful sets"""
            try:
                  # Flatten pairs to alternating key, value, key, value...
                  items = [item for pair in pairs for item in pair]
                  response = self._execute_on_shard(shard_idx, 'MSET', *items)
                  
                  # Server should return count or success indicator
                  if isinstance(response, int):
                        return response
                  elif isinstance(response, dict) and response.get('Status') == 'Done':
                        return len(pairs)
                  else:
                        return len(pairs)   # Assume success
            except Exception as e:
                  logging.error(f"_mset_to_shard error: {e}")
                  return 0


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
                              print(f"Val: ${args[0]} Type: ${type(args[0])}")
                              print(f"Val: ${args[1]} Type: ${type(args[1])}")
                              response = client.set(args[0], args[1])
                        
                        elif command == "GET" and len(args) == 1:
                              print(f"Val: ${args[0]} Type: ${type(args[0])}")
                              response = client.get(args[0])
                        
                        elif command in ("DEL", "DELETE") and len(args) == 1:
                              print(f"Val: ${args[0]} Type: ${type(args[0])}")
                              response = client.delete(args[0])
                        
                        elif command == "FLUSH" and len(args) == 0:
                              response = client.flush()

                        elif command == "MGET" and len(args) >= 1:
                              print(f"MGET keys: {args}")
                              response = client.mget(*args)
                        
                        elif command == "MSET" and len(args) >= 2 and len(args) % 2 == 0:
                              print(f"MSET pairs: {args}")
                              response = client.mset(*args)
                        
                        elif command == "INFO" and len(args) == 0:
                              response = client.info()
                              # Pretty print metrics
                              if isinstance(response, list):
                                    for metrics in response:
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