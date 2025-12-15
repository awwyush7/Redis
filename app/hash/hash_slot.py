# --- CONFIGURATION: Define the Shard Endpoints ---
# IMPORTANT: Ensure your servers are actually running on these ports (9090, 9091, 9092)
import hashlib
import logging


SERVER_ADDRESSES = [
    ('127.0.0.1', 9090),  # Shard 0
    ('127.0.0.1', 9091),  # Shard 1
    ('127.0.0.1', 9092)   # Shard 2
]
NUM_SERVERS = len(SERVER_ADDRESSES)
NUM_SLOTS = 16384 # Standard Redis Cluster slots

# ====================================================================
# NEW CLASS: HashSlotManager
# ====================================================================

class HashSlotManager:
    """
    Calculates the hash slot for a key and maps the slot to a server index.
    """
    def __init__(self, server_addresses, num_slots=NUM_SLOTS):
        self.num_slots = num_slots
        self.num_servers = len(server_addresses)
        self.server_addresses = server_addresses
        
        # Simple static slot-to-server mapping (like basic range sharding)
        # In a real Redis cluster, this mapping is dynamic.
        self.slots_per_server = self.num_slots // self.num_servers
        
        logging.info(f"Initialized HashSlotManager with {self.num_servers} servers and {self.num_slots} slots.")

    def get_slot(self, key):
        """
        Calculates the hash slot using the CRC16 algorithm, as used by Redis Cluster.
        
        Note: Python's standard `zlib.crc32` is often used as an approximation,
        but for correctness, we'll use a library or a hash approximation.
        Here we use a simple SHA1 hash and modulo, which is adequate for a clone.
        """
        key_bytes = str(key).encode('utf-8')
        
        # In real Redis, the algorithm is CRC16(key) % 16384.
        # We use a standard library hash for demonstration simplicity.
        key_hash = int(hashlib.sha1(key_bytes).hexdigest(), 16)
        
        # Map the hash to one of the 16384 slots
        hash_slot = key_hash % self.num_slots
        
        return hash_slot

    def get_server_index_by_key(self, key):
        """
        Determines which server should handle the key based on its hash slot.
        """
        hash_slot = self.get_slot(key)
        
        # Map the hash slot to a server index using integer division
        # Example: Slot 0-5460 goes to Server 0, 5461-10922 to Server 1, etc.
        server_index = hash_slot // self.slots_per_server
        
        # Ensure we don't exceed the number of available servers
        # This handles the remainder slots if 16384 isn't perfectly divisible.
        return min(server_index, self.num_servers - 1)