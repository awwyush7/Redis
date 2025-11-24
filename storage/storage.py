import heapq
import time

class Storage:
    def __init__(self):
        # key → value
        self.storage = {}

        # key → expire_timestamp (int)
        self.ttl_map = {}

        # min-heap of (expire_timestamp, key)
        self.heap = []

    # internal helper to remove expired keys
    def _cleanup(self):
        now = int(time.time())
        # Keep popping while top is expired
        while self.heap and self.heap[0][0] <= now:
            expire_ts, key = heapq.heappop(self.heap)

            # Check if this entry is still valid
            # (Key may have been updated later with a new TTL)
            if self.ttl_map.get(key) == expire_ts:
                # actual expiry
                self.storage.pop(key, None)
                self.ttl_map.pop(key, None)

    def add(self, key, value, ttl_seconds):
        self._cleanup()

        key = str(key)
        value = str(value)

        # If exists and not expired, return error
        if key in self.storage:
            return {"error": "already exists"}

        # set value
        self.storage[key] = value

        # compute expire timestamp
        expire_ts = int(time.time()) + ttl_seconds
        self.ttl_map[key] = expire_ts

        # push to min heap
        heapq.heappush(self.heap, (expire_ts, key))

        return {"ok": True}

    def get(self, key):
        self._cleanup()

        key = str(key)
        return self.storage.get(key)

    def delete(self, key):
        self._cleanup()

        key = str(key)

        if key in self.storage:
            self.storage.pop(key, None)
            self.ttl_map.pop(key, None)
            # heap cleanup is lazy; no need to remove from heap immediately
            return {"ok": True}
        return {"error": "not found"}
    
    def update(self, key, value):
        self._cleanup

        skey = str(key)
        svalue = str(value)

        if key in self.ttl_map:
            self.storage[skey] = svalue
        else:
            return {"error" : "expired"}
        
        return {"ok"  :True}
