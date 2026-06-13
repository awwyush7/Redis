import heapq
import time
import threading

class Storage:
    def __init__(self):
        self.storage = {}
        self.ttl_map = {}
        self.heap = []
        self.replaying = False
        self.lock = threading.Lock()

    def _append(self, instruction):
            with open("aof.txt", "a", encoding="utf-8") as f:
                f.write(instruction + "\n")

    def _cleanup(self):
            now = int(time.time())
            while self.heap and self.heap[0][0] <= now:
                expire_ts, key = heapq.heappop(self.heap)
                if self.ttl_map.get(key) == expire_ts:
                    self.storage.pop(key, None)
                    self.ttl_map.pop(key, None)
                    if not self.replaying:
                        self._append(f"DEL {key}")

    def add(self, key, value, ttl_seconds):
        with self.lock:
            self._cleanup()

            key = str(key)
            value = str(value)

            if key in self.storage:
                return {"error": "already exists"}

            expire_ts = int(time.time()) + ttl_seconds

            self.storage[key] = value
            self.ttl_map[key] = expire_ts

            heapq.heappush(self.heap, (expire_ts, key))

            if not self.replaying:
                self._append(f"SETABS {key} {value} {expire_ts}")

            return {"ok": True}

    def get(self, key):
        with self.lock:
            self._cleanup()
            key = str(key)
            return self.storage.get(key)

    def delete(self, key):
        with self.lock:
            self._cleanup()
            key = str(key)

            if key not in self.storage:
                return {"error": "not found"}

            self.storage.pop(key, None)
            self.ttl_map.pop(key, None)

            if not self.replaying:
                self._append(f"DEL {key}")

            return {"ok": True}

    def update(self, key, value):
        with self.lock:
            self._cleanup()

            key = str(key)
            value = str(value)

            if key not in self.storage:
                return {"error": "expired or not found"}

            self.storage[key] = value

            if not self.replaying:
                self._append(f"UPDATE {key} {value}")

            return {"ok": True}
    
    def replay_aof(self):
        with self.lock:
            self.replaying = True
            file = open("aof.txt","r")
            for line in file:
                content = line.strip().split(" ")
                action = content[0]
                match action:
                    case "SETABS":
                        remaining = int(content[3]) - int(time.time())
                        if(remaining > 0):
                            self.storage[content[1]] = content[2]
                            self.ttl_map[content[1]] = int(content[3])
                            heapq.heappush(self.heap, (int(content[3]), content[1]))
                    case "DEL":
                        self.storage.pop(content[1], None)
                        self.ttl_map.pop(content[1], None)
                    case "UPDATE":
                        self.storage[content[1]] = content[2]
            
            self.replaying = False
            file.close()

            print("DONE REPLAYING")


