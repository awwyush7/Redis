import heapq
import time

class Storage:
    def __init__(self, node_number):
        self.storage = {}
        self.ttl_map = {}
        self.heap = []
        self.replaying = False
        self.node = node_number
        # LOCK REMOVED: Single-threaded async event loop makes this safe
        
        # Per-shard metrics
        self.metrics = {
            'total_ops': 0,
            'get_count': 0,
            'set_count': 0,
            'delete_count': 0,
            'hits': 0,
            'misses': 0,
            'total_latency_ns': 0,
            'avg_latency_ns': 0,
            'keys_count': 0,
            'expired_keys': 0
        }

    def _append(self, instruction):
            with open(f"aof.txt_{self.node}", "a", encoding="utf-8") as f:
                f.write(instruction + "\n")

    def _cleanup(self):
            now = int(time.time())
            while self.heap and self.heap[0][0] <= now:
                expire_ts, key = heapq.heappop(self.heap)
                if self.ttl_map.get(key) == expire_ts:
                    self.storage.pop(key, None)
                    self.ttl_map.pop(key, None)
                    self.metrics['expired_keys'] += 1
                    if not self.replaying:
                        self._append(f"DEL {key}")

    def add(self, key, value, ttl_seconds):
        # LOCK REMOVED: Only one coroutine executes at a time
        start_time = time.perf_counter_ns()
        
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

        # Update metrics
        self.metrics['total_ops'] += 1
        self.metrics['set_count'] += 1
        self.metrics['keys_count'] = len(self.storage)
        
        latency = time.perf_counter_ns() - start_time
        self.metrics['total_latency_ns'] += latency
        self.metrics['avg_latency_ns'] = self.metrics['total_latency_ns'] // max(1, self.metrics['total_ops'])

        return {"Status": "Done"}

    def get(self, key):
        # LOCK REMOVED: Single-threaded async event loop
        start_time = time.perf_counter_ns()
        
        self._cleanup()
        key = str(key)
        val = self.storage.get(key)
        
        # Update metrics
        self.metrics['total_ops'] += 1
        self.metrics['get_count'] += 1
        
        if val is None:
            self.metrics['misses'] += 1
            result = {
                "Status":"Done",
                "Value" : "Does Not Exist"
            }
        else:
            self.metrics['hits'] += 1
            result = {
                 "Status":"Done",
                 "Value" : val
            }
        
        latency = time.perf_counter_ns() - start_time
        self.metrics['total_latency_ns'] += latency
        self.metrics['avg_latency_ns'] = self.metrics['total_latency_ns'] // max(1, self.metrics['total_ops'])
        
        return result

    def delete(self, key):
        # LOCK REMOVED: Single-threaded async event loop
        start_time = time.perf_counter_ns()
        
        self._cleanup()
        key = str(key)

        if key not in self.storage:
            return {"error": "not found"}

        self.storage.pop(key, None)
        self.ttl_map.pop(key, None)

        if not self.replaying:
            self._append(f"DEL {key}")

        # Update metrics
        self.metrics['total_ops'] += 1
        self.metrics['delete_count'] += 1
        self.metrics['keys_count'] = len(self.storage)
        
        latency = time.perf_counter_ns() - start_time
        self.metrics['total_latency_ns'] += latency
        self.metrics['avg_latency_ns'] = self.metrics['total_latency_ns'] // max(1, self.metrics['total_ops'])

        return {"Status": "Done"}

    def update(self, key, value):
        # LOCK REMOVED: Single-threaded async event loop
        self._cleanup()

        key = str(key)
        value = str(value)

        if key not in self.storage:
            return {"error": "expired or not found"}

        self.storage[key] = value

        if not self.replaying:
            self._append(f"UPDATE {key} {value}")

        return {"Status": "Done"}
    
    def replay_aof(self):
        # LOCK REMOVED: Single-threaded async event loop
        self.replaying = True
        try:
            file = open(f"aof.txt_{self.node}", "r")
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
        except FileNotFoundError:
            self.replaying = False
            pass
    
    def get_metrics(self):
        """Return current shard metrics"""
        hit_rate = 0.0
        if self.metrics['get_count'] > 0:
            hit_rate = (self.metrics['hits'] / (self.metrics['hits'] + self.metrics['misses'])) * 100
        
        return {
            'shard_id': self.node,
            'total_operations': self.metrics['total_ops'],
            'get_count': self.metrics['get_count'],
            'set_count': self.metrics['set_count'],
            'delete_count': self.metrics['delete_count'],
            'hit_rate_percent': round(hit_rate, 2),
            'hits': self.metrics['hits'],
            'misses': self.metrics['misses'],
            'avg_latency_ns': self.metrics['avg_latency_ns'],
            'avg_latency_us': round(self.metrics['avg_latency_ns'] / 1000, 2),
            'total_keys': self.metrics['keys_count'],
            'expired_keys': self.metrics['expired_keys']
        }
