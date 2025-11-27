# Mini Redis-like In-Memory Store
TTL • AOF Persistence • Atomic Operations • Expiration Heap

**This project implements a minimal, educational Redis-style key–value data store in Python.**
It is designed for learning, not production use, and focuses on correctness and clarity.

Features
1. **_String Key–Value Storage_**

Simple SET, GET, DEL, UPDATE-style operations backed by Python dictionaries.

2. _**Per-Key TTL (Time-to-Live)**_

Each key can be associated with an expiration time.
Expired keys are automatically removed.

3. **_Automatic Expiration via Min-Heap_**

A min-heap is used to track the earliest expiring key.
O(log n) insertion, O(1) access to the next key to expire.

4. **_AOF (Append-Only File) Persistence_**

All write commands are logged sequentially in store.aof.
On startup, the store replays the AOF to restore state.

5. _**Crash-Safe Recovery**_

Because the AOF stores:
  1. the command
  2. the value
  3. absolute expiration timestamp
…recovery always restores correct TTL behavior.

6. **_Atomic Operations_**

The entire store is protected using a global lock.
This ensures:
  1. no race conditions,
  2. consistent writes,
  3. correctness during expiration and TTL updates.
