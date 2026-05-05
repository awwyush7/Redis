# 🔴 Mini Redis-like In-Memory Store

> TTL • AOF Persistence • Atomic Operations • Expiration Heap

**This project implements a minimal, educational Redis-style key–value data store in Python.**
It is designed for learning, not production use, and focuses on correctness and clarity.

---

## ✨ Features

### 1. String Key–Value Storage
Simple SET, GET, DEL, UPDATE-style operations backed by Python dictionaries.

### 2. Per-Key TTL (Time-to-Live)
Each key can be associated with an expiration time. Expired keys are automatically removed.

### 3. Automatic Expiration via Min-Heap
A min-heap is used to track the earliest expiring key.
- O(log n) insertion
- O(1) access to the next key to expire

### 4. AOF (Append-Only File) Persistence
All write commands are logged sequentially in `store.aof`. On startup, the store replays the AOF to restore state.

### 5. Crash-Safe Recovery
Because the AOF stores:
1. The command
2. The value
3. Absolute expiration timestamp

…recovery always restores correct TTL behavior.

### 6. Atomic Operations
The entire store is protected using a global lock. This ensures:
- No race conditions
- Consistent writes
- Correctness during expiration and TTL updates

---

## 🚀 Getting Started

### Prerequisites
- Python 3.x
- macOS (or Unix-based system)

### Installation & Setup

#### Step 1: Start the Redis Servers
Open your terminal and run:

```bash
python3 -m app.start_servers.py
```

This will start the Redis server instance(s) and initialize the in-memory store.

#### Step 2: Start the Client
In a new terminal window, launch the client:

```bash
python3 -m client.py
```

You should now be connected to the running Redis server.

#### Step 3: Start Writing Commands

Begin issuing commands to interact with your key-value store. Here are some examples:

**Set a key-value pair:**
```bash
SET {key} {value}
```

Example:
```bash
SET username alice
SET count 42
```

**Get a value by key:**
```bash
GET {key}
```

Example:
```bash
GET username
GET count
```

**Delete a key:**
```bash
DEL {key}
```

Example:
```bash
DEL count
```

---

## 📝 Example Session

```bash
# Terminal 1: Start servers
$ python3 -m app.start_servers.py
✓ Redis server started on port 6379

# Terminal 2: Connect client
$ python3 -m client.py
Connected to Redis server
> SET mykey "Hello World"
OK
> GET mykey
"Hello World"
> DEL mykey
(integer) 1
```

---

## 📚 Learn More

For more information about the architecture and implementation details, see the source code documentation.
