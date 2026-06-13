from stage4_async_aof.server import Server
import multiprocessing
import time
import os
import platform
import asyncio

def start_server_instance(host, port, node):
    """Function to create and start a single server instance."""
    # Try to pin this process to a specific CPU core (Linux only)
    if platform.system() == 'Linux':
        try:
            os.sched_setaffinity(0, {node})
            print(f"✓ Shard {node} pinned to CPU core {node}")
        except (AttributeError, OSError) as e:
            print(f"⚠ Could not set CPU affinity for shard {node}: {e}")
    else:
        print(f"ℹ CPU affinity not supported on {platform.system()}")
    
    print(f"Starting server shard {node} on {host}:{port} (async event loop)")
    server = Server(host=host, port=port, node=node)
    asyncio.run(server.run())
    # Server.start() is now async and runs forever

if __name__ == '__main__':
    host = '127.0.0.1'
    base_port = 9090 # Start port
    
    # List to hold references to the processes (changed from threads to processes)
    server_processes = [] 
    
    print(f"🚀 Starting {3} shards with async event loops...")
    print(f"   Each shard = 1 process = 1 async event loop = lock-free")
    print(f"   Architecture: Dragonfly-inspired shard-per-core model\n")
    
    for i in range(3):
        node = i
        port = base_port + i # Unique port for each instance
        
        # Use multiprocessing.Process instead of threading.Thread
        # This gives us true parallelism (no GIL limitation)
        process = multiprocessing.Process(
                target=start_server_instance, 
                args=(host, port, node), 
                daemon=True
            )
        server_processes.append(process)
        process.start()
        
    # The main process must be kept alive
    print("All server shards started. Press Ctrl+C to stop.\n")
    try:
        # Keep the main thread alive indefinitely
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping servers...")
        # Terminate all processes
        for proc in server_processes:
            proc.terminate()
            proc.join(timeout=2)