from app.servers.orchestrator import Orchestrator
from app.servers.server import Server
from app.servers.transport import orchestrator_endpoint, shard_endpoint
import multiprocessing
import os
import platform
import asyncio


SHARD_COUNT = 3

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
    
    endpoint = shard_endpoint(node)
    print(f"Starting server shard {node} on internal endpoint {endpoint.describe()} (async event loop)")
    server = Server(host=host, port=port, node=node, endpoint=endpoint)
    asyncio.run(server.run())
    # Server.start() is now async and runs forever

if __name__ == '__main__':
    host = '127.0.0.1'
    base_port = 9090 # Start port
    
    # List to hold references to the processes (changed from threads to processes)
    server_processes = [] 
    
    print(f"🚀 Starting {SHARD_COUNT} shards with async event loops...")
    print(f"   Each shard = 1 process = 1 async event loop = lock-free")
    print(f"   Architecture: Dragonfly-inspired shard-per-core model\n")
    
    for i in range(SHARD_COUNT):
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

    print(f"All server shards started. Client entrypoint => {orchestrator_endpoint().describe()}\n")
    try:
        asyncio.run(Orchestrator(shard_count=len(server_processes)).run())
    except KeyboardInterrupt:
        print("\n🛑 Stopping servers...")
    finally:
        # Terminate all processes
        for proc in server_processes:
            proc.terminate()
            proc.join(timeout=2)
