# 💡 Corrected app/start_servers.py

from .servers.server import Server
import threading
import time # Import time to keep the main thread alive

def start_server_instance(host, port, node):
    """Function to create and start a single server instance."""
    # This call to Server() now runs in its own dedicated thread.
    print(f"Starting server node {node} on {host}:{port}")
    server = Server(host=host, port=port, node=node)
    
    # The server runs in a perpetual loop inside Server.start(), 
    # so the thread will stay alive here.
    # Note: Server.start() is a blocking call! 
    # If it fails to block (e.g., if you only call Server() in __init__), 
    # you might need a small modification in the Server class.

if __name__ == '__main__':
    host = '127.0.0.1'
    base_port = 9090 # Start port
    
    # List to hold references to the threads
    server_threads = [] 
    
    for i in range(3):
        node = i
        port = base_port + i # Unique port for each instance
        
        thread = threading.Thread(
                target=start_server_instance, 
                args=(host, port, node), 
                daemon=True # Make threads daemons so they quit when main thread exits
            )
        server_threads.append(thread)
        thread.start()
        
    # The main thread must be kept alive, otherwise daemon threads will exit immediately.
    print("All server threads started. Press Ctrl+C to stop.")
    try:
        # Keep the main thread alive indefinitely
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping servers...")
        # Since they are daemon threads, they will be automatically terminated
        # when the main process exits.