import logging
import socket
import ssl
import threading

HOST = '127.0.0.1'
PORT = 9090

# --- SSL/TLS Setup ---
# 1. Create a default context for the server
context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)

# 2. Load the server's private key and certificate
context.load_cert_chain(certfile="server.crt", keyfile="server.key")
# ---------------------


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
server.listen(5)


secure_server_socket = context.wrap_socket(server, server_side=True)


def handle(secure_comm_socket, addr):
    print(f"Connected securely to {address}") 
    try:
        message = secure_comm_socket.recv(1024).decode('utf-8')
        print(f"Message from client is : {message}")
        secure_comm_socket.send("HELLO FROM SERVER (SECURE)😘".encode('utf-8'))
    
    except ssl.SSLError as e:
        print(f"SSL/TLS Communication Error: {e}")
        
    except Exception as e:
        print(f"General Error: {e}")
        
    finally:
        secure_comm_socket.close()
        print(f"Secure connection with {address} ended!")

i = 0


while True:
    communication_socket, address = secure_server_socket.accept()
    print(f"Connection number {i} accepted.")
    i = i + 1
    
    thread = threading.Thread(target=handle, args=(communication_socket, address), daemon=True)
    thread.start()
