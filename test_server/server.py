import socket
import threading

HOST = '192.168.1.6'
PORT = 9090

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
server.listen(5)

def handle(conn, addr):
    print(f"Connected to {addr}")
    msg = conn.recv(1024).decode('utf-8')
    print(f"Message from client: {msg}")
    conn.send("HELLO FROM SERVER".encode('utf-8'))
    conn.close()
    print(f"Connection with {addr} ended")

count = 0
while True:
    conn, addr = server.accept()  # blocking
    print(f"Connection number {count}")
    count += 1
    thread = threading.Thread(target=handle, args=(conn, addr), daemon=True)
    thread.start()
