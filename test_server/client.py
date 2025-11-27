# import socket
 
# HOST = '192.168.1.6'
# PORT = 9090

# socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# socket.connect((HOST,PORT))

# socket.send("HELLO, I AM AYUSH😀".encode('utf-8'))
# print(socket.recv(1024).decode('utf-8'))
import socket
import threading

HOST = '192.168.1.6'
PORT = 9090

def client(n):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    s.send(f"HELLO FROM CLIENT {n}".encode())
    print(s.recv(1024).decode())
    s.close()

threads = []
for i in range(5):  # create 20 clients
    t = threading.Thread(target=client, args=(i,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()
