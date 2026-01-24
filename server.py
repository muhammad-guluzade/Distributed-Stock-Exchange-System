import multiprocessing
import socket
import os
import ipaddress
import threading
import time
import netifaces

server_address = None
server_port = None
broadcast_address = None
broadcast_port = 1234
server_socket = None
crashed = False

def set_broadcast_address():
    global server_address, broadcast_address
    # find active local IP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))  # no traffic sent, just picks interface
    local_ip = s.getsockname()[0]
    s.close()

    # now get subnet mask from OS (Linux way)
    iface = netifaces.gateways()['default'][netifaces.AF_INET][1]
    addr_info = netifaces.ifaddresses(iface)[netifaces.AF_INET][0]
    netmask = addr_info['netmask']

    network = ipaddress.IPv4Network(f"{local_ip}/{netmask}", strict=False)
    server_address = local_ip
    broadcast_address = str(network.broadcast_address)

def discovery():
    # save global view of servers
    # receive up-to-date order book
    # election
    # start multicast listener thread for updates from leader
    # or if elected start thread for backend and distributing updates
    return True

def connection_listen():
    global server_socket, crashed
    while not crashed:
        server_socket.listen()
        conn, addr = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=[conn])
        client_thread.start()
    server_socket.close()

def handle_client(connection):
    global crashed
    while not crashed:
        data = connection.recv(1024)
        msg = data.decode().strip()
        if msg == "PING":
            response = "PONG"
            connection.sendall(response.encode())
            print("sent pong")
            break
        elif msg == "update":
            response = "info"
            connection.sendall(response.encode())
            print("sent info")
        else:
            response = "Incorrect formatting!"
            connection.sendall(response.encode())
            print("sent error response")
    connection.close()

def broadcast_listen():
    global broadcast_port
    # Create a UDP socket
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Allow multiple listeners (Linux/macOS semantics)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Bind to all local interfaces to receive broadcast packets
    broadcast_socket.bind(("0.0.0.0", broadcast_port))
    print("Listening to broadcast announcements")
    while not crashed:
        data, addr = broadcast_socket.recvfrom(1024)
        if data == "CRASH" and not crashed:
            election()
    broadcast_socket.close()

def crash():
    global crashed, broadcast_address, broadcast_port, server_port
    crashed = True
    # Create a UDP socket
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Enable permission to send to broadcast addresses
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    # Send the broadcast announcement
    broadcast_socket.sendto(str.encode(f"CRASH|{server_port}"), (broadcast_address, broadcast_port))

def heartbeat():
    global crashed, broadcast_address, broadcast_port, server_port
    # Create a UDP socket
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Enable permission to send to broadcast addresses
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    while not crashed:
        broadcast_socket.sendto(f"HELLO|{server_port}".encode(), (broadcast_address, broadcast_port))
        # print("sent heartbeat")
        time.sleep(1)
    broadcast_socket.close()


if __name__ == "__main__":

    set_broadcast_address()
    print("Local IP:", server_address)
    print("Broadcast:", broadcast_address)
    discovery()

    # listen for client connections
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((server_address, 0))
    server_port = server_socket.getsockname()[1]

    print(f"Server running on {server_address}:{server_port}")

    connection_listener_thread = threading.Thread(target=connection_listen, daemon=True)
    connection_listener_thread.start()

    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
    heartbeat_thread.start()

    while True:
        command = input("Enter command: ")
        if command == "crash":
            crash()
            break
