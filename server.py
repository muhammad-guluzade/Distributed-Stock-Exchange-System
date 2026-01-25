import multiprocessing
import socket
import os
import ipaddress
import threading
import time
import netifaces
import json

server_address = None
server_port = None
broadcast_address = None
broadcast_port = 1234
server_socket = None
crashed = False
leader = None
isLeader = False
group_view = []


# stock_name: ([(buy_price, buy_amount, buyer_ip, buyer_port),(...)],[(sell_price, sell_amount, seller_ip, seller_port),(...)], [(owned_amount, owner_ip, owner_port),(...)])
# Balance:  ([(balance, owner_ip, owner_port), (...)])
order_book = {"NVIDIA": ([(198, 24, "127.0.0.1", 42),(200, 55, "127.0.0.1", 43)], [(205, 14, "127.0.0.1", 55),(206, 66,  "127.0.0.1", 96)],[(25, "127.0.0.1", 55), (120, "127.0.0.1", 96)]),
              "APPLE":([],[],[]),
              "Balance": ([(20000, "127.0.0.1", 42), (12000, "127.0.0.1", 43)])}


MULTICAST_GROUP = "239.1.1.1"
MULTICAST_PORT = 5000

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
    global leader

    broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    broadcast_sock.bind(("0.0.0.0", broadcast_port))

    print("[Discovery] Searching for a neighbor server...")

    broadcast_sock.settimeout(10)
    try:
        # find available server
        while True:
            data, addr = broadcast_sock.recvfrom(1024)
            msg = data.decode().strip()
            ip = addr[0]
            parts = msg.split("|")
            msg, port_str = parts
            try:
                port = int(port_str)
            except ValueError:
                continue  # bad port, ignore packet
            if msg == "HELLO" and len(parts) == 2:
                print(f"[Discovery] Found server {ip}:{port}")
                break
        # get available servers from found server
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        s.sendall(f"JOIN|{server_port}".encode())
        book = s.recv(1024).decode()
        s.close()

    except socket.timeout:
        leader = (server_address, server_port)
        isLeader = True
        group_view = [(server_address, server_port)]
        print("Found no other servers; this server was declared the leader.")




    # receive up-to-date order book from leader
    # election
    # choose most up-to-date order book and distribute it
    # start multicast listener thread for updates from leader
    # or if elected start thread for backend and distributing updates
    return True


def multicast_listen():

    if msg == "group_view":
    buffer = ""
        while "\n" not in buffer:
            chunk = s.recv(1024).decode()
            if not chunk:
                break
            buffer += chunk
        s.close()

        group_view = json.loads(buffer.strip())  # decode JSON
        group_view = [tuple(x) for x in group_view]

def connection_listen():
    global server_socket, crashed
    while not crashed:
        server_socket.listen()
        conn, addr = server_socket.accept()
        client_thread = threading.Thread(target=handle_tcp, args=[conn])
        client_thread.start()
    server_socket.close()

def send_to_leader(msg):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(leader)
    s.sendall(msg.encode())
    s.close()



def handle_tcp(connection):
    global crashed

    ip, _ = connection.getpeername()
    buffer = ""

    while not crashed:
        data = connection.recv(4096).decode()
        if not data:
            break

        buffer += data

        # Process full messages separated by newline
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            msg = line.strip()

            if msg == "PING":
                connection.sendall(b"PONG\n")
                print("sent pong")

            elif msg == "UPDATE":
                connection.sendall(b"info\n")
                print("sent info")

            elif msg.startswith("JOIN|"):
                port = msg.split("|")[1]
                send_to_leader(f"JOIN|{ip}|{port}")
                print(f"sent join message of ({ip}:{port}) to leader")

            elif msg.startswith("BOOK|"):
                book_json = msg[5:]
                order_book = restore_order_book(json.loads(book_json))
                print("Received order book update")

                # Example response (optional)
                response = "BOOK|" + json.dumps(order_book) + "\n"
                connection.sendall(response.encode())

            else:
                connection.sendall(b"Incorrect formatting!\n")
                print("sent error response")

    connection.close()

def restore_order_book(book):
    restored = {}

    for symbol, value in book.items():
        if symbol == "Balance":
            restored[symbol] = (
                [tuple(entry) for entry in value[0]]
                ,)
        else:
            restored[symbol] = tuple(
                [tuple(entry) for entry in sublist]
                for sublist in value
            )

    return restored

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

    # notify leader who then distributes new view of servers

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

    connection_listener_thread = threading.Thread(target=connection_listen, daemon=True).start()
    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True).start()

    while True:
        command = input("Enter command: ")
        if command == "crash":
            crash()
            break
