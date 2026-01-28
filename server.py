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
leader_lock = threading.Lock()
isLeader = False
group_view = []
group_view_lock = threading.Lock()


# stock_name: ([(buy_price, buy_amount, buyer_ip, buyer_port),(...)],[(sell_price, sell_amount, seller_ip, seller_port),(...)], [(owned_amount, owner_ip, owner_port),(...)])
# Balance:  ([(balance, owner_ip, owner_port), (...)])
order_book = {"NVIDIA": ([(198, 24, "127.0.0.1", 42),(200, 55, "127.0.0.1", 43)], [(205, 14, "127.0.0.1", 55),(206, 66,  "127.0.0.1", 96)],[(25, "127.0.0.1", 55), (120, "127.0.0.1", 96)]),
              "APPLE":([],[],[]),
              "Balance": ([(20000, "127.0.0.1", 42), (12000, "127.0.0.1", 43)])}


MULTICAST_GROUP = "239.1.1.1"
MULTICAST_PORT = 5000

# the number of messages this server has sent to the group
S = 0
S_lock = threading.Lock()
# sequence number of latest message from each server
R = {}
R_lock = threading.Lock()
holdback = []
delivery = []

multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)

received_first_view = False
crashed_servers = []

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


def find_nearest_server():
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
                return (ip, port)
                break
    except socket.timeout:
        return None

def discovery():
    global leader, group_view, received_first_view

    nearest = find_nearest_server()

    if nearest is None:
        # make ourselves the leader
        with leader_lock, group_view_lock:
            leader = (server_address, server_port)
            isLeader = True
            group_view = [(server_address, server_port)]
            received_first_view = True
        print("Found no other servers; this server was declared the leader.")
    else:
        while True:
            with group_view_lock:
                group_view.append(nearest)

            ip, port = nearest

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip, port))

            s.sendall(f"INFO\n".encode())

            buffer = ""
            while True:
                chunk = s.recv(4096).decode()
                if not chunk:
                    break
                buffer += chunk
            s.close()

            lines = buffer.strip().split("\n")

            for line in lines:
                if line.startswith("BOOK|"):
                    book_json = line[5:]
                    global order_book
                    order_book = restore_order_book(json.loads(book_json))
                    print("Received order book")

                elif line.startswith("LEADER|"):
                    _, ip, port = line.split("|")
                    global leader
                    with leader_lock:
                        leader = (ip, int(port))
                        group_view.append(leader)
                        print("Leader is", leader)

                elif line.startswith("ACKS|"):
                    global R
                    with R_lock:
                        R = json.loads(line.split("|")[1])
                        print("Received sequence numbers:", R)

                else:
                    print("Unknown line:", line)

            send_to_leader(f"JOIN")
            # nearest or leader crashed?
            with group_view_lock, leader_lock:
                if nearest in group_view and leader in group_view:
                    break
                else:
                    if leader not in group_view:
                        time.sleep(5)
                    nearest = find_nearest_server()

    # leader sends us group view
    # election
    # choose most up-to-date order book and distribute it
    # start multicast listener thread for updates from leader
    # or if elected start thread for backend and distributing updates
    return True


def multicast(msg):
    global S, multicast_socket
    with S_lock:
        S = S + 1
        multicast_socket.sendto((msg + f"|{server_port}|{S}|{json.dumps(R)}").encode(), (MULTICAST_GROUP, MULTICAST_PORT))

def multicast_listen():
    global crashed, group_view, delivery, holdback
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", MULTICAST_PORT))
    mreq = socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton("0.0.0.0")
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    while not crashed:
        data, addr = sock.recvfrom(4096)
        msg = data.decode().strip()
        ip = addr[0]
        port = msg.split('|')[1]
        s = msg.split('|')[2]
        acks = json.loads(msg.split('|')[3])

        with S_lock, R_lock:
            if S == R[(ip, port)] + 1:
                delivery.append((msg, addr))


def multicast_deliver(msg, addr):
        ip = addr[0]
        port = msg.split('|')[1]
        if msg.startswith("CRASH|"):
            with group_view_lock:
                group_view.remove((ip, port))
            if not received_first_view:
                crashed_servers.append((ip, port))
            if (server_address, server_port) in group_view:
                election()

        if msg.startswith("NEW|"):
            ip = msg.split("|")[1]
            port = msg.split("|")[2]
            print(f"New server joined ({ip}:{port})")
            if (ip, port) not in group_view:
                group_view.append((ip, port))
            if (ip, port) not in R:
                R[(ip, port)] = 0


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
    with leader_lock:
        s.connect(leader)
    s.sendall(msg.encode())
    s.close()

def handle_tcp(connection):
    global crashed, isLeader

    ip, port = connection.getpeername()
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

            elif msg == "INFO":
                connection.sendall(f"BOOK|{json.dumps(order_book)}\n".encode())
                connection.sendall(f"LEADER|{leader[0]}|{leader[1]}\n".encode())
                connection.sendall(f"ACKS|{json.dumps(R)}\n".encode())
                print(f"Gave current order book to ({ip}:{port})")

            elif msg.startswith("VIEW|"):


                received_first_view = True
                #delete from view crashed servers
            elif not isLeader:
                connection.sendall(b"Incorrect formatting!\n")
                print("sent error response")
            elif isLeader:
                if msg == "JOIN":
                    multicast(f"NEW|{ip}|{port}")
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
    global crashed, broadcast_port
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.sendto(str.encode(f"CRASH|{server_port}"), (broadcast_address, broadcast_port))
    multicast(f"CRASH")
    multicast_socket.close()
    # notify leader who then distributes new view of servers

def heartbeat():
    global crashed, broadcast_address, broadcast_port, server_port
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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
