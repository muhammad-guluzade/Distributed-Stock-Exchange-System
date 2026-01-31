import multiprocessing
import socket
import os
import ipaddress
import threading
import time
from random import Random
import uuid
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
R_f = {}
R_f_lock = threading.Lock()
fifo_holdback_queues = {}
fifo_holdback_lock = threading.Lock()
delivery = []

multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)

received_first_view = False
crashed_servers = []

S_b = 0
R_b = {}
S_b_lock = threading.Lock()
R_b_lock = threading.Lock()
# can grow infinitely
Received = {}

server_id = str(uuid.uuid4())

def B_multicast(msg):
    global S_b, multicast_socket
    with S_b_lock, multicast_socket_lock, sent_messages_lock:
        S_b = S_b + 1
        multicast_socket.sendto(f"{server_port}|{server_id}|{S_b}|{msg}".encode(), (MULTICAST_GROUP, MULTICAST_PORT))
        sent_messages[S_b] = {
            "msg": msg,
            "acks": set()
        }

def B_deliver(msg):
    global Received
    ip = msg[0]
    port = msg[1]
    id = msg[2]
    S = msg[3]
    payload = msg[4]
    if (ip, port, id, S) not in Received:
        Received[(ip, port, id, S)] = payload
        if (ip, port, id) != (server_address,server_port, server_id):
            B_multicast(payload)
        R_deliver(payload)

def B_listen():
    global crashed, group_view, delivery
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", MULTICAST_PORT))
    mreq = socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton("0.0.0.0")
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    while not crashed:
        data, addr = sock.recvfrom(4096)
        raw_message = data.decode().strip()
        ip = addr[0]
        port, S_str, payload = raw_message.split('|')
        port = int(port)
        S = int(S_str)

        sender = (ip, port)
        deliver_list = []
        with R_b_lock:
            expected = R_b[sender] + 1
            if S == expected:
                deliver_list.append((ip, port, S, payload))
                R_b[sender] += 1
                # look through holdback queue
                while sender in holdback_queues and (R_b[sender] + 1 in holdback_queues[sender]):
                    next_seq = R_b[sender] + 1
                    deliver_list.append(holdback_queues[sender].pop(next_seq))
                    R_b[sender] += 1
            elif S > expected:
                # missed a message, saving received message in holdback queue
                nack(sender, S, R_b[sender])
                if sender not in holdback_queues:
                    holdback_queues[sender] = {}
                holdback_queues[sender][S] = payload
        for msg in deliver_list:
            B_deliver(msg)

def R_deliver(msg):
    global fifo_holdback_queues
    S = msg.split('|')[0]
    ip = msg.split('|')[1]
    port = msg.split('|')[2]
    payload = msg.split('|')[3]
    sender = (ip, port)
    deliver_list = []
    with R_f_lock:
        expected = R_f[sender] + 1
        if S == expected:
            deliver_list.append((ip, port, payload))
            R_f[sender] += 1

            while sender in fifo_holdback_queues and (R_f[sender] + 1 in fifo_holdback_queues[sender]):
                next_seq = R_f[sender] + 1
                deliver_list.append(fifo_holdback_queues[sender].pop(next_seq))
                R_f[sender] += 1

        elif S > expected:
            if sender not in fifo_holdback_queues:
                fifo_holdback_queues[sender] = {}
            fifo_holdback_queues[sender][S] = payload
    for (ip, port, payload) in deliver_list:
        FIFO_deliver(ip, port, payload)

def R_multicast(msg):
    B_multicast(msg)

def FIFO_multicast(msg):
    global holdback, S_f, R_f
    with S_f_lock:
        S_f += 1
        R_multicast(f"{S_f}|{server_address}|{server_port}|{msg}")


def FIFO_deliver(ip, port, msg):
    if msg == "CRASH":
        print(f"Server ({ip}:{port}) crashed!")
        if not received_first_view:
            crashed_servers.append((ip, port))
        else:
            with group_view_lock:
                group_view.remove((ip, port))
        if (server_address, server_port) in group_view:
            election()

    if msg == "JOIN":
        print(f"New server joined ({ip}:{port})")
        if (ip, port) not in group_view:
            with group_view_lock:
                group_view.append((ip, port))
        if (ip, port) not in R_b:
            with R_b_lock, R_f_lock:
                R_b[(ip, port)] = 0
                R_f[(ip, port)] = 0
        if isLeader:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # send view to new server
    if msg.startswith("UPDATE|"):
        update = msg.split("|")[3]
        print(f"Applying update {update}")
        apply_update(update)


def apply_update(update):
    with order_book_lock:
        pass

def nack(adr, S, R):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(adr)
    s.sendall(f"NACK|{S}|{R}".encode())
    s.close()

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
    global leader, group_view, received_first_view, R_b, isLeader, R_f

    nearest = find_nearest_server()

    if nearest is None:
        # make ourselves the leader
        with leader_lock, group_view_lock:
            leader = (server_address, server_port)
            isLeader = True
            group_view = [(server_address, server_port)]
            R_b = {(server_address, server_port): 0}
            R_f = {(server_address, server_port): 0}
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

                elif line.startswith("B_ACKS|"):
                    global R_b
                    with R_b_lock:
                        R_b = json.loads(line.split("|")[1])
                        print("Received basic sequence numbers:", R_b)

                elif line.startswith("F_ACKS|"):
                    global R_f
                    with R_f_lock:
                        R_f = json.loads(line.split("|")[1])
                        print("Received FIFO sequence numbers:", R_f)

                else:
                    print("Unknown line:", line)

            FIFO_multicast(f"JOIN")
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
            elif msg.startswith("ACK|"):
                s = int(msg.split("|")[1])
                with sent_messages_lock:
                    sent_messages[s][1].add((ip, port))
                    if all(e in sent_messages[s][1] for e in group_view):
                        del sent_messages[s]

            elif msg.startswith("NACK|"):
                s = int(msg.split("|")[1])
                r = int(msg.split("|")[2])
                with multicast_socket_lock:
                    for i in range(r+1, s):
                        multicast_socket.sendall(f"{server_port}|{i}|{sent_messages[i][0]}".encode())
            elif not isLeader:
                connection.sendall(b"Incorrect formatting!\n")
                print("sent error response")
            elif isLeader:

                continue
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
