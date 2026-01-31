import socket
import ipaddress
import threading
import time
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
group_view = {}
group_view_lock = threading.Lock()

# nickname instead of (ip, port)
# stock_name: ([(buy_price, buy_amount, buyer_ip, buyer_port),(...)],[(sell_price, sell_amount, seller_ip, seller_port),(...)], [(owned_amount, owner_ip, owner_port),(...)])
# Balance:  ([(balance, owner_ip, owner_port), (...)])
order_book = {"NVIDIA": ([(198, 24, "127.0.0.1", 42),(200, 55, "127.0.0.1", 43)], [(205, 14, "127.0.0.1", 55),(206, 66,  "127.0.0.1", 96)],[(25, "127.0.0.1", 55), (120, "127.0.0.1", 96)]),
              "APPLE":([],[],[]),
              "Balance": ([(20000, "127.0.0.1", 42), (12000, "127.0.0.1", 43)])}
order_book_lock = threading.Lock()

MULTICAST_GROUP = "239.1.1.1"
MULTICAST_PORT = 5000

# the number of messages this server has sent to the group
S_f = 0
S_f_lock = threading.Lock()
# sequence number of latest message from each server
R_f = {}
R_f_lock = threading.Lock()
fifo_holdback_queues = {}
fifo_holdback_lock = threading.Lock()
delivery = []
delivery_lock = threading.Lock()

received_first_view = False
received_first_view_lock = threading.Lock()
crashed_servers = []

multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
multicast_socket_lock = threading.Lock()

sent_messages = {}
sent_messages_lock = threading.Lock()

holdback_queues = {}

S_b = 0
R_b = {}
S_b_lock = threading.Lock()
R_b_lock = threading.Lock()
# can grow infinitely
Received = {}

server_id = str(uuid.uuid4())

print_lock = threading.Lock()

def pr(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def B_deliver(msg):
    # pr("B delivered a message:", msg)
    global Received
    S = msg[0]
    id = msg[1]
    payload = msg[2]
    S_payload_str, id_payload, payload_payload = payload.split('|', 2)
    S_payload = int(S_payload_str)
    # pr(f"Sending B_ACK to {id} from {server_id}")
    # tcp_to_id(id, f"B_ACK|{S}|{server_id}\n")
    if (S_payload, id_payload) not in Received:
        Received[(S_payload, id_payload)] = True
        if id_payload != server_id:
            B_multicast(payload)

        R_deliver(payload)

def tcp_to_id(id, msg):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with group_view_lock:
        adr = group_view[id]
    s.connect((adr[0], adr[1]))
    s.sendall(msg.encode())
    s.close()

def B_listen():
    global crashed, group_view, delivery
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", MULTICAST_PORT))
    mreq = socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton("0.0.0.0")
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    pr("Started listening to ip multicasts")
    while not crashed:
        data, addr = sock.recvfrom(4096)
        msg = data.decode().strip()
        # pr("Received an ip multicast message:", msg)
        S_str, sender_id, payload = msg.split('|', 2)
        S = int(S_str)

        deliver_list = []
        with R_b_lock:
            if sender_id in R_b:
                expected = R_b[sender_id] + 1
                if S == expected:
                    deliver_list.append((S, sender_id, payload))
                    R_b[sender_id] += 1
                    # look through holdback queue
                    while sender_id in holdback_queues and (R_b[sender_id] + 1 in holdback_queues[sender_id]):
                        next_seq = R_b[sender_id] + 1
                        deliver_list.append((next_seq, sender_id, holdback_queues[sender_id].pop(next_seq)))
                        R_b[sender_id] += 1
                elif S > expected:
                    # missed a message, saving received message in holdback queue
                    nack(S, sender_id, R_b[sender_id])
                    if sender_id not in holdback_queues:
                        holdback_queues[sender_id] = {}
                    holdback_queues[sender_id][S] = payload
        for msg in deliver_list:
            B_deliver(msg)

def B_multicast(msg):
    global S_b, multicast_socket
    with S_b_lock, multicast_socket_lock, sent_messages_lock:
        S_b = S_b + 1
        # pr(f"I do B-multicast {S_b}|{server_id}|{msg}")
        multicast_socket.sendto(f"{S_b}|{server_id}|{msg}".encode(), (MULTICAST_GROUP, MULTICAST_PORT))
        sent_messages[S_b] = {
            "msg": msg,
            "acks": set()
        }


def R_multicast(msg):
    B_multicast(msg)

def FIFO_multicast(msg):
    global holdback, S_f, R_f
    with S_f_lock:
        S_f += 1
        pr("FIFO multicasting message:", f"{S_f}|{server_id}|{msg}")
        R_multicast(f"{S_f}|{server_id}|{msg}")

def R_deliver(msg):
    # pr("R delivering message:", msg)
    global fifo_holdback_queues
    S_str, sender_id, payload = msg.split('|', 2)
    S = int(S_str)
    deliver_list = []
    with R_f_lock:
        expected = R_f[sender_id] + 1
        if S == expected:
            deliver_list.append((sender_id, payload))
            R_f[sender_id] += 1

            while sender_id in fifo_holdback_queues and (R_f[sender_id] + 1 in fifo_holdback_queues[sender_id]):
                next_seq = R_f[sender_id] + 1
                deliver_list.append(fifo_holdback_queues[sender_id].pop(next_seq))
                R_f[sender_id] += 1

        elif S > expected:
            if sender_id not in fifo_holdback_queues:
                fifo_holdback_queues[sender_id] = {}
            fifo_holdback_queues[sender_id][S] = payload
    for (id, payload) in deliver_list:
        FIFO_deliver(sender_id, payload)

def FIFO_deliver(id, msg):
    pr("Delivered a FIFO message: ", msg, "from", id)
    if msg == "CRASH":
        with group_view_lock:
            ip, port = group_view[id]
        pr(f"Server ({ip}:{port}|{id}) crashed!")
        if not received_first_view:
            crashed_servers.append(id)
        else:
            with group_view_lock:
                del group_view[id]
        if id in group_view:
            # election()
            pr("Starting election")

    if msg.startswith("NEW|"):
        ip = msg.split("|")[1]
        port = int(msg.split("|")[2])
        id = msg.split("|")[3]
        pr(f"New server joined ({ip}:{port}:{id})")
        with group_view_lock:
            group_view[id] = (ip, port)
        if id not in R_b:
            with R_b_lock, R_f_lock:
                R_b[id] = 0
                R_f[id] = 0
        # election()
        pr("Starting election")
    if msg.startswith("UPDATE|"):
        update = msg.split("|")[3]
        pr(f"Applying update {update}")
        apply_update(update)


def apply_update(update):
    with order_book_lock:
        pass

def nack(S, id, R):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with group_view_lock:
        adr = group_view[id]
    s.connect((adr[0], adr[1]))
    s.sendall(f"NACK|{S}|{R}\n".encode())
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

    pr("[Discovery] Searching for a neighbor server...")

    broadcast_sock.settimeout(5)
    try:
        # find available server
        while True:
            data, addr = broadcast_sock.recvfrom(1024)
            msg = data.decode().strip()
            ip = addr[0]
            parts = msg.split("|")
            msg, port_str, id = parts
            try:
                port = int(port_str)
            except ValueError:
                continue  # bad port, ignore packet
            if msg == "HELLO" and len(parts) == 3:
                pr(f"[Discovery] Found server {ip}:{port}:{id}")
                return (ip, port, id)
                break
    except socket.timeout:
        return None

def discovery():
    global leader, group_view, received_first_view, R_b, isLeader, R_f

    nearest = find_nearest_server()

    if nearest is None:
        # make ourselves the leader
        with leader_lock, group_view_lock, R_b_lock, R_f_lock, received_first_view_lock:
            leader =  server_id
            isLeader = True
            group_view = {server_id: (server_address, server_port)}
            R_b = {server_id: 0}
            R_f = {server_id: 0}
            received_first_view = True
        pr("Found no other servers; this server was declared the leader.")
    else:
        while True:
            ip, port, id = nearest
            with group_view_lock:
                group_view[id] = (ip, port)


            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip, port))

            s.sendall(f"JOIN|{server_address}|{server_port}|{server_id}\n".encode())

            buffer = ""
            lines = []

            while len(lines) < 5:
                chunk = s.recv(4096).decode()
                if not chunk:
                    raise ConnectionError("Server closed connection during startup")

                buffer += chunk

                while "\n" in buffer and len(lines) < 5:
                    line, buffer = buffer.split("\n", 1)
                    lines.append(line)

            for line in lines:
                if line.startswith("BOOK|"):
                    book_json = line[5:]
                    global order_book
                    with order_book_lock:
                        order_book = restore_order_book(json.loads(book_json))
                    pr("Received order book")

                elif line.startswith("LEADER|"):
                    _, leader_ip, leader_port, leader_id = line.split("|")
                    with leader_lock, group_view_lock:
                        leader = leader_id
                        group_view[leader_id] = (leader_ip, int(leader_port))
                        pr("Leader is", leader)

                elif line.startswith("B_ACKS|"):
                    with R_b_lock:
                        R_b = json.loads(line.split("|")[1])
                        pr("Received basic sequence numbers:", R_b)

                elif line.startswith("F_ACKS|"):
                    with R_f_lock:
                        R_f = json.loads(line.split("|")[1])
                        pr("Received FIFO sequence numbers:", R_f)

                elif line.startswith("VIEW|"):
                    with group_view_lock:
                        group_view = json.loads(line.split("|")[1])
                        pr("Received view:", group_view)
                else:
                    pr("Unknown line:", line)

            s.sendall(f"JOINED\n".encode())
            s.close()

            # nearest or leader crashed?
            with group_view_lock, leader_lock:
                if nearest[2] in group_view and leader in group_view:
                    break
                else:
                    if leader not in group_view:
                        time.sleep(5)
                    nearest = find_nearest_server()

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
    ip, port, _ = leader
    with leader_lock:
        s.connect((ip, port))
    s.sendall(msg.encode())
    s.close()

def handle_tcp(connection):
    global crashed, isLeader
    ip, port = connection.getpeername()
    buffer = ""
    done = False

    while not crashed and not done:
        data = connection.recv(4096).decode()
        if not data:
            break

        buffer += data

        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            msg = line.strip()

            if msg == "PING":
                connection.sendall(b"PONG\n")
                done = True

            elif msg.startswith("UPDATE|"):
                pr("Need to send Update")
                connection.sendall(b"info\n")
                done = True

            elif msg.startswith("JOIN|"):
                new_ip = msg.split("|")[1]
                new_port = msg.split("|")[2]
                new_id = msg.split("|")[3]
                with order_book_lock, leader_lock, R_f_lock, R_b_lock, group_view_lock:
                    connection.sendall(f"BOOK|{json.dumps(order_book)}\n".encode())
                    connection.sendall(f"LEADER|{group_view[leader][0]}|{group_view[leader][1]}|{leader}\n".encode())
                    connection.sendall(f"B_ACKS|{json.dumps(R_b)}\n".encode())
                    connection.sendall(f"F_ACKS|{json.dumps(R_f)}\n".encode())
                    connection.sendall(f"VIEW|{json.dumps(group_view)}\n".encode())
                    pr(f"Sent infos to {ip, port, new_id}")
                buff = ""
                while "\n" not in buff:
                    data = connection.recv(4096).decode()
                    buff += data
                ack = buff.strip()
                if ack == "JOINED":
                    FIFO_multicast(f"NEW|{new_ip}|{new_port}|{new_id}")
                    pr(f"Let everyone know that {new_ip, new_port, new_id} has joined")
                else:
                    pr("Didn't receive JOINED ACK")
                done = True

            # elif msg.startswith("B_ACK|"):
            #     Seq_number = int(msg.split("|")[1])
            #     id = msg.split("|")[2]
            #     # pr(f"Received B_ACK from {id} about message {Seq_number}")
            #     with sent_messages_lock:
            #         if Seq_number in sent_messages:
            #             acked_from = sent_messages[Seq_number]["acks"]
            #             acked_from.add(id)
            #             if all(e in sent_messages[Seq_number]["acks"] for e in group_view):
            #                 # pr(f"Now deleting msg ({sent_messages[Seq_number]["msg"]}) from sent_messages")
            #                 del sent_messages[Seq_number]
            #     done = True

            elif msg.startswith("NACK|"):
                s = int(msg.split("|")[1])
                r = int(msg.split("|")[2])
                with multicast_socket_lock:
                    for i in range(r+1, s):
                        multicast_socket.sendto(f"{i}|{server_id}|{sent_messages[i]["msg"]}".encode(), (MULTICAST_GROUP, MULTICAST_PORT))
                done = True
            else:
                connection.sendall(b"Incorrect formatting!\n")
                done = True

    connection.close()

def restore_order_book(book):
    restored = {}

    for symbol, value in book.items():
        if symbol == "Balance":
            restored[symbol] = (
                [tuple(entry) for entry in value]
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
    pr("Listening to broadcast announcements")

    while not crashed:
        data, addr = broadcast_socket.recvfrom(1024)
        if data == "CRASH" and not crashed:
            #election()
            pr("starting election")
    broadcast_socket.close()

def crash():
    global crashed, broadcast_address, broadcast_port, server_port
    crashed = True
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.sendto(str.encode(f"CRASH|{server_port}|{server_id}"), (broadcast_address, broadcast_port))
    FIFO_multicast(f"CRASH")
    multicast_socket.close()
    # notify leader who then distributes new view of servers

def heartbeat():
    global crashed, broadcast_address, broadcast_port, server_port
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    while not crashed:
        broadcast_socket.sendto(f"HELLO|{server_port}|{server_id}".encode(), (broadcast_address, broadcast_port))
        # pr("sent heartbeat")
        time.sleep(1)
    broadcast_socket.close()

if __name__ == "__main__":

    set_broadcast_address()
    pr("Broadcast:", broadcast_address)

    # listen for client connections
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((server_address, 0))
    server_port = server_socket.getsockname()[1]
    pr("Local IP:", server_address)
    pr("Local Port:", server_port)
    pr("Local ID:", server_id)

    connection_listener_thread = threading.Thread(target=connection_listen, daemon=True).start()
    b_listener_thread = threading.Thread(target=B_listen, daemon=True).start()
    discovery()
    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True).start()

    pr(f"Server running on {server_address}:{server_port}:{server_id}")

    while True:
        command = input("Enter command: ")
        if command == "crash":
            crash()
            break
