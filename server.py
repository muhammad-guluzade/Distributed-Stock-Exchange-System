import socket
import ipaddress
import threading
import time
import uuid
from threading import get_native_id

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

order_book = {"NVIDIA": ([(200, 24, "Max"),(198, 55, "Paul")], [(205, 14, "Max"),(206, 66,  "Paul")],{"Max": 10000, "Paul":250}),
              "APPLE":([],[],{}),
              "Balance": {"Max": 5000, "Paul":12000}}
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
new_servers = []
changed_servers_lock = threading.Lock()
inc_changes = []
multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
multicast_socket_lock = threading.Lock()

leader = None
leader_lock = threading.Lock()
isLeader = False
leader_socket = None
leader_queue_lock = threading.Lock()
leader_queue = []

election_results_lock = threading.Lock()
election_results = {}

sent_messages = {}
sent_messages_lock = threading.Lock()

client_lock = threading.Lock()
client_list = {}

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
    # pr("B delivered message:", msg)
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
        if not crashed:
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
    global leader, isLeader, inc_changes, leader_socket
    pr("FIFO delivered message: ", msg, "from", id)
    if msg == "CRASH":
        with group_view_lock:
            ip, port = group_view[id]
        pr(f"Server ({ip}:{port}|{id}) crashed!")

        with group_view_lock:
            if id in group_view and id is not leader:
                inc_changes.append(f"CRASH|{id}")

        if isLeader :
            FIFO_multicast(f"CONFIRMCRASH|{id}")

        if id is leader:
            FIFO_deliver(f"CONFIRMCRASH|{id}")

    if msg.startswith("CONFIRMCRASH"):
        id = msg.split("|")[1]
        if not received_first_view:
            with changed_servers_lock:
                crashed_servers.append(id)
        else:
            with group_view_lock:
                change = f"CRASH|{id}"
                if id in group_view:
                    if change in inc_changes:
                        inc_changes.remove(change)
                        del group_view[id]
            election(change)
            pr("Starting election")

    if msg.startswith("NEW|"):
        ip = msg.split("|")[1]
        port = int(msg.split("|")[2])
        id = msg.split("|")[3]
        pr(f"New server joined ({ip}:{port}:{id})")
        inc_changes.append(f"NEW|{ip}|{port}|{id}")

        if isLeader :
            FIFO_multicast(f"CONFIRMNEW|{ip}|{port}|{id}")

    if msg.startswith("CONFIRMNEW|"):
        ip = msg.split("|")[1]
        port = int(msg.split("|")[2])
        id = msg.split("|")[3]

        if not received_first_view:
            with changed_servers_lock:
                new_servers[id] = (ip, port)
        else:
            with group_view_lock:
                group_view[id] = (ip, port)
            if id not in R_b:
                with R_b_lock, R_f_lock:
                    R_b[id] = 0
                    R_f[id] = 0
                election(f"NEW|{id}", )
                pr("Starting election")

    if msg.startswith("AVG|"):
        avg = float(msg.split("|")[1])
        group_size = int(msg.split("|")[2])
        identifier = msg.split("|", 3)[3]
        with election_results_lock:
            if identifier not in election_results:
                election_results[identifier] = {}
            election_results[identifier][id] = avg

            if len(election_results[identifier]) == group_size:
                results = election_results[identifier]

                best_sid = min(results, key=lambda sid: (results[sid], sid))
                best_rtt = results[best_sid]

                pr(f"Election winner: {best_sid} (RTT {best_rtt})")

                with leader_lock:
                    leader = best_sid
                    if leader == server_id:
                        isLeader = True
                        for msg in inc_changes:
                            FIFO_multicast("CONFIRM" + msg)
                        inc_changes = []
                        leader_socket = None
                    else:
                        leader_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        leader_socket.connect((group_view[leader][0], group_view[leader][1]))
                with leader_queue_lock:
                    for msg in leader_queue:
                        send_to_leader(msg)
                del election_results[identifier]

    if msg.startswith("UPDATE|") and id != server_id:
        updates = json.loads(msg.split("|", 1)[1])
        pr(f"Applying updates {updates}")
        apply_updates(updates)


def election(identifier):
    global isLeader, leader_socket
    with leader_lock:
        if leader_socket is not None:
            leader_socket.close()
            leader_socket = None
        isLeader = False
    res = []
    with group_view_lock:
        group = group_view.values()
    for ip, port in group:
        rtt = measure_server_rtt(ip, port)
        res.append(rtt)
    avg = sum(res) / len(res)

    with group_view_lock:
        FIFO_multicast(f"AVG|{avg}|{len(group_view)}|{identifier}")


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
                    with group_view_lock, changed_servers_lock:
                        group_view = json.loads(line.split("|")[1])
                        pr("Received view:", group_view)
                        for crashed_server in crashed_servers:
                            del group_view[crashed_server]
                        for new_server in new_servers:
                            group_view[new_server] = new_servers[new_server]
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


def update(update):
    changes = make_matches(update)
    apply_updates(changes)
    if len(changes) > 0:
        FIFO_multicast(f"UPDATE|{json.dumps(changes)}")

def external_pay_out(uname, amount):
    print(f"Transferred {-amount}€ to external account of {uname}")

def external_pay_in(uname, amount):
    print(f"Received {amount}€ from external account of {uname}")

def external_stock_in(uname, stock, amount):
    print(f"Received {amount} stocks of {stock} from external account of {uname}")

def external_stock_out(uname, stock, amount):
    print(f"Transferred {-amount} stocks of {stock} to external account of {uname}")

def make_matches(update):

    updates = []
    if update.startswith("BALANCE_CHANGE|"):
        uname = update.split("|")[1]
        amount = int(update.split("|")[2])
        with order_book_lock:
            if uname not in order_book["Balance"]:
                order_book["Balance"][uname] = 0

            if amount < 0:
                total_buy_orders = 0
                for possible_stock in order_book:
                    if possible_stock != "Balance":
                        for pr, amt, own in order_book[possible_stock][0]:
                            if own == uname:
                                total_buy_orders += pr * amt
                balance = max(order_book["Balance"][uname] - total_buy_orders, 0)

                if balance < -amount:
                    external_pay_out(uname, -balance)
                    return [f"BALANCE_CHANGE|{uname}|{-balance}"]
                else:
                    external_pay_out(uname, amount)
            external_pay_in(uname, amount)
            return [update.strip()]

    elif update.startswith("STOCK_TRANSFER|"):
        _, stock, uname, amount = update.strip().split("|")
        amount = int(amount)
        with (order_book_lock):
            if stock not in order_book:
                order_book[stock] = ([],[],{})
            stock_balances = order_book[stock][2]
            if uname not in stock_balances:
                stock_balances[uname] = 0

            if amount < 0:
                total_sell_orders = 0
                for pr, amt, own in order_book[stock][1]:
                      if own == uname:
                          total_sell_orders += amt

                stock_balance = order_book[stock][2].get(uname, 0) - total_sell_orders

                if stock_balance < -amount:
                    external_stock_out(uname, stock, -stock_balance)
                    return [f"STOCK_TRANSFER|{stock}|{uname}|{-stock_balance}"]
                else:
                    external_stock_out(uname, stock, amount)
            external_stock_in(uname, stock, amount)
            return [update.strip()]
    elif update.startswith("BUY_ORDER"):
        _, stock, price, amount, uname = update.strip().split("|")
        price = int(price)
        amount = int(amount)

        with order_book_lock:
            total_buy_orders = 0
            for possible_stock in order_book:
                if possible_stock != "Balance":
                    for pr, amt, own in order_book[possible_stock][0]:
                        if own == uname:
                            total_buy_orders += pr * amt
            balance = max(order_book["Balance"][uname] - total_buy_orders, 0)
            max_affordable = balance // price
            amount = min(amount, max_affordable)
            if stock not in order_book:
                order_book[stock] = ([],[],{})
            if len(order_book[stock][1]) > 0:
                running_amount = 0
                cost = 0
                sell_orders = order_book[stock][1]

                i = 0
                while i < len(sell_orders):
                    offer_price = sell_orders[i][0]
                    offer_amount = sell_orders[i][1]
                    offer_user = sell_orders[i][2]
                    print("amount, running amount", amount, running_amount)
                    print("offer infos: ", offer_price, offer_amount, offer_user)
                    if offer_price > price:
                        updates.append(f"PUT_BUY_ORDER|{stock}|{price}|{amount - running_amount}|{uname}")
                        break

                    elif offer_amount < amount - running_amount:
                        running_amount += offer_amount
                        cost += offer_amount * offer_price
                        print("running amount", running_amount)
                        print("offer amount", offer_amount)
                        print("offer price", offer_price)
                        print("cost", cost)
                        updates.append(f"BALANCE_CHANGE|{offer_user}|{offer_amount * offer_price}")
                        updates.append(f"STOCK_TRANSFER|{stock}|{offer_user}|{-offer_amount}")
                        updates.append(f"REDUCE_SELL_ORDER|{stock}|{offer_price}|{offer_amount}|{offer_user}|{offer_amount}")
                        i += 1

                    elif offer_amount >= amount - running_amount:
                        bought_amount = amount - running_amount
                        running_amount += bought_amount
                        cost += bought_amount * offer_price
                        updates.append(f"BALANCE_CHANGE|{offer_user}|{bought_amount * offer_price}")
                        updates.append(f"STOCK_TRANSFER|{stock}|{offer_user}|{-bought_amount}")
                        updates.append(f"REDUCE_SELL_ORDER|{stock}|{offer_price}|{offer_amount}|{offer_user}|{bought_amount}")
                        break
                if cost != 0:
                    updates.append(f"BALANCE_CHANGE|{uname}|{-cost}")
                if running_amount != 0:
                    updates.append(f"STOCK_TRANSFER|{stock}|{uname}|{running_amount}")
                if i == len(sell_orders) and running_amount < amount:
                    updates.append(f"PUT_BUY_ORDER|{stock}|{price}|{amount - running_amount}|{uname}")
            else:
                updates.append(f"PUT_BUY_ORDER|{stock}|{price}|{amount}|{uname}")

    elif update.startswith("SELL_ORDER"):
        _, stock, price, amount, uname = update.strip().split("|")
        price = int(price)
        amount = int(amount)

        with order_book_lock:
            if stock not in order_book:
                return []
            if uname not in order_book[stock][2]:
                return []

            total_sell_orders = 0
            for pr, amt, own in order_book[stock][1]:
                  if own == uname:
                      total_sell_orders += amt

            stock_balance = order_book[stock][2].get(uname, 0) - total_sell_orders
            max_sellable = max(stock_balance, 0)
            amount = min(amount, max_sellable)

            if len(order_book[stock][0]) > 0:
                running_amount = 0
                gain = 0
                buy_orders = order_book[stock][0]

                i = 0
                while i < len(buy_orders):
                    offer_price = buy_orders[i][0]
                    offer_amount = buy_orders[i][1]
                    offer_user = buy_orders[i][2]
                    if offer_price < price:
                        updates.append(f"PUT_SELL_ORDER|{stock}|{price}|{amount - running_amount}|{uname}")
                        break

                    elif offer_amount < amount - running_amount:
                        running_amount += offer_amount
                        gain += offer_amount * offer_price
                        updates.append(f"BALANCE_CHANGE|{offer_user}|{- offer_amount * offer_price}")
                        updates.append(f"STOCK_TRANSFER|{stock}|{offer_user}|{offer_amount}")
                        updates.append(f"REDUCE_BUY_ORDER|{stock}|{offer_price}|{offer_amount}|{offer_user}|{offer_amount}")
                        i += 1

                    elif offer_amount >= amount - running_amount:
                        sold_amount = amount - running_amount
                        running_amount += sold_amount
                        gain += sold_amount * offer_price
                        updates.append(f"BALANCE_CHANGE|{offer_user}|{-sold_amount * offer_price}")
                        updates.append(f"STOCK_TRANSFER|{stock}|{offer_user}|{sold_amount}")
                        updates.append(f"REDUCE_BUY_ORDER|{stock}|{offer_price}|{offer_amount}|{offer_user}|{sold_amount}")
                        break
                if gain != 0:
                    updates.append(f"BALANCE_CHANGE|{uname}|{gain}")
                if running_amount != 0:
                    updates.append(f"STOCK_TRANSFER|{stock}|{uname}|{-running_amount}")
                if i == len(buy_orders) and running_amount < amount:
                    updates.append(f"PUT_SELL_ORDER|{stock}|{price}|{amount - running_amount}|{uname}")
            else:
                updates.append(f"PUT_SELL_ORDER|{stock}|{price}|{amount}|{uname}")
    else:
        raise Exception

    return updates

def apply_updates(updates):
    with order_book_lock:
        for update in updates:
            if update.startswith("BALANCE_CHANGE|"):
                _, uname, amount = update.strip().split("|")
                amount = int(amount)
                order_book["Balance"][uname] = order_book["Balance"].get(uname, 0) + amount

            elif update.startswith("STOCK_TRANSFER|"):
                _, stock, uname, amount = update.strip().split("|")
                amount = int(amount)
                if stock not in order_book:
                    order_book[stock] = ([], [], {})
                stock_balances = order_book[stock][2]
                stock_balances[uname] = stock_balances.get(uname, 0) + amount
                if stock_balances[uname] == 0:
                    order_book[stock][2].pop(uname, None)

            elif update.startswith("PUT_BUY_ORDER|"):
                _, stock, price, amount, uname = update.strip().split("|")
                price = int(price)
                amount = int(amount)
                if stock not in order_book:
                    order_book[stock] = ([],[],{})
                buy_orders = order_book[stock][0]

                i = 0
                while i < len(buy_orders) and buy_orders[i][0] >= price:
                    i += 1

                buy_orders.insert(i, (price, amount, uname))


            elif update.startswith("PUT_SELL_ORDER|"):
                _, stock, price, amount, uname = update.strip().split("|")
                price = int(price)
                amount = int(amount)
                if stock not in order_book:
                    raise Exception
                sell_orders = order_book[stock][1]

                i = 0
                while i < len(sell_orders) and sell_orders[i][0] <= price:
                    i += 1

                sell_orders.insert(i, (price, amount, uname))

            elif update.startswith("REDUCE_BUY_ORDER|"):
                _, stock, price, amount, uname, by = update.strip().split("|")
                price = int(price)
                amount = int(amount)
                by = int(by)

                if stock not in order_book:
                    raise Exception
                buy_orders = order_book[stock][0]
                for i, (p, a, u) in enumerate(buy_orders):
                    if p == price and a == amount and u == uname:
                        new_amount = a - by

                        if new_amount > 0:
                            buy_orders[i] = (p, new_amount, u)
                        elif new_amount == 0:
                            buy_orders.pop(i)  # fully filled
                        else:
                            raise Exception
                        break

            elif update.startswith("REDUCE_SELL_ORDER|"):
                _, stock, price, amount, uname, by = update.strip().split("|")
                price = int(price)
                amount = int(amount)
                by = int(by)

                if stock not in order_book:
                    raise Exception
                sell_orders = order_book[stock][1]

                for i, (p, a, u) in enumerate(sell_orders):
                    if p == price and a == amount and u == uname:
                        new_amount = a - by

                        if new_amount > 0:
                            sell_orders[i] = (p, new_amount, u)
                        elif new_amount == 0:
                            sell_orders.pop(i)  # fully filled
                        else:
                            raise Exception
                        break
            else:
                raise Exception


def get_client_id(ip, port):
    with client_lock:
        return client_list[(ip,port)]

def send_info_client(connection):
    ip, port = connection.getpeername()
    uname = get_client_id(ip, port)
    with order_book_lock:
        book = {k: v[:2] for k, v in order_book.items() if k != "Balance"}
        balance = order_book["Balance"].get(uname, 0)
        stocks = {s: v[2][uname] for s, v in order_book.items() if
                  s != "Balance" and uname in v[2]}  # = {"NVIDIA": 2000, "APPLE":10000}
    connection.sendall(f"ORDERS|{json.dumps(book)}\n".encode())
    connection.sendall(f"BALANCE|{balance}\n".encode())
    connection.sendall(f"OWNED_STOCKS|{json.dumps(stocks)}\n".encode())

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
                connection.sendall("PONG\n".encode())
                done = True

            elif msg.startswith("LOGIN|"):
                uname = msg.split("|")[1]
                with client_lock:
                    client_list[(ip, port)] = uname

            elif msg == "REFRESH":
                send_info_client(connection)

            elif msg.startswith("BALANCE_CHANGE|"):
                amount = msg.split("|")[1]
                client_id = get_client_id(ip, port)
                send_to_leader(f"BALANCE_CHANGE|{client_id}|{amount}\n")

            elif msg.startswith("STOCK_TRANSFER|"):
                stock = msg.split("|")[1]
                amount = msg.split("|")[2]
                client_id = get_client_id(ip, port)
                send_to_leader(f"STOCK_TRANSFER|{stock}|{client_id}|{amount}\n")

            elif msg.startswith("BUY_ORDER|"):
                stock = msg.split("|")[1]
                price = msg.split("|")[2]
                amount = msg.split("|")[3]
                client_id = get_client_id(ip, port)
                send_to_leader(f"BUY_ORDER|{stock}|{price}|{amount}|{client_id}\n")

            elif msg.startswith("SELL_ORDER"):
                stock = msg.split("|")[1]
                price = msg.split("|")[2]
                amount = msg.split("|")[3]
                client_id = get_client_id(ip, port)
                send_to_leader(f"SELL_ORDER|{stock}|{price}|{amount}|{client_id}\n")

            elif msg == "LOGOUT":
                with client_lock:
                    del client_list[(ip, port)]
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
                connection.sendall("Incorrect formatting!\n".encode())
                done = True

    connection.close()


def send_to_leader(msg):
    with leader_lock:
        if leader_socket is not None:
            leader_socket.sendall(msg.encode())
        elif isLeader:
            update(msg)
        else:
            with leader_queue_lock:
                leader_queue.append(msg)

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
