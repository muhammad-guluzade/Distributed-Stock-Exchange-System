import json
import socket
import threading
import time
import os

broadcast_port = 1234

server_address = None
server_port = None

servers = set()   # {(ip, port)}
lock = threading.Lock()

tcp_sock = None
tcp_lock = threading.Lock()

switch_request = None
switch_lock = threading.Lock()
connection_lost_logged = False

crashed = False

username = ""

order_book = None
balance = None
owned_stocks = None


def broadcast_listener():
    global server_address, server_port, tcp_sock
    broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    broadcast_sock.bind(("0.0.0.0", broadcast_port))

    print("[Broadcast Listener] Waiting for HELLO/CRASH...")

    while not crashed:
        data, addr = broadcast_sock.recvfrom(1024)
        msg = data.decode().strip()
        ip = addr[0]
        parts = msg.split("|")

        msg, port_str, id_str = parts
        port = int(port_str)

        with lock:
            if msg == "HELLO":
                servers.add((ip, port))
            elif msg == "CRASH":
                dead = (ip, port)
                servers.discard(dead)

                with tcp_lock:
                    cur = (server_address, server_port)

                    if cur == dead:
                        print("⚠ Current server crashed. Disconnecting...")
                        global connection_lost_logged
                        connection_lost_logged = False

                        try:
                            tcp_sock.close()
                        except:
                            pass

                        tcp_sock = None
                        server_address = None
                        server_port = None


def measure_server_rtt(ip, port, timeout=2):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)

        start = time.time()
        s.connect((ip, port))
        s.sendall("PING\n".encode())
        data = s.recv(1024)
        rtt = time.time() - start
        s.close()

        if not data:
            return float("inf")
        return rtt
    except socket.timeout:
        return float("inf")
    except ConnectionRefusedError:
        return float("inf")

def connect_tcp(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    return s


def switch_connection(ip, port):
    global tcp_sock, server_address, server_port

    with tcp_lock:
        if tcp_sock is not None:
            tcp_sock.sendall(f"LOGOUT\n".encode())
            tcp_sock.close()
    new_sock = connect_tcp(ip, port)

    with tcp_lock:
        tcp_sock = new_sock
        server_address, server_port = ip, port
        tcp_sock.sendall(f"LOGIN|{username}\n".encode())


def select_best_server_loop():
    global connection_lost_logged

    while not crashed:
        time.sleep(5)

        with lock:
            nodes = list(servers)

        if not nodes:
            with tcp_lock:
                no_connection = tcp_sock is None

            if no_connection and not connection_lost_logged:
                print("🔍 No servers available. Waiting and retrying...")
                connection_lost_logged = True
            continue

        best = None
        best_rtt = float("inf")

        for ip, port in nodes:
            rtt = measure_server_rtt(ip, port)

            if rtt < best_rtt:
                best_rtt = rtt
                best = (ip, port)

        if best is None or best_rtt == float("inf"):
            with tcp_lock:
                no_connection = tcp_sock is None
            if no_connection:
                print(nodes)
            if no_connection and not connection_lost_logged:
                print("🔍 Servers found but none responding. Retrying...")
                connection_lost_logged = True
            continue

        connection_lost_logged = False

        with tcp_lock:
            cur = (server_address, server_port)

        if cur != best:
            print("🔁 Connecting to best server:", best)
            switch_connection(*best)

def clear():
    print("\n" * 50)


def display_market():
    clear()
    while order_book is None:
        time.sleep(0.5)

    print("====== MARKET ======\n")

    if order_book is None:
        print("No data yet...")
        return

    for stock, (buys, sells) in order_book.items():
        print(f"[{stock}]")

        print("  BUY (bids)")
        for p, a, u in buys:
            print(f"   {p:>6} | {a:>5} | {u}")

        print("  SELL (asks)")
        for p, a, u in sells:
            print(f"   {p:>6} | {a:>5} | {u}")

        print()

    print(f"Balance: {balance}")
    print(f"Owned: {owned_stocks}")
    print("====================")



def user_input_loop():
    global tcp_sock, server_address, server_port, crashed
    refresh = False
    while not crashed:
        # try:
            if not refresh:
                time.sleep(2.5)
            refresh = False
            display_market()
            msg = input(
                "Choose one:\n"
                "(1) Deposit money\n"
                "(2) Withdraw money\n"
                "(3) Deposit stock\n"
                "(4) Withdraw stock\n"
                "(5) Buy stock\n"
                "(6) Sell stock\n"
                "(7) Refresh market\n"
                "(8) Logout\n"
                "(9) Crash\n"
            )

            if msg == "1":
                amount = int(input("Amount to deposit: "))
                if amount <= 0:
                    print("Amount to deposit must be greater than 0")
                    time.sleep(2)
                    continue
                else:
                    with tcp_lock:
                        if tcp_sock:
                            tcp_sock.sendall(f"BALANCE_CHANGE|{amount}\n".encode())
                        else:
                            print("Your message could not be sent. Please try again.")

            elif msg == "2":
                amount = int(input("Amount to withdraw: "))
                if amount <= 0:
                    print("Amount to withdraw must be greater than 0")
                    time.sleep(2)
                    continue
                elif amount > balance:
                    print("You do not have enough funds")
                    time.sleep(2)
                    continue
                else:
                    with tcp_lock:
                        if tcp_sock:
                            tcp_sock.sendall(f"BALANCE_CHANGE|{-int(amount)}\n".encode())
                        else:
                            print("Your message could not be sent. Please try again.")

            elif msg == "3":
                stock = input("Stock symbol: ")
                if stock == "Balances":
                    print("Incorrect stock name")
                    time.sleep(2)
                    continue
                amount = int(input("Amount to deposit: "))
                if amount <= 0:
                    print("Amount to deposit must be greater than 0")
                    time.sleep(2)
                    continue
                with tcp_lock:
                    if tcp_sock:
                        tcp_sock.sendall(f"STOCK_TRANSFER|{stock}|{amount}\n".encode())
                    else:
                        print("Your message could not be sent. Please try again.")

            elif msg == "4":
                stock = input("Stock symbol: ")
                if stock == "Balances":
                    print("Incorrect stock name")
                    time.sleep(2)
                    continue
                if stock not in order_book or stock not in owned_stocks:
                    print("You don't own the stock")
                    time.sleep(2)
                    continue
                amount = int(input("Amount to withdraw: "))
                if amount <= 0:
                    print("Amount to withdraw must be greater than 0")
                    time.sleep(2)
                    continue
                if amount > int(owned_stocks[stock]):
                    print("You do not own enough stock")
                    time.sleep(2)
                with tcp_lock:
                        if tcp_sock:
                            tcp_sock.sendall(f"STOCK_TRANSFER|{stock}|{-int(amount)}\n".encode())
                        else:
                            print("Your message could not be sent. Please try again.")

            elif msg == "5":
                stock = input("Stock symbol: ")
                if stock == "Balances":
                    print("Incorrect stock name")
                    time.sleep(2)
                    continue

                price = int(input("Price per stock: "))
                if price < 0:
                    print("Price per stock must not be negative")
                    time.sleep(2)
                    continue
                amount = int(input("Amount: "))
                if amount <= 0:
                    print("Amount must be greater than 0")
                    time.sleep(2)
                    continue

                total_buy_orders = 0
                for possible_stock, (buy_orders, _) in order_book.items():
                    if possible_stock != "Balance":
                        for pr, amt, own in buy_orders:
                            if own == username:
                                total_buy_orders += int(pr) * int(amt)
                if price * amount >= balance - total_buy_orders:
                    print("You do not have enough funds")
                    time.sleep(2)
                    continue

                with tcp_lock:
                        if tcp_sock:
                            tcp_sock.sendall(f"BUY_ORDER|{stock}|{price}|{amount}\n".encode())
                        else:
                            print("Your message could not be sent. Please try again.")

            elif msg == "6":
                stock = input("Stock symbol: ")
                if stock == "Balances":
                    print("Incorrect stock name")
                    time.sleep(2)
                    continue
                if stock not in order_book or stock not in owned_stocks:
                    print("You do not own the stock")
                    time.sleep(2)
                    continue
                price = int(input("Price per stock: "))
                if price < 0:
                    print("Price per stock must not be negative")
                    time.sleep(2)
                    continue
                amount = int(input("Amount: "))
                if amount <= 0:
                    print("Amount must be greater than 0")
                    time.sleep(2)
                    continue

                total_sell_orders = 0
                for pr, amt, own in order_book[stock][1]:
                      if own == username:
                          total_sell_orders += int(amt)
                if  amount >= int(owned_stocks[stock]) - total_sell_orders:
                    print("You do not own enough stock")
                    time.sleep(2)
                    continue
                with tcp_lock:
                    if tcp_sock:
                        tcp_sock.sendall(f"SELL_ORDER|{stock}|{price}|{amount}\n".encode())
                    else:
                        print("Your message could not be sent. Please try again.")

            elif msg == "7":
                refresh = True
                continue

            elif msg == "8" or msg == "9":
                with tcp_lock:
                    tcp_sock.sendall("LOGOUT\n".encode())
                    tcp_sock.close()
                    tcp_sock = None
                crashed = True
                if msg == "8":
                    print("You logged out")
                break

        # except Exception:
        #     print("⚠ Connection lost!")
        #     global connection_lost_logged
        #     connection_lost_logged = True
        #
        #     with tcp_lock:
        #         try: tcp_sock.close()
        #         except: pass
        #         tcp_sock = None
        #         server_address = None
        #         server_port = None
        #     while not tcp_sock:
        #         time.sleep(2)

def refresher():
    global order_book, balance, owned_stocks
    while not crashed:
        with tcp_lock:
            if tcp_sock is not None:
                tcp_sock.sendall(f"REFRESH\n".encode())
                buffer = ""
                lines = []
                while not crashed and len(lines) < 3:
                    chunk = tcp_sock.recv(4096).decode()
                    if not chunk:
                        raise ConnectionError("Server closed connection during startup")

                    buffer += chunk
                    while "\n" in buffer and len(lines) < 3:
                        line, buffer = buffer.split("\n", 1)
                        lines.append(line)

                for line in lines:
                    if line.startswith("ORDERS|"):
                        order_book = json.loads(line.split("|", 1)[1])

                    elif line.startswith("BALANCE|"):
                        balance = int(line.split("|")[1])

                    elif line.startswith("OWNED_STOCKS|"):
                        owned_stocks = json.loads(line.split("|", 1)[1])
        time.sleep(2)

if __name__ == "__main__":
    username = input("Enter username:\n")
    # username = "Eric"
    print(f"Hello, {username}!")

    threading.Thread(target=refresher, daemon=True).start()
    threading.Thread(target=broadcast_listener, daemon=True).start()
    threading.Thread(target=select_best_server_loop, daemon=True).start()

    print("Waiting until at least one server is discovered...")
    while True:
        with lock:
            if servers:
                first = next(iter(servers))
                break
        time.sleep(0.2)

    print(f"Connecting to first server: {first[0]}:{first[1]}")
    switch_connection(*first)

    threading.Thread(target=user_input_loop, daemon=True).start()

    while not crashed:
        time.sleep(1)
