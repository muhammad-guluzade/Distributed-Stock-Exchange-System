import socket
import threading
import time

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

def broadcast_listener():
    global server_address, server_port, tcp_sock
    broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    broadcast_sock.bind(("0.0.0.0", broadcast_port))

    print("[Broadcast Listener] Waiting for HELLO/CRASH...")

    while True:
        data, addr = broadcast_sock.recvfrom(1024)
        msg = data.decode().strip()
        ip = addr[0]

        parts = msg.split("|")

        if len(parts) != 2:
            continue  # ignore malformed packet

        msg, port_str = parts

        try:
            port = int(port_str)
        except ValueError:
            continue  # bad port, ignore packet

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
        s.sendall("PING".encode())
        data = s.recv(1024)
        rtt = time.time() - start
        s.close()

        if not data:
            return float("inf")
        return rtt
    except socket.timeout:
        return float("inf")

def connect_tcp(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    return s


def switch_connection(ip, port):
    global tcp_sock, server_address, server_port

    new_sock = connect_tcp(ip, port)

    with tcp_lock:
        old = tcp_sock
        tcp_sock = new_sock
        server_address, server_port = ip, port

    if old:
        try:
            old.close()
        except Exception:
            pass


def select_best_server_loop():
    global connection_lost_logged

    while True:
        time.sleep(5)

        with lock:
            nodes = list(servers)

        # 🚨 NO SERVERS AT ALL
        if not nodes:
            with tcp_lock:
                no_connection = tcp_sock is None

            if no_connection and not connection_lost_logged:
                print("🔍 No servers available. Waiting and retrying...")
                connection_lost_logged = True
            continue

        # Measure RTTs
        best = None
        best_rtt = float("inf")

        for ip, port in nodes:
            rtt = measure_server_rtt(ip, port)

            if rtt < best_rtt:
                best_rtt = rtt
                best = (ip, port)

        # 🚨 Servers exist but none reachable
        if best is None or best_rtt == float("inf"):
            with tcp_lock:
                no_connection = tcp_sock is None

            if no_connection and not connection_lost_logged:
                print("🔍 Servers found but none responding. Retrying...")
                connection_lost_logged = True
            continue

        # A working server exists
        connection_lost_logged = False

        with tcp_lock:
            cur = (server_address, server_port)

        if cur != best:
            print("🔁 Connecting to best server:", best)
            switch_connection(*best)


def user_input_loop():
    global tcp_sock, server_address, server_port

    while True:
        msg = input("Enter the message:\n")

        with tcp_lock:
            s = tcp_sock

        if not s:
            print("❌ No server connected. Waiting for failover...")
            continue

        try:
            s.sendall(msg.encode())
            resp = s.recv(1024)
            if resp:
                print("Server replied:", resp.decode())

        except Exception:
            print("⚠ Connection lost!")
            global connection_lost_logged
            connection_lost_logged = False

            with tcp_lock:
                try: s.close()
                except: pass
                tcp_sock = None
                server_address = None
                server_port = None



if __name__ == "__main__":
    threading.Thread(target=broadcast_listener, daemon=True).start()
    threading.Thread(target=select_best_server_loop, daemon=True).start()

    print("Waiting until at least one server is discovered...")
    while True:
        with lock:
            if servers:
                first = next(iter(servers))
                break
        time.sleep(0.2)

    switch_connection(*first)

    threading.Thread(target=user_input_loop, daemon=True).start()

    while True:
        time.sleep(1)

