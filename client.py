import socket
import threading
import time

broadcast_port = 1234

server_address = None
server_port = None

servers = {}  # ip -> {port, rtt}
lock = threading.Lock()

tcp_sock = None
tcp_lock = threading.Lock()

def broadcast_listener():
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_sock.bind(("0.0.0.0", broadcast_port))

    print("[Broadcast Listener] Waiting for HELLO/CRASH...")

    while True:
        data, _ = udp_sock.recvfrom(1024)
        parts = data.decode().strip().split("|")
        if not parts:
            continue

        if parts[0] == "HELLO" and len(parts) >= 3:
            ip, port = parts[1], int(parts[2])
            with lock:
                servers[ip] = {"port": port, "rtt": float("inf")}

        elif parts[0] == "CRASH" and len(parts) >= 2:
            crashed_ip = parts[1]
            with lock:
                servers.pop(crashed_ip, None)


def measure_server_rtt(ip, port, timeout=2):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)

        start = time.time()
        s.connect((ip, port))
        s.sendall(b"PING\n")
        data = s.recv(1024)
        rtt = time.time() - start

        s.close()
        # optional: verify PONG
        if not data:
            return float("inf")
        return rtt
    except Exception:
        return float("inf")


def rtt_probe_loop():
    while True:
        time.sleep(5)

        with lock:
            items = list(servers.items())  # (ip, info)

        for ip, info in items:
            port = info["port"]
            rtt = measure_server_rtt(ip, port)

            with lock:
                if ip in servers:
                    servers[ip]["rtt"] = rtt

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
    while True:
        time.sleep(5)

        with lock:
            if not servers:
                continue
            best_ip = min(servers, key=lambda ip: servers[ip]["rtt"])
            best_port = servers[best_ip]["port"]
            best_rtt = servers[best_ip]["rtt"]

        # only switch if we actually have a measurement
        if best_rtt == float("inf"):
            continue

        if server_address != best_ip:
            print(f"[Selector] Switching to best server {best_ip}:{best_port} (rtt={best_rtt:.4f}s)")
            switch_connection(best_ip, best_port)


if __name__ == "__main__":
    threading.Thread(target=broadcast_listener, daemon=True).start()
    threading.Thread(target=rtt_probe_loop, daemon=True).start()
    threading.Thread(target=select_best_server_loop, daemon=True).start()

    print("Waiting until at least one server is discovered...")
    while True:
        with lock:
            if servers:
                break
        time.sleep(0.2)

    while True:
        with tcp_lock:
            if tcp_sock is not None:
                break
        time.sleep(0.2)

    while True:
        msg = input("Enter the message: ")
        with tcp_lock:
            s = tcp_sock
        if not s:
            print("No active connection.")
            continue

        s.sendall((msg + "\n").encode())
        resp = s.recv(1024)
        if resp:
            print(resp.decode(errors="replace"))
