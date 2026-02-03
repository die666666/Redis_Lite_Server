import socket
import threading
import time
import json
import os

HOST = "127.0.0.1"
PORT = 6379

store = {}
expiry = {}
lock = threading.Lock()
DUMP_FILE = "dump.json"


# ---------- Persistence ----------
def load_db():
    if os.path.exists(DUMP_FILE):
        with open(DUMP_FILE, "r") as f:
            data = json.load(f)
            store.update(data.get("store", {}))
            expiry.update(data.get("expiry", {}))


def save_db():
    with open(DUMP_FILE, "w") as f:
        json.dump({"store": store, "expiry": expiry}, f)


# ---------- Expiry ----------
def is_expired(key):
    if key in expiry and time.time() >= expiry[key]:
        store.pop(key, None)
        expiry.pop(key, None)
        return True
    return False


# ---------- RESP ----------
def parse_resp(data):
    parts = data.split(b"\r\n")
    args = []
    i = 0
    while i < len(parts):
        if parts[i].startswith(b"$"):
            i += 1
            if i < len(parts):
                args.append(parts[i].decode())
        i += 1
    return args


# ---------- Client Handler ----------
def handle_client(conn, addr):
    print(f"Client connected: {addr}")
    with conn:
        while True:
            data = conn.recv(4096)
            if not data:
                break

            try:
                args = parse_resp(data)
                if not args:
                    conn.sendall(b"-ERR empty command\r\n")
                    continue

                cmd = args[0].upper()

                with lock:
                    # PING
                    if cmd == "PING":
                        conn.sendall(b"+PONG\r\n")

                    # ECHO
                    elif cmd == "ECHO" and len(args) == 2:
                        msg = args[1]
                        conn.sendall(f"${len(msg)}\r\n{msg}\r\n".encode())

                    # SET key value [EX|PX|EXAT|PXAT time]
                    elif cmd == "SET" and len(args) >= 3:
                        key, value = args[1], args[2]
                        store[key] = value
                        expiry.pop(key, None)

                        i = 3
                        while i < len(args):
                            opt = args[i].upper()
                            t = int(args[i + 1])

                            if opt == "EX":
                                expiry[key] = time.time() + t
                            elif opt == "PX":
                                expiry[key] = time.time() + t / 1000
                            elif opt == "EXAT":
                                expiry[key] = t
                            elif opt == "PXAT":
                                expiry[key] = t / 1000
                            else:
                                conn.sendall(b"-ERR syntax error\r\n")
                                break
                            i += 2

                        conn.sendall(b"+OK\r\n")

                    # GET
                    elif cmd == "GET" and len(args) == 2:
                        key = args[1]
                        if key not in store or is_expired(key):
                            conn.sendall(b"$-1\r\n")
                        else:
                            val = store[key]
                            conn.sendall(f"${len(val)}\r\n{val}\r\n".encode())

                    # EXISTS
                    elif cmd == "EXISTS" and len(args) == 2:
                        key = args[1]
                        exists = key in store and not is_expired(key)
                        conn.sendall(b":1\r\n" if exists else b":0\r\n")

                    # DEL
                    elif cmd == "DEL" and len(args) >= 2:
                        deleted = 0
                        for k in args[1:]:
                            if k in store:
                                store.pop(k, None)
                                expiry.pop(k, None)
                                deleted += 1
                        conn.sendall(f":{deleted}\r\n".encode())

                    # INCR
                    elif cmd == "INCR" and len(args) == 2:
                        key = args[1]
                        val = int(store.get(key, "0")) + 1
                        store[key] = str(val)
                        conn.sendall(f":{val}\r\n".encode())

                    # DECR
                    elif cmd == "DECR" and len(args) == 2:
                        key = args[1]
                        val = int(store.get(key, "0")) - 1
                        store[key] = str(val)
                        conn.sendall(f":{val}\r\n".encode())

                    # LPUSH
                    elif cmd == "LPUSH" and len(args) >= 3:
                        key = args[1]
                        store.setdefault(key, [])
                        if not isinstance(store[key], list):
                            conn.sendall(b"-ERR wrong type\r\n")
                        else:
                            for v in args[2:]:
                                store[key].insert(0, v)
                            conn.sendall(f":{len(store[key])}\r\n".encode())

                    # RPUSH
                    elif cmd == "RPUSH" and len(args) >= 3:
                        key = args[1]
                        store.setdefault(key, [])
                        if not isinstance(store[key], list):
                            conn.sendall(b"-ERR wrong type\r\n")
                        else:
                            store[key].extend(args[2:])
                            conn.sendall(f":{len(store[key])}\r\n".encode())

                    # SAVE
                    elif cmd == "SAVE":
                        save_db()
                        conn.sendall(b"+OK\r\n")

                    else:
                        conn.sendall(b"-ERR unknown or invalid command\r\n")

            except Exception as e:
                print("Error:", e)
                conn.sendall(b"-ERR server error\r\n")


# ---------- Server ----------
def start_server():
    load_db()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"Redis Lite running on {HOST}:{PORT}")

        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    start_server()
