import socket
import time

def check_worker_alive(addr, timeout=2):
    try:
        with socket.create_connection(addr, timeout=timeout):
            return True
    except Exception:
        return False

def heartbeat_monitor(workers, interval=5):
    while True:
        for addr in workers:
            alive = check_worker_alive(addr)
            print(f"[HEARTBEAT] Worker {addr} {'vivo' if alive else 'caído'}")
        time.sleep(interval)