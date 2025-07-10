import time

def log_event(msg):
    print(f"[LOG {time.strftime('%H:%M:%S')}] {msg}")