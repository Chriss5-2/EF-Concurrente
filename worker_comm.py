import socket
import pickle

def recibir_todo(sock):
    buffer = b""
    while True:
        parte = sock.recv(4096)
        if not parte:
            break
        buffer += parte
    return buffer

def enviar_a_worker(worker_addr, chunk, operacion):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        print(f"[MAESTRO] Conectando a {worker_addr}...")
        s.connect(worker_addr)
        print(f"[MAESTRO] Conectado a {worker_addr}, enviando datos...")
        payload = pickle.dumps((chunk, operacion))
        s.sendall(len(payload).to_bytes(4, 'big'))  # Enviar tamaño primero
        s.sendall(payload)
        data = recibir_todo(s)
    return pickle.loads(data)