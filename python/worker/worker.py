import socket
import threading
import pickle
import math
import numpy as np

HOST = 'localhost'
PORT = int(input("Puerto del worker (ej. 9001, 9002, 9003): "))

def recibir_todo(conn):
    # Recibe primero 4 bytes con el tamaño
    size_data = conn.recv(4)
    if not size_data:
        return b""
    size = int.from_bytes(size_data, 'big')
    buffer = b""
    while len(buffer) < size:
        parte = conn.recv(min(4096, size - len(buffer)))
        if not parte:
            break
        buffer += parte
    return buffer

def procesar(chunk, operacion):
    if operacion == 'math1':
        return [((math.sin(x) + math.cos(x)) ** 2) / (math.sqrt(abs(x)) + 1) for x in chunk]
    elif operacion == 'condicional':
        resultado = []
        for x in chunk:
            try:
                if x % 3 == 0 or (500 <= x <= 1000):
                    resultado.append((x * math.log(x)) % 7)
                else:
                    resultado.append(x)
            except:
                resultado.append(-1)
        return resultado
    return chunk

def manejar_cliente(conn):
    try:
        print(f"[WORKER:{PORT}] Esperando datos del cliente...")
        data = recibir_todo(conn)
        print(f"[WORKER:{PORT}] Datos recibidos, deserializando...")
        chunk, operacion = pickle.loads(data)
        print(f"[WORKER:{PORT}] Recibido chunk de tamaño {len(chunk)} con operacion '{operacion}'")
        resultado = procesar(chunk, operacion)
        conn.sendall(pickle.dumps(resultado))
        print(f"[WORKER:{PORT}] Resultado enviado correctamente")
    except Exception as e:
        print(f"[WORKER:{PORT}] Error: {e}")
    finally:
        conn.close()

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"[WORKER:{PORT}] Esperando conexiones...")
        while True:
            conn, _ = s.accept()
            threading.Thread(target=manejar_cliente, args=(conn,)).start()

if __name__ == '__main__':
    main()