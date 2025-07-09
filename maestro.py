import socket
import threading
import pickle
import math
import numpy as np
from DArrayDouble import DArrayDouble
from DArrayInt import DArrayInt

WORKERS = [('localhost', 9001), ('localhost', 9002), ('localhost', 9003)]
CHUNK_SIZE = 10000 // len(WORKERS)

def dividir_array(array):
    n = len(WORKERS)
    tam = len(array)
    base = tam // n
    resto = tam % n
    partes = []
    inicio = 0
    for i in range(n):
        fin = inicio + base + (1 if i < resto else 0)
        partes.append(array[inicio:fin])
        inicio = fin
    return partes

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

def operacion(x):
    return ((math.sin(x) + math.cos(x)) ** 2) / (math.sqrt(abs(x)) + 1)

def main():
    print("[MAESTRO] Iniciando procesamiento...")

    # Usar DArrayDouble
    arr_double = DArrayDouble()
    for i in range(10000):
        arr_double.append(float(i) + 1.0)
    partes = arr_double.dividir(len(WORKERS))
    print(f"[MAESTRO] Partes DArrayDouble: {[len(p) for p in partes]}")

    # Usar DArrayInt
    arr_int = DArrayInt()
    for i in range(10000):
        arr_int.append(i + 1)
    partes_int = arr_int.dividir(len(WORKERS))
    print(f"[MAESTRO] Partes DArrayInt: {[len(p) for p in partes_int]}")

    # Elige qué partes enviar a los workers:
    # partes = partes      # para doubles
    # partes = partes_int  # para enteros

    # Ejemplo usando partes_int:
    partes = partes_int
    # Ejemplo usando partes
    #partes = partes  # para doubles

    hilos = []
    resultados = [None] * len(WORKERS)

    def worker_thread(i, addr, chunk):
        try:
            resultados[i] = enviar_a_worker(addr, chunk, 'math1')
            print(f"[MAESTRO] Resultado parcial de {addr}: {resultados[i][:3]}")
        except Exception as e:
            print(f"[ERROR] Worker {addr} fallo: {e}")

    for i, addr in enumerate(WORKERS):
        t = threading.Thread(target=worker_thread, args=(i, addr, partes[i]))
        hilos.append(t)
        t.start()

    for t in hilos:
        t.join()

    print("[MAESTRO] Resultados recibidos:", resultados)
    try:
        final = np.concatenate([r for r in resultados if r is not None])
        print("[MAESTRO] Procesamiento completo. Resultado ejemplo:", final[:5])
    except ValueError:
        print("[MAESTRO] No se pudo concatenar resultados: Todos los workers fallaron.")

    print("[MAESTRO] Finalizando ejecución.")

if __name__ == '__main__':
    main()