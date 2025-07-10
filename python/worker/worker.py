# python/worker/worker.py

import socket
import threading
import pickle
import math
import numpy as np
import os
import concurrent.futures

# --- CONFIGURACIÓN DEL WORKER ---
# Se pide el puerto al iniciar para poder lanzar varios workers fácilmente.
try:
    PORT = int(input("Introduce el puerto para este worker (ej. 9001, 9002, 9003): "))
    HOST = 'localhost'
except ValueError:
    print("Puerto inválido. Saliendo.")
    exit()

# --- FUNCIONES DE PROCESAMIENTO PARALELIZADO ---
# Estas funciones procesan una pequeña parte (un sub-chunk) de los datos.

def sub_procesar_math1(sub_chunk):
    """Aplica la operación matemática compleja a un sub-chunk."""
    return [((math.sin(x) + math.cos(x)) ** 2) / (math.sqrt(abs(x)) + 1) for x in sub_chunk]

def sub_procesar_condicional(sub_chunk):
    """Aplica la operación condicional a un sub-chunk, con resiliencia local."""
    resultado_parcial = []
    for x in sub_chunk:
        try:
            # La condición del Ejemplo 2
            if x % 3 == 0 or (500 <= x <= 1000):
                # La operación puede fallar si x <= 0, demostrando resiliencia
                resultado_parcial.append((x * math.log(x)) % 7)
            else:
                resultado_parcial.append(x)
        except (ValueError, TypeError):
            # Si un hilo/cálculo falla, no detiene el resto.
            # Se registra un valor de error y se continúa.
            resultado_parcial.append(-1) # Valor de error
    return resultado_parcial

# --- FUNCIÓN PRINCIPAL DE PROCESAMIENTO (ORQUESTADOR) ---

def procesar(chunk, operacion):
    """
    Función orquestadora que divide el trabajo y lo ejecuta en paralelo.
    MEJORA: Usa un ThreadPool para paralelizar el procesamiento del chunk
    utilizando los núcleos de la CPU disponibles en el worker.
    """
    num_threads = os.cpu_count() or 4  # Usar todos los núcleos disponibles, o 4 como fallback
    # Dividir el chunk en partes iguales para cada hilo
    sub_chunks = np.array_split(np.array(chunk), num_threads)
    
    resultado_final = []

    # Seleccionar la función de trabajo correcta
    if operacion == 'math1':
        target_func = sub_procesar_math1
    elif operacion == 'condicional':
        target_func = sub_procesar_condicional
    else:
        return chunk # Si la operación no se conoce, devolver el chunk original

    # Usar un ThreadPoolExecutor para gestionar los hilos
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # map() aplica la función a cada sub-chunk en un hilo separado
        # y devuelve los resultados en orden.
        resultados_parciales = executor.map(target_func, sub_chunks)
        
        # Unir los resultados de todos los hilos
        for res in resultados_parciales:
            resultado_final.extend(res)
            
    return resultado_final

# --- LÓGICA DE RED (SIN CAMBIOS, YA ERA CORRECTA) ---

def recibir_todo(conn):
    """Recibe datos del socket, respetando el tamaño enviado."""
    size_data = conn.recv(4)
    if not size_data:
        return b""
    msg_size = int.from_bytes(size_data, 'big')
    
    buffer = b""
    while len(buffer) < msg_size:
        parte = conn.recv(min(4096, msg_size - len(buffer)))
        if not parte:
            break
        buffer += parte
    return buffer

def manejar_cliente(conn, addr):
    """Maneja una conexión entrante de un maestro."""
    print(f"[WORKER:{PORT}] Conexión aceptada de {addr}")
    try:
        data = recibir_todo(conn)
        if not data:
            print(f"[WORKER:{PORT}] No se recibieron datos de {addr}.")
            return
            
        chunk, operacion = pickle.loads(data)
        print(f"[WORKER:{PORT}] Recibido chunk de tamaño {len(chunk)} para operación '{operacion}'.")
        print(f"[WORKER:{PORT}] Iniciando procesamiento paralelo con {os.cpu_count() or 4} hilos...")
        
        resultado = procesar(chunk, operacion)
        
        print(f"[WORKER:{PORT}] Procesamiento completado. Enviando resultado...")
        payload = pickle.dumps(resultado)
        conn.sendall(len(payload).to_bytes(4, 'big'))
        conn.sendall(payload)
        print(f"[WORKER:{PORT}] Resultado enviado correctamente a {addr}.")
        
    except Exception as e:
        print(f"[WORKER:{PORT}] Error manejando al cliente {addr}: {e}")
    finally:
        conn.close()
        print(f"[WORKER:{PORT}] Conexión con {addr} cerrada.")

def main():
    """Función principal que inicia el servidor del worker."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Permite reusar el puerto
        s.bind((HOST, PORT))
        s.listen()
        print(f"✅ [WORKER] Worker iniciado en {HOST}:{PORT}. Esperando conexiones del maestro...")
        while True:
            conn, addr = s.accept()
            # Se crea un hilo por cada petición del maestro para no bloquear el servidor
            thread = threading.Thread(target=manejar_cliente, args=(conn, addr))
            thread.start()

if __name__ == '__main__':
    main()