# python/maestro/maestro.py

import threading
import time
import numpy as np
import random

# --- IMPORTACIONES DEL PROYECTO ---
# Usamos las rutas relativas correctas
from python.darray.DArrayDouble import DArrayDouble
from python.darray.DArrayInt import DArrayInt
from python.worker.replicator import enviar_con_replicacion
from python.protocol.utils import log_event
from python.check.heartbeat import check_worker_alive

# --- CONFIGURACIÓN DEL CLÚSTER ---
# Lista de todos los workers potenciales del clúster
WORKERS = [('localhost', 9001), ('localhost', 9002), ('localhost', 9003)]

# --- ESTADO COMPARTIDO Y GESTIÓN DE WORKERS ACTIVOS ---
# MEJORA: Usamos un conjunto para la lista de workers activos por eficiencia
# y un Lock para garantizar la seguridad en el acceso concurrente.
ACTIVE_WORKERS = set()
WORKER_LOCK = threading.Lock()

def heartbeat_monitor():
    """
    Hilo que se ejecuta en segundo plano para comprobar qué workers están vivos.
    MEJORA: Mantiene actualizada la lista global ACTIVE_WORKERS.
    """
    while True:
        for worker_addr in WORKERS:
            is_alive = check_worker_alive(worker_addr, timeout=1)
            with WORKER_LOCK:
                if is_alive and worker_addr not in ACTIVE_WORKERS:
                    log_event(f"Worker {worker_addr} detectado como ACTIVO.", "HEARTBEAT")
                    ACTIVE_WORKERS.add(worker_addr)
                elif not is_alive and worker_addr in ACTIVE_WORKERS:
                    log_event(f"Worker {worker_addr} detectado como CAÍDO.", "HEARTBEAT")
                    ACTIVE_WORKERS.remove(worker_addr)
        time.sleep(5)  # Chequear cada 5 segundos

def worker_thread_task(chunk, operacion, resultados, index):
    """
    Función ejecutada por cada hilo del maestro.
    MEJORA: Elige dinámicamente un worker principal y una réplica de la lista de activos.
    """
    log_event(f"Iniciando tarea para el chunk {index}...", "TAREA")
    
    with WORKER_LOCK:
        active_list = list(ACTIVE_WORKERS)
    
    if len(active_list) == 0:
        log_event(f"No hay workers activos. La tarea para el chunk {index} no se puede procesar.", "ERROR")
        resultados[index] = None
        return

    # Elegir worker principal y réplica de forma dinámica
    # Esto asegura que no se envíen tareas a workers caídos.
    principal = random.choice(active_list)
    replicas_posibles = [w for w in active_list if w != principal]
    
    replica = random.choice(replicas_posibles) if replicas_posibles else None
    
    workers_pair = [principal]
    if replica:
        workers_pair.append(replica)
        log_event(f"Chunk {index}: Principal={principal}, Réplica={replica}", "TAREA")
    else:
        log_event(f"Chunk {index}: Principal={principal}. No hay réplica disponible.", "TAREA")

    # La función de replicación se encarga de la lógica de fallo y recuperación
    resultado = enviar_con_replicacion(workers_pair, chunk, operacion)
    
    if resultado is not None:
        log_event(f"Chunk {index} procesado con éxito.", "TAREA")
    else:
        log_event(f"Chunk {index} falló incluso con réplica. No se pudo procesar.", "ERROR")
    
    resultados[index] = resultado

def ejecutar_ejemplo(nombre_ejemplo, darray, operacion):
    """Función genérica para ejecutar los ejemplos y mostrar resultados."""
    log_event(f"--- Iniciando {nombre_ejemplo} ---", "MAESTRO")
    
    with WORKER_LOCK:
        num_workers = len(ACTIVE_WORKERS)
    
    if num_workers == 0:
        log_event("No hay workers activos para distribuir el trabajo. Abortando.", "ERROR")
        return
        
    partes = darray.dividir(num_workers)
    log_event(f"Array dividido en {len(partes)} partes para {num_workers} workers activos.", "MAESTRO")
    
    resultados = [None] * len(partes)
    hilos = []
    
    for i, chunk in enumerate(partes):
        t = threading.Thread(target=worker_thread_task, args=(chunk, operacion, resultados, i))
        hilos.append(t)
        t.start()
        
    for t in hilos:
        t.join()
        
    log_event("Todos los hilos de trabajo han terminado.", "MAESTRO")
    
    # Consolidar resultados
    resultados_validos = [r for r in resultados if r is not None]
    
    if len(resultados_validos) < len(partes):
        log_event("Advertencia: No se recibieron todos los resultados. El resultado final puede ser incompleto.", "MAESTRO")
        
    if not resultados_validos:
        log_event("No se pudo obtener ningún resultado. El procesamiento falló.", "ERROR")
    else:
        try:
            resultado_final = np.concatenate(resultados_validos)
            log_event(f"Procesamiento completo. Tamaño del resultado final: {len(resultado_final)}", "MAESTRO")
            log_event(f"Ejemplo de resultado: {resultado_final[:5]}", "MAESTRO")
        except ValueError as e:
            log_event(f"Error al consolidar resultados: {e}", "ERROR")

def main():
    """Función principal del maestro."""
    log_event("Iniciando Maestro...", "MAESTRO")

    # Iniciar el monitor de heartbeat en un hilo separado
    monitor_hilo = threading.Thread(target=heartbeat_monitor, daemon=True)
    monitor_hilo.start()
    log_event("Monitor de Heartbeat iniciado.", "MAESTRO")
    
    # Dar tiempo al heartbeat para detectar workers iniciales
    log_event("Esperando 3 segundos para la detección inicial de workers...", "MAESTRO")
    time.sleep(3)

    # --- EJEMPLO 1: Procesamiento Matemático Paralelo (DArrayDouble) ---
    arr_double = DArrayDouble()
    for i in range(20000):
        arr_double.append(float(i))
    ejecutar_ejemplo("Ejemplo 1: Procesamiento Matemático", arr_double, 'math1')

    print("\n" + "="*50 + "\n")
    
    # --- EJEMPLO 2: Evaluación Condicional y Resiliencia Local (DArrayInt) ---
    arr_int = DArrayInt()
    for i in range(10001):
        arr_int.append(i)
    ejecutar_ejemplo("Ejemplo 2: Evaluación Condicional", arr_int, 'condicional')

    log_event("Maestro ha finalizado todas las tareas.", "MAESTRO")

if __name__ == '__main__':
    main()