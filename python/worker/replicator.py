# Importar la función de comunicación corregida
from python.protocol.worker_comm import enviar_a_worker
from python.protocol.utils import log_event

def enviar_con_replicacion(workers, chunk, operacion):
    """
    Envía el chunk a los workers (principal y réplica).
    Si el principal falla, intenta con la réplica.
    
    Args:
        workers (list): Una lista con la tupla de dirección del worker principal
                        y opcionalmente la de la réplica.
        chunk: El fragmento de datos a procesar.
        operacion (str): El nombre de la operación a realizar.
    """
    if not workers:
        log_event("No hay workers especificados para enviar la tarea.", "ERROR")
        return None

    # Intentar con el worker principal
    principal = workers[0]
    try:
        log_event(f"Intentando procesar chunk con el worker principal {principal}.", "REPLICATOR")
        return enviar_a_worker(principal, chunk, operacion)
    except Exception as e:
        log_event(f"Worker principal {principal} falló: {e}. Intentando con réplica...", "REPLICATOR")
        
        # Si no hay réplica, el proceso falla aquí
        if len(workers) < 2:
            log_event(f"No hay réplica disponible para el worker {principal}. La tarea falló.", "ERROR")
            return None
            
        # Intentar con la réplica
        replica = workers[1]
        try:
            log_event(f"Recuperando tarea con la réplica {replica}.", "REPLICATOR")
            return enviar_a_worker(replica, chunk, operacion)
        except Exception as e2:
            log_event(f"La réplica {replica} también falló: {e2}. La tarea no se pudo completar.", "ERROR")
            return None