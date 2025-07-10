from python.protocol.worker_comm import enviar_a_worker

def enviar_con_replicacion(workers, chunk, operacion):
    """
    Envía el chunk a dos workers (principal y réplica).
    Si el principal falla, intenta con la réplica.
    """
    try:
        # Principal
        return enviar_a_worker(workers[0], chunk, operacion)
    except Exception as e:
        print(f"[REPLICATOR] Worker {workers[0]} falló: {e}. Recuperando desde réplica...")
        try:
            # Réplica
            return enviar_a_worker(workers[1], chunk, operacion)
        except Exception as e2:
            print(f"[REPLICATOR] Réplica {workers[1]} también falló: {e2}")
            return None