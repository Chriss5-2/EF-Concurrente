import socket
import pickle
from .utils import log_event # Usar import relativo

def _recibir_todo(sock):
    """
    Función auxiliar para recibir datos del socket, respetando el tamaño enviado.
    Primero lee 4 bytes para determinar el tamaño del payload.
    """
    # Recibir los 4 bytes que indican el tamaño del mensaje
    size_data = sock.recv(4)
    if not size_data:
        return b""
    msg_size = int.from_bytes(size_data, 'big')
    
    # Ahora recibir exactamente esa cantidad de bytes
    buffer = b""
    while len(buffer) < msg_size:
        parte = sock.recv(min(4096, msg_size - len(buffer)))
        if not parte:
            # La conexión se cerró inesperadamente
            break
        buffer += parte
    return buffer

def enviar_a_worker(worker_addr, chunk, operacion):
    """
    Conecta a un worker, le envía una tarea y espera una respuesta.
    Utiliza el protocolo [tamaño][payload] en ambas direcciones.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        log_event(f"Conectando a {worker_addr}...", "COMM")
        s.connect(worker_addr)
        log_event(f"Conectado a {worker_addr}, enviando tarea.", "COMM")
        
        # Serializar y enviar la tarea
        payload = pickle.dumps((chunk, operacion))
        s.sendall(len(payload).to_bytes(4, 'big')) # Enviar tamaño
        s.sendall(payload)                         # Enviar payload
        
        log_event(f"Tarea enviada a {worker_addr}. Esperando respuesta...", "COMM")
        
        # Recibir la respuesta usando la lógica correcta
        data_recibida = _recibir_todo(s)
        
        if not data_recibida:
            log_event(f"No se recibió respuesta de {worker_addr}", "ERROR")
            raise ConnectionAbortedError("No se recibió respuesta del worker")
            
        log_event(f"Respuesta recibida de {worker_addr}.", "COMM")
        return pickle.loads(data_recibida)