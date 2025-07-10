import time

def log_event(msg, context="INFO"):
    """
    Imprime un mensaje de log con formato, incluyendo un contexto opcional.
    
    Args:
        msg (str): El mensaje a registrar.
        context (str, optional): La etiqueta de contexto (ej. "MAESTRO", "ERROR"). 
                                 Por defecto es "INFO".
    """
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] [{context.upper()}] {msg}")