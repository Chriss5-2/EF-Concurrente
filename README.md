# README.md
# Proyecto Final: Librería Distribuida, Concurrente y Tolerante a Fallos

## Requisitos
- Python 3.x
- NumPy

## Archivos
- `maestro.py`: ejecuta el nodo principal
- `worker.py`: ejecuta los nodos trabajadores (3 instancias en diferentes terminales)
- `DArrayDouble.py`: clase simple para dividir arrays

## Instrucciones
1. Ejecuta 3 terminales para workers:
```
python worker.py  # Puerto 9001
python worker.py  # Puerto 9002
python worker.py  # Puerto 9003
```
2. Luego ejecuta el maestro (desde la raíz):
```
python -m python.maestro.maestro
```

## Qué hace
- Crea un array de 10,000 elementos y lo distribuye
- Cada worker aplica una función matemática compleja usando hilos
- El maestro recibe los resultados y los combina