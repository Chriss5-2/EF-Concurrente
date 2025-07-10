class DArrayInt:
    def __init__(self, capacity=4):
        self._capacity = capacity
        self._size = 0
        self._data = [0] * self._capacity

    def append(self, value):
        if self._size == self._capacity:
            self._resize(2 * self._capacity)
        self._data[self._size] = value
        self._size += 1

    def _resize(self, new_capacity):
        new_data = [0] * new_capacity
        for i in range(self._size):
            new_data[i] = self._data[i]
        self._data = new_data
        self._capacity = new_capacity

    def get(self, index):
        if 0 <= index < self._size:
            return self._data[index]
        else:
            raise IndexError("Index out of bounds")

    def set(self, index, value):
        if 0 <= index < self._size:
            self._data[index] = value
        else:
            raise IndexError("Index out of bounds")

    def pop(self):
        if self._size == 0:
            raise IndexError("Pop from empty array")
        value = self._data[self._size - 1]
        self._size -= 1
        return value

    def size(self):
        return self._size

    def __str__(self):
        return str([self._data[i] for i in range(self._size)])

    def dividir(self, n):
        tam = self._size
        base = tam // n
        resto = tam % n
        partes = []
        inicio = 0
        for i in range(n):
            fin = inicio + base + (1 if i < resto else 0)
            partes.append(self._data[inicio:fin])
            inicio = fin
        return partes