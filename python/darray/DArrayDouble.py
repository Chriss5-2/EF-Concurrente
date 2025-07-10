class DArrayDouble:
    def __init__(self, capacidad_inicial=4):
        self.data = [0.0] * capacidad_inicial
        self.n = 0

    def append(self, valor):
        if self.n == len(self.data):
            self._expandir()
        self.data[self.n] = valor
        self.n += 1

    def pop(self):
        if self.n == 0:
            raise IndexError("pop from empty DArrayDouble")
        self.n -= 1
        return self.data[self.n]

    def get(self, idx):
        if 0 <= idx < self.n:
            return self.data[idx]
        raise IndexError("index out of range")

    def set(self, idx, valor):
        if 0 <= idx < self.n:
            self.data[idx] = valor
        else:
            raise IndexError("index out of range")

    def size(self):
        return self.n

    def _expandir(self):
        self.data += [0.0] * len(self.data)

    def __str__(self):
        return str(self.data[:self.n])

    def dividir(self, n):
        tam = self.n
        base = tam // n
        resto = tam % n
        partes = []
        inicio = 0
        for i in range(n):
            fin = inicio + base + (1 if i < resto else 0)
            partes.append(self.data[inicio:fin])
            inicio = fin
        return partes