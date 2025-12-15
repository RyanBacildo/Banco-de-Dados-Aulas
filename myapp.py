import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation



X, y = gerar_dataset()

# ----- Gradiente Descendente -----
m = 0
b = 0
L = 0.0001
iteracoes = 2000

historico_m = []
historico_b = []

for i in range(iteracoes):
    Y_pred = m * X + b

    derivadam = np.sum(2 * X * (Y_pred - y))
    derivada_b = np.sum(2 * (Y_pred - y))

    m -= L * derivadam
    b -= L * derivada_b

    historico_m.append(m)
    historico_b.append(b)

# ----- Animação -----
fig, ax = plt.subplots()
ax.scatter(X, y, color="blue")
linha, = ax.plot(X, historico_m[0] * X + historico_b[0], color="red")

ax.set_title("Treinamento com Gradiente Descendente")
ax.set_xlabel("X")
ax.set_ylabel("y")

def atualizar(frame):
    m = historico_m[frame]
    b = historico_b[frame]
    linha.set_ydata(m * X + b)
    ax.set_title(f"Iteração: {frame+1} | m={m:.3f} | b={b:.3f}")
    return linha,

anim = FuncAnimation(fig, atualizar, frames=len(historico_m), interval=5000)

plt.show()