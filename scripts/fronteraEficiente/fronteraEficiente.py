import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import minimize

# ====================
# Datos de los activos
# ====================
tickers = ['AAPL', 'NVDA', 'ALB','AMD']
start_date = '2025-01-01'
end_date = '2025-07-01'

N = len(tickers)

# ==========================================================
# Obtener Matriz de Covarianza y vector rendimiento esperado
# ==========================================================
def obtener_datos_y_calculos(tickers, start_date, end_date):
    datos = yf.download(tickers, start=start_date, end=end_date,auto_adjust=False)['Adj Close']
    rendimientos = datos.pct_change().dropna()
    matriz_covarianzas = rendimientos.cov()
    vector_rendimientos = rendimientos.mean()
    return matriz_covarianzas, vector_rendimientos

Sigma, mu = obtener_datos_y_calculos(tickers, start_date, end_date)

# ==========================
# Datos para el problema QP
# ========================== 
def objetivo(w, Sigma):
    return np.dot(w.T, np.dot(Sigma, w))

def restriccion_suma_pesos(w):
    return np.sum(w) - 1

def restriccion_rendimiento(w, mu, mu_0):
    return np.dot(w, mu) - mu_0

w_init = np.ones(N) / N

limites = [(0,1)] * N

# ===================================
# Encontramos dos portafolios óptimos
# ===================================
pesos_optimos = []
for mu_0 in [min(mu),max(mu)]:
    varianza = minimize(
        objetivo,
        w_init,
        args=(Sigma,),
        method='SLSQP',
        bounds=limites,
        constraints = (
            {'type': 'eq', 'fun': restriccion_suma_pesos},
            {'type': 'eq', 'fun': restriccion_rendimiento, 'args': (mu, mu_0)}
            )
    )
    pesos_optimos.append(varianza.x)


w_1 = pesos_optimos[0]
w_2 = pesos_optimos[1]

# ======================================================
# Parametrización de la la Recta Eficiente en R^N (TMFT) 
# ======================================================
t = np.linspace(0, 1, 50) 
W = t[:, np.newaxis] * w_1 + (1 - t[:, np.newaxis]) * w_2

# Evaluaciones en el plano varianza-rendimiento

Sigma = Sigma.to_numpy() 
mu = mu.to_numpy()

puntos = []
for w in W:
    puntos.append((objetivo(w, Sigma),np.dot(w,mu)))


# ============================
# Grafica del Markowitz Bullet
# ============================

# Extraemos los rendimientos y riesgos de la frontera eficiente
target_volatilities, target_returns = zip(*puntos)

# Graficamos la frontera eficiente
plt.figure(figsize=(10,6))
plt.plot(target_volatilities, target_returns, label='Frontera Eficiente', color='green')
plt.title('Markowitz Bullet')
plt.xlabel('Varianza')
plt.ylabel('Rendimiento Esperado')
plt.grid(True)
plt.show()
