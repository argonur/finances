# Sistemas de Recolección y Migración de Datos Financieros

## Sistema Automatizado de Recolección de Índices S&P 500 y Nasdaq-100

- Los datos históricos previamente almacenados se encuentran en el directorio `/data/HistoricalData_[indice].csv` en total son 2 archivos.
- El propósito del script es:
  - Determinar el ultimo día hábil (si hoy es lunes, se buscara datos del viernes anterior).
  - Considerar que la bolsa no opera los fines de semana ni días festivos.
  - Obtener datos financieros (API’ no oficial, yfinance de acceso gratuito).
  - Descargar los precios de cierre de los índices específicos S&P 500 `(^SPX)` y Nasdaq-100 `(^NDX)`
  - Guardar las nuevas entradas en los archivos CSV.

### Como usar

Dentro del directorio `/scripts/` ejecutar el siguiente comando: `python3 historical_index.py`

```bash
# Salida esperada:

==================================================
MENÚ DE ACTUALIZACIÓN DE ÍNDICES BURSÁTILES
==================================================
1. Ejecutar en modo automático (recomendado post-cierre)
2. Ejecutar en modo manual (ahora mismo)
3. Ingresar fecha específica (formato MM/DD/AAAA)
4. Salir

Seleccione una opción:
```

1. _Opción 1_ : Busca el ultimo cierre bursátil de manera automática.
   1. Si la bolsa ya ha cerrado (4 PM, hora local de Nueva York) busca, guarda y respalda en la base de datos el ultimo índice.
   2. Si la bolsa aun no ha cerrado, no buscara el ultimo cierre.
2. _Opción 2_: Busca el ultimo cierre bursátil incluso si la bolsa aun no ha cerrado (Recomendado para buscar el ultimo cierre inmediato)
   1. Considere que si la bolsa aun no ha cerrado y fuerza el buscar y almacenar el índice del día actual, la información puede no ser 100% fiable
3. _Opción 3_: Buscar una fecha en particular y almacenar el cierre.

---

## Sistema de Migración de Datos Históricos, Dividendos y Reportes de Ajustes

- El directorio `/scripts/datos` es el contenedor de todos los **datos históricos, eventos (Dividendos - Split)** de los activos o índices.
- Si el directorio no existe, los datos no están actualizados o no existen, ejecutar el siguiente comando dentro de `/scripts`:

```bash
python3 dividendos.py
```

Este comando, creara el directorio `/datos` dentro de \*/scripts, descargara y organizara toda la información dentro de directorios con el nombre del activo/índice de la siguiente manera:

```bash
📁 /scripts/datos/
  ├── 📁 Apple/
  │   ├── 📁 datos_completos
  │   ├── 📁 reportes_ajustes
  │   └── 📁 reportes_eventos
  ├── 📁 Microsoft/
  └── 📁 S&P 500/
```

---

Ahora que los datos existen y están actualizados procedemos a respaldar la información en nuestra base de datos en Neón, ejecuta el siguiente comando:

```bash
/scripts> python3 migration_stock_historical_data.py
# Salida esperada:
✅ Cargados 40 activos desde JSON

======================================================================
###################### INICIO DE CARGA DE DATOS ######################
======================================================================

Conexión exitosa a NeonDB
✅ Activos cargados exitosamente
❌ Error cargando eventos: cannot access local variable 'archivo_reciente' where it is not associated with a value
✅ Datos cargados para S&P500 - 10206 registros
✅ Datos cargados para NASDAQ-100 - 10018 registros
✅ Datos cargados para Apple Inc. - 10206 registros
✅ Datos cargados para Amazon.com Inc. - 7079 registros
✅ Datos cargados para Adobe Inc. - 9799 registros
✅ Datos cargados para Alibaba Group Holding Ltd. - 2714 registros
✅ Datos cargados para Berkshire Hathaway Inc. - 7336 registros
✅ Datos cargados para Ford Motor Co. - 10206 registros
✅ Datos cargados para Taiwan Semiconductor Manufacturing Co. - 6977 registros
✅ Datos cargados para Alibaba Group Holding Ltd. - 2714 registros
✅ Datos cargados para Apple Inc. - 10206 registros
✅ Datos cargados para Microsoft Corp. - 9905 registros
✅ Datos cargados para Alphabet Inc. - 5253 registros
✅ Datos cargados para Amazon.com Inc. - 7079 registros
✅ Datos cargados para Tesla Inc. - 3778 registros
✅ Datos cargados para Meta Platforms Inc. - 3301 registros
✅ Datos cargados para Netflix Inc. - 5817 registros
✅ Datos cargados para NVIDIA Corp. - 6654 registros
✅ Datos cargados para PayPal Holdings Inc. - 2516 registros
✅ Datos cargados para Salesforce.com Inc. - 5293 registros
✅ Datos cargados para Adobe Inc. - 9799 registros
✅ Datos cargados para Intuit Inc. - 8135 registros
✅ Datos cargados para Oracle Corp. - 9906 registros
✅ Datos cargados para International Business Machines Corp. - 10206 registros
✅ Datos cargados para Walmart Inc. - 10206 registros
✅ Datos cargados para Exxon Mobil Corp. - 10206 registros
✅ Datos cargados para Chevron Corp. - 10206 registros
✅ Datos cargados para Berkshire Hathaway Inc. - 7336 registros
✅ Datos cargados para Visa Inc. - 4352 registros
✅ Datos cargados para JPMorgan Chase & Co. - 10206 registros
✅ Datos cargados para UnitedHealth Group Inc. - 10206 registros
✅ Datos cargados para Procter & Gamble Co. - 10206 registros
✅ Datos cargados para The Home Depot Inc. - 10206 registros
✅ Datos cargados para Walt Disney Co. - 10206 registros
✅ Datos cargados para Verizon Communications Inc. - 10206 registros
✅ Datos cargados para Intel Corp. - 10206 registros
✅ Datos cargados para Cisco Systems Inc. - 8910 registros
✅ Datos cargados para PepsiCo Inc. - 10206 registros
✅ Datos cargados para Coca-Cola Co. - 10206 registros
✅ Datos cargados para AT&T Inc. - 10206 registros
✅ Datos históricos cargados exitosamente
🔌 Conexión cerrada

======================================================================
########################## CARGA COMPLETADA ##########################
======================================================================
```

---

## Base de datos (Neón → PostgreSQL)

Los Query para crear las tablas en la base de datos se detallan a continuación:

- Tabla para activos

```sql
CREATE TABLE activos (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) UNIQUE NOT NULL,
    nombre VARCHAR(100) NOT NULL,  -- ¡Quitamos UNIQUE individual!
    tipo VARCHAR(20) CHECK (tipo IN ('indice', 'accion')),
    UNIQUE (id, nombre)  -- Restricción compuesta necesaria
);
```

- Tabla para eventos

```sql
-- Crear tabla eventos (con redundancia controlada)
CREATE TABLE eventos (
    id SERIAL PRIMARY KEY,
    activo_id INTEGER NOT NULL REFERENCES activos(id) ON DELETE CASCADE,
    activo_nombre VARCHAR(100) NOT NULL,
    fecha DATE NOT NULL,
    evento NUMERIC(10,4) NOT NULL,
    tipo VARCHAR(20) CHECK (tipo IN ('dividendo', 'split')),
    UNIQUE (activo_id, fecha, tipo),
    FOREIGN KEY (activo_id, activo_nombre) REFERENCES activos(id, nombre) ON UPDATE CASCADE
);
```

- Tabla para datos históricos

```sql
-- Tabla datos_historicos (redundancia controlada)
CREATE TABLE datos_historicos (
    id SERIAL PRIMARY KEY,
    activo_id INTEGER NOT NULL REFERENCES activos(id) ON DELETE CASCADE,
    activo_name VARCHAR(100) NOT NULL,
    fecha DATE NOT NULL,
    apertura NUMERIC(12,6),
    maximo NUMERIC(12,6),
    minimo NUMERIC(12,6),
    cierre NUMERIC(12,6),
    cierre_ajustado NUMERIC(12,6),
    volumen BIGINT,
    UNIQUE (activo_id, fecha),
    FOREIGN KEY (activo_id, activo_name) REFERENCES activos(id, nombre) ON UPDATE CASCADE
);
```

La `String` de conexión de la base de datos a nuestro script la puede generar en su Dashboard de [Neón](https://neon.com/), pegar la cadena en una variable de entorno dentro del archivo `.env` con un nombre.

## Por ultimo, modificar el nombre de la variable de entorno dentro de los scripts.

Querys basicos para consultar informacion de las tablas, ejecutarlas en el SQL Editor de Neón.

- Consultar de la tabla activos todos los datos de la columna tickers

```sql
    SELECT * FROM activos WHERE tickers;
```

- Consultar de la tabla eventos solo los `Split`

```sql

```
