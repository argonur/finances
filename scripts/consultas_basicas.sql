CREATE TABLE activos (
    id SERIAL PRIMARY KEY,
    ticker TEXT UNIQUE NOT NULL,
    nombre TEXT NOT NULL,
    tipo TEXT CHECK (tipo IN ('accion', 'indice')) NOT NULL
);

CREATE TABLE eventos (
    id SERIAL PRIMARY KEY,
    activo_id INTEGER NOT NULL REFERENCES activos (id) ON DELETE CASCADE,
    activo_nombre TEXT NOT NULL,
    fecha DATE NOT NULL,
    evento TEXT NOT NULL,
    tipo TEXT CHECK (
        tipo IN ('dividendo', 'split')
    ) NOT NULL,
    UNIQUE (activo_id, fecha, tipo)
);

CREATE TABLE datos_historicos (
    id SERIAL PRIMARY KEY,
    activo_id INTEGER NOT NULL REFERENCES activos (id) ON DELETE CASCADE,
    activo_nombre TEXT NOT NULL,
    fecha DATE NOT NULL,
    apertura NUMERIC,
    maximo NUMERIC,
    minimo NUMERIC,
    cierre NUMERIC,
    cierre_ajustado NUMERIC,
    volumen BIGINT,
    UNIQUE (activo_id, fecha)
);

CREATE TABLE fear_greed (
    id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    indice NUMERIC,
    fear_greed_index TEXT NOT NULL,
    UNIQUE (fecha)
);

-- Listar todos los activos
SELECT * FROM activos ORDER BY ticker;

-- Buscar un activo por ticker
SELECT * FROM activos WHERE ticker = '^SPX';

-- Promedio mensual de cierre por activo
SELECT
    activo_nombre,
    DATE_TRUNC('month', fecha) AS mes,
    ROUND(AVG(cierre), 2) AS promedio_cierre
FROM datos_historicos
GROUP BY
    activo_nombre,
    mes
ORDER BY activo_nombre, mes;

-- Rango de fechas con datos historicos
SELECT
    activo_nombre,
    MIN(fecha) AS primera_fecha,
    MAX(fecha) AS ultima_fecha,
    COUNT(*) AS total_dias
FROM datos_historicos
GROUP BY
    activo_nombre
ORDER BY primera_fecha;

-- Calcular rendimiento porcentual entre 2 fechas
SELECT
    activo_nombre,
    MAX(cierre) AS cierre_final,
    MIN(cierre) AS cierre_inicial,
    ROUND(
        (
            (MAX(cierre) - MIN(cierre)) / MIN(cierre)
        ) * 100,
        2
    ) AS rendimiento_porcentual
FROM datos_historicos
WHERE
    fecha BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY
    activo_nombre;