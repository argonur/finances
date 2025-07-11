from psycopg2 import extras
import pandas as pd 
import os
import sys
import json
# Añadir la ruta del modulo de conexion manualmente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_utils.db_connection import obtener_conexion

# Cargar Tipo de conexión de base de datos
TIPO_CONEXION = "local" # 'local' o 'neon'
# Conectar a la base de datos
conn = obtener_conexion(TIPO_CONEXION, "HISTORICAL")

def cargar_activos(conn, activos):
    """Carga los activos en la base de datos"""
    try:
        cur = conn.cursor()
        for activo in activos:
            # Determinar tipo de activo
            tipo = 'indice' if activo['ticker'].startswith('^') else 'accion'
            
            # Insertar o actualizar activo usando ticker como clave única
            query = """
            INSERT INTO activos (ticker, nombre, tipo)
            VALUES (%s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE
            SET nombre = EXCLUDED.nombre, tipo = EXCLUDED.tipo
            RETURNING id
            """
            cur.execute(query, (activo['ticker'], activo['nombre'], tipo))
            activo_id = cur.fetchone()[0]
            activo['id'] = activo_id
        
        conn.commit()
        print("✅ Activos cargados exitosamente")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Error cargando activos: {e}")
        return False

def cargar_eventos(conn, activos):
    """Carga los eventos desde archivos CSV a la base de datos"""
    try:
        cur = conn.cursor()
        for activo in activos:
            carpeta = f"./datos/{activo['nombre']}/reportes_eventos"
            
            if not os.path.exists(carpeta):
                print(f"⚠️ Carpeta no encontrada para eventos: {carpeta}")
                continue
                
            archivos = [f for f in os.listdir(carpeta) 
                    if f.startswith('Eventos') and f.endswith('.csv')]
            
            if not archivos:
                print(f"ℹ️ No se encontraron eventos para {activo['nombre']}")
                continue
                
            archivo_reciente = max(
                [os.path.join(carpeta, f) for f in archivos],
                key=os.path.getmtime
            )
            
            # Verificar tamaño después de tener archivo_reciente
            if os.path.getsize(archivo_reciente) == 0:
                print(f"ℹ️ Archivo vacío para eventos de {activo['nombre']}")
                continue
                
            try:
                df = pd.read_csv(archivo_reciente)
                if df.empty:
                    print(f"ℹ️ DataFrame vacío para eventos de {activo['nombre']}")
                    continue
                    
                # Convertir tipo a minúsculas y validar
                df['Tipo'] = df['Tipo'].str.lower().fillna('dividendo')
                df['Tipo'] = df['Tipo'].apply(lambda x: x if x in ['dividendo', 'split'] else 'dividendo')
                
                # Filtrar filas válidas
                df = df.dropna(subset=['Fecha', 'Evento'])
                
                # Insertar en lote
                for _, row in df.iterrows():
                    query = """
                    INSERT INTO eventos (activo_id, activo_nombre, fecha, evento, tipo)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (activo_id, fecha, tipo) DO NOTHING
                    """
                    cur.execute(query, (
                        activo['id'],
                        activo['nombre'],
                        row['Fecha'],
                        row['Evento'],
                        row['Tipo']
                    ))
                    
            except Exception as e:
                print(f"❌ Error procesando eventos para {activo['nombre']}: {e}")
                continue
        
        conn.commit()
        print("✅ Eventos cargados exitosamente")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Error cargando eventos: {e}")
        return False

from psycopg2 import extras  # Añadir al inicio del script

def cargar_datos_historicos(conn, activos):
    """Carga los datos históricos a la base de datos de manera optimizada"""
    try:
        cur = conn.cursor()
        for activo in activos:
            try:
                carpeta = f"./datos/{activo['nombre']}/datos_completos"
                if not os.path.exists(carpeta):
                    print(f"⚠️ Carpeta no encontrada para datos: {carpeta}")
                    continue
                
                # Buscar archivo más reciente
                archivos = [f for f in os.listdir(carpeta) 
                        if f.startswith('Datos_Completos') and f.endswith('.csv')]
                if not archivos:
                    print(f"ℹ️ No se encontraron datos para {activo['nombre']}")
                    continue
                
                archivo_reciente = max(
                    [os.path.join(carpeta, f) for f in archivos],
                    key=os.path.getmtime
                )
                
                # Leer y procesar datos
                df = pd.read_csv(archivo_reciente)
                if df.empty:
                    print(f"ℹ️ DataFrame vacío para datos de {activo['nombre']}")
                    continue
                
                # Normalizar nombres de columnas
                df.columns = df.columns.str.lower().str.replace(' ', '_')
                
                # Buscar columna de fecha
                fecha_col = next((col for col in df.columns if 'fecha' in col or 'date' in col), None)
                if not fecha_col:
                    print(f"❌ Columna de fecha no encontrada en {archivo_reciente}")
                    continue
                
                # Mapeo de columnas
                col_map = {
                    'open': next((c for c in df.columns if 'open' in c), None),
                    'high': next((c for c in df.columns if 'high' in c), None),
                    'low': next((c for c in df.columns if 'low' in c), None),
                    'close': next((c for c in df.columns if 'close' in c), None),
                    'adj_close': next((c for c in df.columns if 'adj_close' in c or 'ajustado' in c), None),
                    'volume': next((c for c in df.columns if 'volume' in c or 'volumen' in c), None)
                }
                
                # Crear DataFrame limpio
                df_clean = pd.DataFrame({
                    'fecha': pd.to_datetime(df[fecha_col], errors='coerce'),
                    'open': df[col_map['open']] if col_map['open'] else None,
                    'high': df[col_map['high']] if col_map['high'] else None,
                    'low': df[col_map['low']] if col_map['low'] else None,
                    'close': df[col_map['close']] if col_map['close'] else None,
                    'adj_close': df[col_map['adj_close']] if col_map['adj_close'] else None,
                    'volume': df[col_map['volume']] if col_map['volume'] else None
                })
                
                # Filtrar fechas inválidas
                df_clean = df_clean.dropna(subset=['fecha'])
                df_clean['fecha'] = df_clean['fecha'].dt.date
                
                # Convertir volumen a Int64 (permite enteros y NaN)
                if 'volume' in df_clean.columns:
                    df_clean['volume'] = pd.to_numeric(df_clean['volume'], errors='coerce').astype(pd.Int64Dtype())
                
                # Preparar datos para inserción masiva
                data_tuples = [
                    (
                        activo['id'],
                        activo['ticker'],
                        row['fecha'],
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close'],
                        row['adj_close'],
                        row['volume'] if 'volume' in df_clean.columns else None
                    )
                    for _, row in df_clean.iterrows()
                ]
                
                if not data_tuples:
                    print(f"ℹ️ No hay datos válidos para {activo['nombre']}")
                    continue
                
                # Inserción masiva
                query = """
                    INSERT INTO datos_historicos (
                        activo_id, activo_nombre, fecha, apertura, maximo, minimo, 
                        cierre, cierre_ajustado, volumen
                    )
                    VALUES %s
                    ON CONFLICT (activo_id, fecha) DO UPDATE
                    SET
                        apertura = EXCLUDED.apertura,
                        maximo = EXCLUDED.maximo,
                        minimo = EXCLUDED.minimo,
                        cierre = EXCLUDED.cierre,
                        cierre_ajustado = EXCLUDED.cierre_ajustado,
                        volumen = EXCLUDED.volumen
                """
                extras.execute_values(
                    cur,
                    query,
                    data_tuples,
                    page_size=50  # Ajustar según necesidades
                )
                print(f"✅ Datos cargados para {activo['nombre']} - {len(data_tuples)} registros")
                
            except Exception as e:
                print(f"❌ Error procesando {activo['nombre']}: {e}")
                conn.rollback()
        
        conn.commit()
        print("✅ Datos históricos cargados exitosamente")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error general cargando datos históricos: {e}")
        return False
    

# Cargar configuración de activos
try:
    with open('../../stock_symbols.json', 'r') as f:
        activos = json.load(f)
    print(f"✅ Cargados {len(activos)} activos desde JSON")
except Exception as e:
    print(f"❌ Error cargando JSON de activos: {e}")
    exit(1)

# Proceso principal
print("\n" + "="*70)
print(" INICIO DE CARGA DE DATOS ".center(70, '#'))
print("="*70 + "\n")

# Verificar conexión
if conn:
    try:
        # Paso 1: Cargar activos
        if cargar_activos(conn, activos):
            # Paso 2: Cargar eventos
            cargar_eventos(conn, activos)
            
            # Paso 3: Cargar datos históricos
            cargar_datos_historicos(conn, activos)
        else:
            print("⛔ Abortando por error en activos")
        
    except Exception as e:
        print(f"🔥 Error crítico: {e}")
    finally:
        conn.close()
        print("🔌 Conexión cerrada")
else:
    print("⛔ No se pudo conectar a la base de datos. Abortando.")

print("\n" + "="*70)
print(" CARGA COMPLETADA ".center(70, '#'))
print("="*70 + "\n")