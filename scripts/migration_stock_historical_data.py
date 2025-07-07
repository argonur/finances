import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import json

# Cargar variables de entorno
load_dotenv()
NEON_DB_URL = os.getenv("NEON_HISTORICAL_DATA_DB_URL")

def conectar_db():
    """Establece conexión con la base de datos Neon"""
    try:
        conn = psycopg2.connect(
            NEON_DB_URL,
            sslmode='require'
        )
        print("Conexión exitosa a NeonDB")
        return conn
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        return None

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
            # Construir ruta de carpeta
            carpeta = f"./datos/{activo['nombre']}/reportes_eventos"
            
            # Verificar si existe la carpeta
            if not os.path.exists(carpeta):
                print(f"⚠️ Carpeta no encontrada para eventos: {carpeta}")
                continue
            if os.path.getsize(archivo_reciente) == 0:
                print(f"ℹ️ Archivo vacío para eventos de {activo['nombre']}")
                continue
                
            df = pd.read_csv(archivo_reciente)
                
            # Buscar archivo más reciente
            archivos = [f for f in os.listdir(carpeta) 
                    if f.startswith('Eventos') and f.endswith('.csv')]
            
            if not archivos:
                print(f"ℹ️ No se encontraron eventos para {activo['nombre']}")
                continue
                
            # Seleccionar archivo más reciente
            archivo_reciente = max(
                [os.path.join(carpeta, f) for f in archivos],
                key=os.path.getmtime
            )
            
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

def cargar_datos_historicos(conn, activos):
    """Carga los datos históricos a la base de datos"""
    try:
        cur = conn.cursor()
        for activo in activos:
            # Construir ruta de carpeta
            carpeta = f"./datos/{activo['nombre']}/datos_completos"
            
            # Verificar si existe la carpeta
            if not os.path.exists(carpeta):
                print(f"⚠️ Carpeta no encontrada para datos: {carpeta}")
                continue
                
            # Buscar archivo más reciente
            archivos = [f for f in os.listdir(carpeta) 
                    if f.startswith('Datos_Completos') and f.endswith('.csv')]
            
            if not archivos:
                print(f"ℹ️ No se encontraron datos para {activo['nombre']}")
                continue
                
            # Seleccionar archivo más reciente
            archivo_reciente = max(
                [os.path.join(carpeta, f) for f in archivos],
                key=os.path.getmtime
            )
            
            try:
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
                    
                # Definir mapeo de columnas
                mapeo_columnas = {
                    'open': next((c for c in df.columns if 'open' in c), None),
                    'high': next((c for c in df.columns if 'high' in c), None),
                    'low': next((c for c in df.columns if 'low' in c), None),
                    'close': next((c for c in df.columns if 'close' in c), None),
                    'adj_close': next((c for c in df.columns if 'adj_close' in c or 'ajustado' in c), None),
                    'volume': next((c for c in df.columns if 'volume' in c or 'volumen' in c), None)
                }
                
                # Filtrar filas válidas
                df = df.dropna(subset=[fecha_col])
                
                # Insertar en lote
                for _, row in df.iterrows():
                    query = """
                    INSERT INTO datos_historicos (
                        activo_id, fecha, apertura, maximo, minimo, 
                        cierre, cierre_ajustado, volumen
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (activo_id, fecha) DO UPDATE
                    SET
                        apertura = EXCLUDED.apertura,
                        maximo = EXCLUDED.maximo,
                        minimo = EXCLUDED.minimo,
                        cierre = EXCLUDED.cierre,
                        cierre_ajustado = EXCLUDED.cierre_ajustado,
                        volumen = EXCLUDED.volumen
                    """
                    valores = (
                        activo['id'],
                        row[fecha_col],
                        row[mapeo_columnas['open']] if mapeo_columnas['open'] and not pd.isna(row[mapeo_columnas['open']]) else None,
                        row[mapeo_columnas['high']] if mapeo_columnas['high'] and not pd.isna(row[mapeo_columnas['high']]) else None,
                        row[mapeo_columnas['low']] if mapeo_columnas['low'] and not pd.isna(row[mapeo_columnas['low']]) else None,
                        row[mapeo_columnas['close']] if mapeo_columnas['close'] and not pd.isna(row[mapeo_columnas['close']]) else None,
                        row[mapeo_columnas['adj_close']] if mapeo_columnas['adj_close'] and not pd.isna(row[mapeo_columnas['adj_close']]) else None,
                        int(row[mapeo_columnas['volume']]) if mapeo_columnas['volume'] and not pd.isna(row[mapeo_columnas['volume']]) else None
                    )
                    cur.execute(query, valores)
                    
            except Exception as e:
                print(f"❌ Error procesando datos para {activo['nombre']}: {e}")
                conn.rollback()  # Rollback de cualquier operación pendiente
                continue  # Continuar con el siguiente activo
        
        conn.commit()
        print("✅ Datos históricos cargados exitosamente")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Error general cargando datos históricos: {e}")
        return False

# Cargar configuración de activos
try:
    with open('../stock_symbols.json', 'r') as f:
        activos = json.load(f)
    print(f"✅ Cargados {len(activos)} activos desde JSON")
except Exception as e:
    print(f"❌ Error cargando JSON de activos: {e}")
    exit(1)

# Proceso principal
print("\n" + "="*70)
print(" INICIO DE CARGA DE DATOS ".center(70, '#'))
print("="*70 + "\n")

conn = conectar_db()
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