import requests
import json
from datetime import datetime, timedelta
import os
import sys
import pandas as pd
from fear_and_greed import get as get_fear_greed_current # Para el valor actual
# Añadir la ruta del modulo de conexion manualmente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_utils.db_connection import obtener_conexion

# Conexión a la base de datos
TIPO_CONEXION = 'local' # 'local' o 'neon'
conn = obtener_conexion(TIPO_CONEXION)
carpeta = f"./historical_fear_greed"

def load_historical_fear_greed(conn, dates):
    """ Cargar a la base de datos todos los datos historicos de Fear and Greed"""
    try:
        cur = conn.cursor()
        for date in dates:
            if not os.path.exists(carpeta):
                print(f"⚠️ Carpeta no encontrada para eventos: {carpeta}")
                continue
            try:
                df = pd.read_csv('./historical_fear_greed/cnn_fear_greed_historical.csv')
                if df.empty:
                    print(f"ℹ️ DataFrame vacío")
                    continue
                # Filtrar filas vacias
                df = df.dropna(subset=['Date', 'Value'])

                # Insertar en lote
                for _, row in df.iterrows():
                    query = """
                            INSERT INTO fear_greed (fecha, indice, fear_greed_index)
                            VALUES(%s, %s, %s)
                            ON CONFLICT (fecha) DO UPDATE
                            """
                    cur.execute(query, (
                        date['Date'],
                        date['Value'],
                        date['Description']
                    ))
            except Exception as e:
                print(f"❌ Error procesando datos historicos")
                continue

        conn.commit()
        print("✅ Eventos cargados exitosamente")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Error cargando datos historicos: {e}")
        return False


def get_current_cnn_fear_greed_index():
    """
    Obtiene el valor actual del CNN Fear & Greed Index usando la librería fear-and-greed.
    """
    try:
        fgi = get_fear_greed_current()
        print(f"Valor actual del CNN Fear & Greed Index: {fgi.value.__round__()} ({fgi.description})")
        print(f"Última actualización: {fgi.last_update}")
        return fgi
    except Exception as e:
        print(f"Error al obtener el valor actual: {e}")
        return None

def get_historical_cnn_fear_greed_index(start_date_str, end_date_str):
    """
    Intenta obtener el historial del CNN Fear & Greed Index desde una API no oficial de CNN.
    Nota: La disponibilidad y formato de esta URL pueden cambiar sin previo aviso.
    """
    base_url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/"
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    historical_data = []

    # La API de CNN parece dar todos los datos hasta la fecha actual desde una fecha inicial si se le pasa el parámetro.
    # Intentaremos obtener todos los datos desde la fecha de inicio.
    
    # La URL parece aceptar solo una fecha, y luego devuelve todo el historial hasta hoy.
    # Por ejemplo, si pides 2022-01-01, te da desde 2022-01-01 hasta hoy.
    # La forma más eficiente es pedir la fecha de inicio una sola vez.
    
    url = f"{base_url}{start_date.strftime('%Y-%m-%d')}"
    
    print(f"Intentando obtener datos históricos de: {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status() # Lanza una excepción para errores HTTP
        data = response.json()

        if 'fear_and_greed_historical' in data and 'data' in data['fear_and_greed_historical'] and 'rating' in data['fear_and_greed_historical']:
            for item in data['fear_and_greed_historical']['data']:
                date_timestamp_ms = item['x']
                date = datetime.fromtimestamp(date_timestamp_ms / 1000) # Convertir ms a segundos
                rate = item['rating']
                value = item['y']
                
                # Filtrar por el rango de fechas solicitado
                if start_date <= date <= end_date:
                    historical_data.append({'Date': date.strftime('%Y-%m-%d'), 'Value': value, 'Description': rate})
            
            df = pd.DataFrame(historical_data)
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            df.sort_index(inplace=True)
            df['Value'] = df['Value'].round().astype(int)
            df['Description'] = df['Description']
            return df
        else:
            print("Estructura de datos JSON inesperada.")
            return pd.DataFrame()
            
    except requests.exceptions.RequestException as e:
        print(f"Error al hacer la solicitud HTTP: {e}")
        print("La URL para datos históricos de CNN podría haber cambiado o estar protegida.")
        return pd.DataFrame()
    except json.JSONDecodeError as e:
        print(f"Error al decodificar la respuesta JSON: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    print("--- Valor Actual del CNN Fear & Greed Index ---")
    current_fgi = get_current_cnn_fear_greed_index()
    print("\n" + "="*50 + "\n")

    print("--- Historial del CNN Fear & Greed Index ---")
    # Define el rango de fechas para el historial (ejemplo: últimos 30 días)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365) # Último año
    
    historical_df = get_historical_cnn_fear_greed_index(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    if not historical_df.empty:
        print(f"\nDatos históricos del CNN Fear & Greed Index desde {start_date.strftime('%Y-%m-%d')} hasta {end_date.strftime('%Y-%m-%d')}:")
        print(historical_df.tail()) # Mostrar las últimas 5 entradas
        print(f"\nTotal de entradas históricas: {len(historical_df)}")
        print("\n--- Respaldo del CNN Fear & Greed Index ---")
        load_historical_fear_greed(conn, historical_df)
        
        # Opcional: Guardar a un archivo CSV
        historical_df.to_csv("./historical_fear_greed/cnn_fear_greed_historical.csv")
        print("Datos históricos guardados en /historical_fear_greed/cnn_fear_greed_historical.csv")
    else:
        print("No se pudieron obtener datos históricos o el DataFrame está vacío.")
