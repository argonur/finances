import yfinance as yf
import pandas as pd
import os
import psycopg2
from datetime import datetime, time, timedelta
import pytz
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def get_index_data(symbol, specific_date=None):
    """Obtiene los datos diarios del índice"""
    try:
        ticker = yf.Ticker(symbol)
        ny_tz = pytz.timezone('America/New_York')
        now_ny = datetime.now(ny_tz)
        
        if specific_date:
            # Convertir fecha específica al formato de Yahoo Finance (MM/DD/YYYY -> YYYY-MM-DD)
            yahoo_date = datetime.strptime(specific_date, '%m/%d/%Y').strftime('%Y-%m-%d')
            start_date = yahoo_date
            end_date = (datetime.strptime(yahoo_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            data = ticker.history(start=start_date, end=end_date)
        else:
            # Determinar qué fecha necesitamos
            if now_ny.time() < time(16, 0) and now_ny.weekday() <= 4:  # Antes de las 4 PM en día hábil
                # Obtener datos del día hábil anterior
                yesterday = now_ny - timedelta(days=1)
                # Si ayer fue fin de semana, retroceder hasta el viernes
                if yesterday.weekday() > 4:  # Sábado o domingo
                    yesterday = yesterday - timedelta(days=(yesterday.weekday() - 4))
                
                start_date = yesterday.strftime('%Y-%m-%d')
                end_date = (yesterday + timedelta(days=1)).strftime('%Y-%m-%d')
                data = ticker.history(start=start_date, end=end_date)
            else:
                # Obtener datos del día actual (después del cierre)
                data = ticker.history(period="1d")
        
        if data.empty:
            return None
            
        # Convertir valores NumPy a Python nativo y redondear
        close_val = round(float(data['Close'].iloc[-1]), 2)
        open_val = round(float(data['Open'].iloc[-1]), 2)
        high_val = round(float(data['High'].iloc[-1]), 2)
        low_val = round(float(data['Low'].iloc[-1]), 2)
        
        return {
            'Date': data.index[-1].strftime('%m/%d/%Y'),  # Formato MM/DD/YYYY
            'Close/Last': close_val,
            'Open': open_val,
            'High': high_val,
            'Low': low_val
        }
    except Exception as e:
        print(f"Error al obtener datos: {e}")
        return None

def save_to_csv(data, filename):
    """Guarda los datos en CSV con los más recientes primero"""
    try:
        # Crear DataFrame con nuevos datos
        new_row = pd.DataFrame([data])
        
        if os.path.exists(filename):
            # Leer datos existentes
            df = pd.read_csv(filename)
            
            # Verificar si la fecha ya existe
            if data['Date'] in df['Date'].values:
                print(f"Datos ya existen para {data['Date']} en {filename}. No se actualizó.")
                return False
                
            # Insertar nuevo dato al principio
            df = pd.concat([new_row, df], ignore_index=True)
        else:
            # Crear nuevo archivo
            df = new_row
        
        # Guardar con los datos más recientes primero
        df.to_csv(filename, index=False)
        print(f"Datos actualizados: {filename}")
        return True
    except Exception as e:
        print(f"Error al guardar datos: {e}")
        return False

def get_db_connection():
    """Conexión a Neon PostgreSQL"""
    try:
        return psycopg2.connect(os.getenv("NEON_DB_URL"))
    except Exception as e:
        print(f"Error de conexión a DB: {e}")
        return None

def save_to_database(data, index_name):
    """Guarda datos en la base de datos Neon"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        table = "sp500" if index_name == 'S&P 500' else "nasdaq"
        
        # Convertir fecha de MM/DD/YYYY a YYYY-MM-DD
        db_date = datetime.strptime(data['Date'], '%m/%d/%Y').strftime('%Y-%m-%d')
        
        with conn.cursor() as cur:
            # Insertar o actualizar registro
            sql = f"""
            INSERT INTO {table} (date, close_last, open, high, low)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE
            SET close_last = EXCLUDED.close_last,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low
            """
            # Convertir todos los valores a tipos nativos explícitamente
            cur.execute(sql, (
                db_date,
                float(data['Close/Last']),  # Conversión explícita a float
                float(data['Open']),
                float(data['High']),
                float(data['Low'])
            ))
        
        conn.commit()
        print(f"Datos guardados en DB: {index_name} | {data['Date']}")
        return True
    
    except Exception as e:
        print(f"Error guardando en DB: {e}")
        return False
    finally:
        conn.close()

def should_run_auto():
    """Determina si es momento de ejecución automática post-cierre"""
    try:
        ny_tz = pytz.timezone('America/New_York')
        now_ny = datetime.now(ny_tz)
        
        # Verificar si es día hábil (Lunes a Viernes)
        if now_ny.weekday() > 4:  # Sábado y Domingo
            return False

        # Verificar si la hora actual está después del cierre (4:00 PM ET)
        market_close_time = time(16, 0)  # 4 PM
        return now_ny.time() > market_close_time
    except Exception as e:
        print(f"Error verificando horario automático: {e}")
        return False

# Símbolos y archivos
INDEX_SYMBOLS = {
    'S&P 500': '^SPX',
    'NASDAQ-100': '^NDX'
}

CSV_FILES = {
    'S&P 500': '../data/HistoricalData_spx.csv',
    'NASDAQ-100': '../data/HistoricalData_nasdaq.csv'
}

def update_indices(specific_date=None):
    """Actualiza los índices para la fecha actual o específica"""
    for name, symbol in INDEX_SYMBOLS.items():
        print(f"\nObteniendo {name}....")
        data = get_index_data(symbol, specific_date)
        
        if data:
            print(f"Datos obtenidos: {data['Date']} | Cierre: {data['Close/Last']}")
            save_to_csv(data, CSV_FILES[name])
            save_to_database(data, name)
        else:
            print(f"No se pudieron obtener datos para {name}.")

def main(auto_mode=False, specific_date=None):
    if specific_date:
        print(f"\nModo fecha específica: {specific_date}")
        update_indices(specific_date)
    elif auto_mode:
        print("\nEjecutando en modo automático...")
        update_indices()
    else:
        print("\nEjecutando en modo manual...")
        update_indices()

def display_menu():
    """Muestra el menú interactivo y maneja las opciones"""
    while True:
        print("\n" + "="*50)
        print("MENÚ DE ACTUALIZACIÓN DE ÍNDICES BURSÁTILES")
        print("="*50)
        print("1. Ejecutar en modo automático (recomendado post-cierre)")
        print("2. Ejecutar en modo manual (ahora mismo)")
        print("3. Ingresar fecha específica (formato MM/DD/AAAA)")
        print("4. Salir")
        
        choice = input("\nSeleccione una opción: ")
        
        if choice == '1':
            if should_run_auto():
                print("\nIniciando modo automático...")
                main(auto_mode=True)
            else:
                print("\nNo es momento de ejecución automática (bolsa abierta o fuera de horario).")
        elif choice == '2':
            print("\nIniciando modo manual...")
            main()
        elif choice == '3':
            fecha = input("\nIngrese la fecha (MM/DD/AAAA): ")
            try:
                # Validar formato de fecha
                datetime.strptime(fecha, '%m/%d/%Y')
                main(specific_date=fecha)
            except ValueError:
                print("Formato de fecha inválido. Use MM/DD/AAAA.")
        elif choice == '4':
            print("\nSaliendo del programa...")
            break
        else:
            print("\nOpción inválida. Por favor seleccione 1-4.")
        
        input("\nPresione Enter para continuar...")

if __name__ == "__main__":
    # Siempre mostrar el menú principal
    display_menu()