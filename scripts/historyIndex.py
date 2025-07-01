import yfinance as yf
import pandas as pd
import os
from datetime import datetime, time, timedelta
import pytz
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def get_index_data(symbol, specific_date=None):
    """Obtiene los datos diarios del índice"""
    try:
        ticker = yf.Ticker(symbol)
        
        if specific_date:
            # Convertir fecha específica al formato de Yahoo Finance
            yahoo_date = datetime.strptime(specific_date, '%d/%m/%Y').strftime('%Y-%m-%d')
            start_date = yahoo_date
            end_date = (datetime.strptime(yahoo_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            data = ticker.history(start=start_date, end=end_date)
        else:
            # Último día disponible
            data = ticker.history(period="1d")
        
        if data.empty:
            return None
            
        # Convertir fecha al formato DD/MM/YYYY y valores a 2 decimales
        return {
            'Date': data.index[-1].strftime('%d/%m/%Y'),  # Nuevo formato de fecha
            'Close/Last': round(data['Close'].iloc[-1], 2),
            'Open': round(data['Open'].iloc[-1], 2),
            'High': round(data['High'].iloc[-1], 2),
            'Low': round(data['Low'].iloc[-1], 2)
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

def should_run_auto():
    """Determina si es momento de ejecución automática post-cierre"""
    ny_tz = pytz.timezone('America/New_York')
    now_ny = datetime.now(ny_tz)
    
    # Verificar si es día hábil (Lunes a Viernes)
    if now_ny.weekday() > 4:  # Sábado y Domingo
        return False

    # Verificar si la hora actual está después del cierre (4:00 PM ET)
    market_close_time = time(16, 0)  # 4 PM
    return now_ny.time() > market_close_time

# Símbolos y archivos
INDEX_SYMBOLS = {
    'S&P 500': '^GSPC',
    'NASDAQ': '^IXIC'
}

CSV_FILES = {
    'S&P 500': '../data/HistoricalData_spx.csv',
    'NASDAQ': '../data/HistoricalData_nasdaq.csv'
}

def update_indices(specific_date=None):
    """Actualiza los índices para la fecha actual o una fecha específica"""
    for name, symbol in INDEX_SYMBOLS.items():
        print(f"\nObteniendo {name}....")
        data = get_index_data(symbol, specific_date)
        
        if data:
            print(f"Datos obtenidos: {data['Date']} | Cierre: {data['Close/Last']}")
            save_to_csv(data, CSV_FILES[name])
        else:
            print(f"No se pudieron obtener datos para {name}.")

def main(auto_mode=False, specific_date=None):
    if specific_date:
        print(f"\nModo fecha específica: {specific_date}")
        update_indices(specific_date)
    elif auto_mode:
        if should_run_auto():
            print("\nEjecutando en modo automático...")
            update_indices()
        else:
            print("\nNo es momento de ejecución automática (bolsa abierta o fuera de horario).")
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
        print("3. Ingresar fecha específica (formato DD/MM/AAAA)")
        print("4. Salir")
        
        choice = input("\nSeleccione una opción: ")
        
        if choice == '1':
            print("\nIniciando modo automático...")
            main(auto_mode=True)
        elif choice == '2':
            print("\nIniciando modo manual...")
            main()
        elif choice == '3':
            fecha = input("\nIngrese la fecha (DD/MM/AAAA): ")
            try:
                # Validar formato de fecha
                datetime.strptime(fecha, '%d/%m/%Y')
                main(specific_date=fecha)
            except ValueError:
                print("Formato de fecha inválido. Use DD/MM/AAAA.")
        elif choice == '4':
            print("\nSaliendo del programa...")
            break
        else:
            print("\nOpción inválida. Por favor seleccione 1-4.")
        
        input("\nPresione Enter para continuar...")

if __name__ == "__main__":
    # Por defecto se ejecuta en modo automático si es horario válido
    if should_run_auto():
        print("Ejecutando en modo automático por defecto...")
        main(auto_mode=True)
    else:
        # Si no es horario válido, mostrar el menú
        display_menu()