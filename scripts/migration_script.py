import pandas as pd
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def migrate_csv_to_db():
    """Migra todos los datos históricos de CSV a Neon DB"""
    # Tablas y archivos
    tables = {
        'nasdaq': [
            '../data/HistoricalData_nasdaq.csv',
            '../data/Data_nasdaq.csv'
        ],
        'sp500': [
            '../data/HistoricalData_spx.csv',
            '../data/Data_spx.csv'
        ]
    }
    
    try:
        # Conectar a Neon
        conn = psycopg2.connect(os.getenv("NEON_DB_URL"))
        cur = conn.cursor()
        
        for table, filenames in tables.items():
            for filename in filenames:
                if os.path.exists(filename):
                    # Leer CSV con estructura limpia
                    df = pd.read_csv(filename)
                    
                    print(f"Migrando {len(df)} registros de {filename} a {table}...")
                    
                    # Asegurar nombres de columnas consistentes
                    df = df.rename(columns={
                        'Close_Last': 'close_last',
                        'Close/Last': 'close_last',
                        'CloseLast': 'close_last',
                        'Open': 'open',
                        'High': 'high',
                        'Low': 'low',
                        'Date': 'date'
                    })
                    
                    # Seleccionar solo las columnas necesarias
                    required_columns = ['date', 'close_last', 'open', 'high', 'low']
                    df = df[required_columns]
                    
                    # Insertar datos
                    for _, row in df.iterrows():
                        try:
                            # Convertir fecha a formato PostgreSQL (YYYY-MM-DD)
                            db_date = datetime.strptime(row['date'], '%m/%d/%Y').strftime('%Y-%m-%d')
                            
                            insert_sql = f"""
                            INSERT INTO {table} (date, close_last, open, high, low)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (date) DO UPDATE
                            SET close_last = EXCLUDED.close_last,
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low
                            """
                            
                            # Manejar valores faltantes (reemplazar NaN con 0.0)
                            close_val = float(row['close_last']) if pd.notna(row['close_last']) else 0.0
                            open_val = float(row['open']) if pd.notna(row['open']) else 0.0
                            high_val = float(row['high']) if pd.notna(row['high']) else 0.0
                            low_val = float(row['low']) if pd.notna(row['low']) else 0.0
                            
                            cur.execute(insert_sql, (
                                db_date,
                                close_val,
                                open_val,
                                high_val,
                                low_val
                            ))
                        except ValueError as e:
                            print(f"Error de valor en fila: {row} - {str(e)}")
                            continue
                        except Exception as e:
                            print(f"Error insertando {row.get('date', 'fecha desconocida')}: {str(e)}")
                            continue
                    
                    print(f"{table} migrado exitosamente desde {filename}")
        
        conn.commit()
        print("Migración completa!")
    
    except Exception as e:
        print(f"Error en migración: {str(e)}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    migrate_csv_to_db()
