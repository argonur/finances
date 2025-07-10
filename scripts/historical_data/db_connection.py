import psycopg2
import os
from dotenv import load_dotenv
import sys

load_dotenv()

def conectar_neon():
    """Conexión a NeonDB"""
    try:
        conn = psycopg2.connect(
            os.getenv("NEON_DB_URL"),
            sslmode='require'
        )
        print("Conexión exitosa a NeonDB")
        return conn
    except Exception as e:
        print(f"Error conectando a Neon: {e}")
        return None

def conectar_local():
    """Conexión a PostgreSQL instalado en WSL (localhost)"""
    try:
        # Configuración simplificada para WSL
        config = {
            'host': "localhost",
            'port': os.getenv("LOCAL_DB_PORT", "5432"),  # Usa 5432 por defecto
            'database': os.getenv("LOCAL_DB_NAME", "finances"),
            'user': os.getenv("LOCAL_DB_USER"),
            'password': os.getenv("LOCAL_DB_PASSWORD")
        }
        
        print("Intentando conectar a PostgreSQL local en WSL...")
        #print(f"Configuración: {config}")

        conn = psycopg2.connect(**config)
        print("✅ Conexión exitosa a PostgreSQL local en WSL")
        return conn
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"❌ Error conectando a PostgreSQL local (línea {exc_tb.tb_lineno}): {e}")
        print("Verifica que el servicio PostgreSQL está corriendo en WSL:")
        print("  sudo service postgresql start")
        return None

def obtener_conexion(tipo='neon'):
    """Selector de conexión basado en tipo"""
    if tipo == 'local':
        return conectar_local()
    else:
        return conectar_neon()