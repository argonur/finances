import pandas as pd
import os
import numpy as np

def clean_financial_csv(file_path):
    """
    Limpia archivos CSV financieros reemplazando valores problemáticos con 0.0
    y manteniendo la estructura consistente
    """
    try:
        # Leer archivo con manejo de valores problemáticos
        df = pd.read_csv(file_path, dtype=str)
        
        # Paso 1: Identificar y eliminar columnas completamente vacías
        df = df.dropna(axis=1, how='all')
        
        # Paso 2: Normalizar nombres de columnas
        df.columns = df.columns.str.strip().str.replace('/', '_').str.replace(' ', '')
        
        # Paso 3: Mapeo de columnas esperadas
        expected_columns = {
            'Date': 'Date',
            'Close_Last': 'Close_Last',
            'Open': 'Open',
            'High': 'High',
            'Low': 'Low'
        }
        
        # Paso 4: Reconstruir estructura de columnas
        valid_columns = []
        for col in expected_columns:
            if col in df.columns:
                valid_columns.append(col)
            else:
                # Si falta una columna, la creamos con valores 0.0
                df[col] = '0.0'
                valid_columns.append(col)
        
        # Seleccionar solo las columnas válidas
        df = df[valid_columns]
        
        # Paso 5: Limpieza de valores - reemplazar problemáticos con 0.0
        numeric_cols = ['Close_Last', 'Open', 'High', 'Low']
        
        for col in numeric_cols:
            # Reemplazar valores problemáticos con '0.0'
            df[col] = df[col].replace(['--', '', 'NaN', 'N/A', ' ', 'NA'], '0.0')
            
            # Eliminar comas y espacios
            df[col] = df[col].str.replace(',', '').str.replace(' ', '')
            
            # Convertir a numérico
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        # Paso 6: Manejo de fechas
        df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')
        
        # Eliminar filas con fechas completamente inválidas
        df = df.dropna(subset=['Date'])
        
        # Formatear fecha consistentemente
        df['Date'] = df['Date'].dt.strftime('%m/%d/%Y')
        
        # Paso 8: Guardar
        df.to_csv(file_path, index=False)
        print(f"Archivo limpiado exitosamente: {file_path}")
        return True
        
    except Exception as e:
        print(f"Error procesando {file_path}: {str(e)}")
        return False

# Limpiar ambos archivos
clean_financial_csv('../data/HistoricalData_spx.csv')
clean_financial_csv('../data/HistoricalData_nasdaq.csv')