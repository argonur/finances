import pandas as pd
import numpy as np

def clean_financial_csv(file_path):
    """
    Limpia archivos CSV financieros eliminando filas con '--' en Volume
    y desplazando los valores de Open, High y Low una posición a la izquierda.
    """
    try:
        # Leer archivo con manejo de valores problemáticos
        df = pd.read_csv(file_path, dtype=str)
        
        # Paso 1: Identificar y eliminar columnas completamente vacías
        df = df.dropna(axis=1, how='all')
        
        # Normalizar nombres de columnas
        df.columns = df.columns.str.strip().str.replace('/', '_').str.replace(' ', '')
        
        # Paso 2: Verificar si la columna Volume existe
        if 'Volume' in df.columns:
            # Paso 3: Filtrar filas donde Volume es '--'
            rows_to_shift = df[df['Volume'] == '--'].index
            
            # Paso 4: Desplazar valores de Open, High y Low una posición a la izquierda
            for index in rows_to_shift:
                if index < len(df) - 1:  # Asegurarse de que no se salga del rango
                    df.at[index, 'Open'] = df.at[index + 1, 'Open']
                    df.at[index, 'High'] = df.at[index + 1, 'High']
                    df.at[index, 'Low'] = df.at[index + 1, 'Low']
                    
                    # Limpiar el valor de Volume
                    df.at[index, 'Volume'] = np.nan  # O puedes dejarlo como '--' si prefieres

            # Eliminar filas donde Volume era '--'
            df = df[df['Volume'] != '--']

        # Paso 5: Manejo de fechas
        df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')
        
        # Eliminar filas con fechas completamente inválidas
        df = df.dropna(subset=['Date'])
        
        # Formatear fecha consistentemente
        df['Date'] = df['Date'].dt.strftime('%m/%d/%Y')
        
        # Paso 6: Eliminar columnas vacías que pueden haber quedado
        df = df.replace('', np.nan).dropna(axis=1, how='all')

        # Paso 7: Eliminar columnas que están vacías después del desplazamiento
        df = df.fillna('')  # Rellenar NaN con cadenas vacías para evitar comas
        df = df.loc[:, (df != '').any(axis=0)]  # Mantener solo columnas que tienen datos

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
