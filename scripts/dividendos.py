import yfinance as yf
import pandas as pd
from datetime import datetime
import numpy as np
import os

def guardar_datos_completos(datos, nombre_indice):
    """Guarda todos los datos históricos en un archivo CSV"""
    try:
        # Crear directorio si no existe
        os.makedirs('datos_completos', exist_ok=True)
        
        # Preparar DataFrame
        df_completo = datos.copy()
        df_completo.reset_index(inplace=True)
        
        # Renombrar columnas para mayor claridad
        if isinstance(df_completo.columns, pd.MultiIndex):
            df_completo.columns = ['_'.join(col).replace(' ', '_') for col in df_completo.columns]
        
        # Guardar CSV con todos los datos
        fecha_actual = datetime.now().strftime('%Y%m%d')
        nombre_archivo = f"datos_completos/Datos_Completos_{nombre_indice}_{fecha_actual}.csv"
        df_completo.to_csv(nombre_archivo, index=False, float_format='%.4f')
        
        return nombre_archivo
    except Exception as e:
        print(f"Error guardando datos completos: {str(e)}")
        return None

def descargar_eventos(ticker, nombre_indice):
    """Descarga dividendos y splits para acciones individuales"""
    try:
        if ticker.startswith('^'):
            return pd.DataFrame(columns=['Fecha', 'Evento', 'Tipo'])
        
        obj = yf.Ticker(ticker)
        eventos = []
        
        # Dividendos
        dividendos = obj.dividends
        if not dividendos.empty:
            for fecha, valor in dividendos.items():
                # Convertir a UTC y quitar zona horaria
                fecha_utc = fecha.tz_convert('UTC').tz_localize(None)
                eventos.append({'Fecha': fecha_utc, 'Evento': valor, 'Tipo': 'Dividendo'})
        
        # Splits
        splits = obj.splits
        if not splits.empty:
            for fecha, valor in splits.items():
                # Convertir a UTC y quitar zona horaria
                fecha_utc = fecha.tz_convert('UTC').tz_localize(None)
                eventos.append({'Fecha': fecha_utc, 'Evento': valor, 'Tipo': 'Split'})
        
        return pd.DataFrame(eventos)
    
    except Exception as e:
        print(f"Error descargando eventos para {nombre_indice}: {str(e)}")
        return pd.DataFrame(columns=['Fecha', 'Evento', 'Tipo'])

def generar_reporte_indice(ticker, nombre_indice):
    try:
        # Descargar datos con parámetros optimizados (agregué end=...)
        datos = yf.download(
            ticker, 
            start="2000-01-01", 
            end=datetime.now().strftime('%Y-%m-%d'),
            auto_adjust=False,
            progress=False,
            group_by='ticker'
        )
        
        if datos.empty:
            print(f"No se encontraron datos para {nombre_indice}")
            return None, None, None
        
        # Guardar todos los datos históricos
        archivo_completo = guardar_datos_completos(datos, nombre_indice)
        if archivo_completo:
            print(f"Datos completos guardados: {archivo_completo}")
        
        # Manejar estructura de columnas MultiIndex
        col_cierre = None
        col_ajustado = None
        
        if isinstance(datos.columns, pd.MultiIndex):
            for col in datos.columns:
                if col[1] == 'Close':
                    col_cierre = col
                if col[1] == 'Adj Close':
                    col_ajustado = col
        else:
            col_cierre = 'Close'
            col_ajustado = 'Adj Close'
        
        if not col_cierre or not col_ajustado:
            print(f"Columnas críticas no encontradas para {nombre_indice}")
            print("Columnas disponibles:", datos.columns.tolist())
            return None, None, None
        
        # Crear reporte con diferencias
        reporte = pd.DataFrame({
            'Fecha': datos.index,
            'Cierre': datos[col_cierre].values,
            'Cierre_Ajustado': datos[col_ajustado].values
        })
        
        # Calcular diferencias
        reporte['Diferencia_Absoluta'] = reporte['Cierre'] - reporte['Cierre_Ajustado']
        reporte['Diferencia_Relativa'] = (reporte['Diferencia_Absoluta'] / reporte['Cierre']) * 100
        
        # Filtrar diferencias significativas (>0.0001%)
        reporte = reporte[np.abs(reporte['Diferencia_Relativa']) > 0.0001]
        
        # Si no hay diferencias, crear reporte vacío para consistencia
        if reporte.empty:
            reporte = pd.DataFrame(columns=[
                'Fecha', 'Cierre', 'Cierre_Ajustado', 
                'Diferencia_Absoluta', 'Diferencia_Relativa'
            ])
            print("No se encontraron diferencias significativas")
        else:
            # Ordenar por fecha descendente
            reporte = reporte.sort_values('Fecha', ascending=False)
        
        # Guardar reporte de diferencias
        os.makedirs('./reportes', exist_ok=True)
        fecha_reporte = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_archivo_reporte = f"./reportes/Reporte_Ajustes_{nombre_indice}_{fecha_reporte}.csv"
        reporte.to_csv(nombre_archivo_reporte, index=False, float_format='%.4f')

        # Descargar y vincular eventos (dividendos/splits)
        eventos = descargar_eventos(ticker, nombre_indice)
        reporte_completo = reporte.copy()
        
        if not eventos.empty:
            # Asegurar que las fechas sean comparables
            reporte_completo['Fecha'] = pd.to_datetime(reporte_completo['Fecha'])
            eventos['Fecha'] = pd.to_datetime(eventos['Fecha'])
            
            # Fusionar manteniendo todas las filas del reporte
            reporte_completo = pd.merge(
                reporte_completo, 
                eventos, 
                on='Fecha', 
                how='left'
            )
            
            # Guardar versión completa
            reporte_completo.to_csv(
                f"./reportes/Reporte_Completo_{nombre_indice}.csv", 
                index=False,
                float_format='%.4f'
            )
        
        return nombre_archivo_reporte, archivo_completo, reporte_completo
    
    except Exception as e:
        print(f"Error procesando {nombre_indice}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None, None

# Configuración de índices
indices = [
    {'ticker': '^SPX', 'nombre': 'S&P500'},
    {'ticker': '^NDX', 'nombre': 'NASDAQ-100'},
    {'ticker': 'AAPL', 'nombre': 'Apple Inc.'},
    {'ticker': 'MSFT', 'nombre': 'Microsoft Corp.'},
    {'ticker': 'GOOGL', 'nombre': 'Alphabet Inc.'},
    {'ticker': 'AMZN', 'nombre': 'Amazon.com Inc.'},
    {'ticker': 'TSLA', 'nombre': 'Tesla Inc.'},
    {'ticker': 'META', 'nombre': 'Meta Platforms Inc.'},
    {'ticker': 'NFLX', 'nombre': 'Netflix Inc.'},
    {'ticker': 'NVDA', 'nombre': 'NVIDIA Corp.'},
    {'ticker': 'BRK-B', 'nombre': 'Berkshire Hathaway Inc.'},
    {'ticker': 'V', 'nombre': 'Visa Inc.'},
    {'ticker': 'JPM', 'nombre': 'JPMorgan Chase & Co.'},
    {'ticker': 'UNH', 'nombre': 'UnitedHealth Group Inc.'},
    {'ticker': 'PG', 'nombre': 'Procter & Gamble Co.'},
    {'ticker': 'HD', 'nombre': 'The Home Depot Inc.'},
    {'ticker': 'DIS', 'nombre': 'Walt Disney Co.'},
    {'ticker': 'VZ', 'nombre': 'Verizon Communications Inc.'},
    {'ticker': 'INTC', 'nombre': 'Intel Corp.'},
    {'ticker': 'CSCO', 'nombre': 'Cisco Systems Inc.'},
    {'ticker': 'PEP', 'nombre': 'PepsiCo Inc.'},
    {'ticker': 'KO', 'nombre': 'Coca-Cola Co.'},
    {'ticker': 'T', 'nombre': 'AT&T Inc.'}
]

# Procesamiento robusto con verificación
for indice in indices:
    print(f"\n{'='*70}")
    print(f" Procesando {indice['nombre']} ".center(70, '#'))
    archivo_reporte, archivo_completo, df = generar_reporte_indice(indice['ticker'], indice['nombre'])
    
    if archivo_reporte:
        print(f"\nReporte de diferencias generado: {archivo_reporte}")
        if df is not None and not df.empty:
            print(f"Registros con diferencias: {len(df)}")
            print("\nMuestra de diferencias:")
            print(df.head(3))
        else:
            print("No se encontraron diferencias significativas")
    
    print(f"\n{'='*70}")

print("\n" + " PROCESO COMPLETADO ".center(70, '=') + "\n")