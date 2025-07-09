import os
import yfinance as yf
import pandas as pd
from datetime import datetime
import numpy as np
import json

# Definir fecha de inicio para los datos históricos
FECHA_INICIO = "2000-01-01"
FECHA_INICIO_MAXIMA = "1800-01-01"

def crear_estructura_carpetas(nombre_activo):
    """Crea la estructura de carpetas para un activo"""
    carpetas = {
        'base': f"./datos/{nombre_activo}",
        'datos_completos': f"./datos/{nombre_activo}/datos_completos",
        'reportes_ajustes': f"./datos/{nombre_activo}/reportes_ajustes",
        'reportes_eventos': f"./datos/{nombre_activo}/reportes_eventos"
    }
    
    for carpeta in carpetas.values():
        os.makedirs(carpeta, exist_ok=True)
    
    return carpetas

def guardar_datos_completos(datos, nombre_activo):
    """Guarda todos los datos históricos en la carpeta correspondiente"""
    try:
        carpetas = crear_estructura_carpetas(nombre_activo)
        
        # Preparar DataFrame
        df_completo = datos.copy()
        df_completo.reset_index(inplace=True)
        
        # Renombrar columnas si es MultiIndex
        if isinstance(df_completo.columns, pd.MultiIndex):
            df_completo.columns = ['_'.join(col).replace(' ', '_') for col in df_completo.columns]
        
        # Guardar CSV
        fecha_actual = datetime.now().strftime('%Y%m%d')
        nombre_archivo = f"{carpetas['datos_completos']}/Datos_Completos_{nombre_activo}_{fecha_actual}.csv"
        df_completo.to_csv(nombre_archivo, index=False, float_format='%.6f')
        
        return nombre_archivo
    except Exception as e:
        print(f"Error guardando datos completos: {str(e)}")
        return None

def guardar_eventos(ticker, nombre_activo):
    """Descarga y guarda eventos (dividendos/splits) en archivo separado"""
    try:
        carpetas = crear_estructura_carpetas(nombre_activo)
        eventos = []
        
        # Para índices, no hay eventos
        if ticker.startswith('^'):
            # Guardar DataFrame vacío para consistencia
            df_eventos = pd.DataFrame(columns=['Fecha', 'Evento', 'Tipo'])
        else:
            obj = yf.Ticker(ticker)
            
            # Dividendos
            dividendos = obj.dividends
            if not dividendos.empty:
                for fecha, valor in dividendos.items():
                    if hasattr(fecha, 'tz'):
                        fecha = fecha.tz_convert('UTC').tz_localize(None)
                    eventos.append({'Fecha': fecha, 'Evento': valor, 'Tipo': 'Dividendo'})
            
            # Splits
            splits = obj.splits
            if not splits.empty:
                for fecha, valor in splits.items():
                    if hasattr(fecha, 'tz'):
                        fecha = fecha.tz_convert('UTC').tz_localize(None)
                    eventos.append({'Fecha': fecha, 'Evento': valor, 'Tipo': 'Split'})
            
            df_eventos = pd.DataFrame(eventos)
        
        # Guardar eventos
        fecha_reporte = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_archivo = f"{carpetas['reportes_eventos']}/Eventos_{nombre_activo}_{fecha_reporte}.csv"
        df_eventos.to_csv(nombre_archivo, index=False)
        
        return nombre_archivo
    except Exception as e:
        print(f"Error guardando eventos para {nombre_activo}: {str(e)}")
        return None

def generar_reporte_ajustes(datos, nombre_activo):
    """Genera y guarda el reporte de diferencias de ajustes"""
    try:
        carpetas = crear_estructura_carpetas(nombre_activo)
        
        # Manejar estructura de columnas
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
            print(f"Columnas críticas no encontradas para {nombre_activo}")
            return None
        
        # Crear reporte
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
        
        # Guardar reporte
        fecha_reporte = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_archivo = f"{carpetas['reportes_ajustes']}/Ajustes_{nombre_activo}_{fecha_reporte}.csv"
        reporte.to_csv(nombre_archivo, index=False, float_format='%.6f')
        
        return nombre_archivo
    except Exception as e:
        print(f"Error generando reporte de ajustes: {str(e)}")
        return None

def procesar_activo(ticker, nombre_activo):
    """Función principal para procesar un activo"""
    try:
        # Descargar datos históricos
        datos = yf.download(
            ticker, 
            start=FECHA_INICIO_MAXIMA, 
            end=datetime.now().strftime('%Y-%m-%d'),
            auto_adjust=False,
            progress=False,
            group_by='ticker'
        )
        
        if datos.empty:
            print(f"No se encontraron datos para {nombre_activo}")
            return None, None, None
        
        # Guardar datos completos
        archivo_completo = guardar_datos_completos(datos, nombre_activo)
        if archivo_completo:
            print(f"Datos completos guardados: {archivo_completo}")
        
        # Generar reporte de ajustes
        archivo_ajustes = generar_reporte_ajustes(datos, nombre_activo)
        if archivo_ajustes:
            print(f"Reporte de ajustes guardado: {archivo_ajustes}")
        
        # Guardar eventos
        archivo_eventos = guardar_eventos(ticker, nombre_activo)
        if archivo_eventos:
            print(f"Reporte de eventos guardado: {archivo_eventos}")
        
        return archivo_completo, archivo_ajustes, archivo_eventos
    
    except Exception as e:
        print(f"Error procesando {nombre_activo}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None, None

# Configuración de activos a procesar
activos = json.load(open('../stock_symbols.json', 'r'))

# Procesar cada activo
for activo in activos:
    print("\n" + "="*70)
    print(f" Procesando {activo['nombre']} ".center(70, '#'))
    completo, ajustes, eventos = procesar_activo(activo['ticker'], activo['nombre'])
    print(f"\nResultados para {activo['nombre']}:")
    print(f" - Datos completos: {completo}")
    print(f" - Reporte ajustes: {ajustes}")
    print(f" - Reporte eventos: {eventos}")
    print("="*70 + "\n")

print("Proceso completado".center(70, '='))