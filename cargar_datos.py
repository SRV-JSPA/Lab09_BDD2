import pandas as pd
from pymongo import MongoClient
import os
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import seaborn as sns
from matplotlib.ticker import FuncFormatter

plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("Set2")

def conexion():
    connection_string = "mongodb+srv://gaby:2912@lab01.cg7w5.mongodb.net/?retryWrites=true&w=majority&appName=lab01"
    client = MongoClient(connection_string)
    db = client["lab01"] 
    return db

def procesar_datos(df):
    df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d.%m.%Y', errors='coerce')
    
    numeric_columns = ['Último', 'Apertura', 'Máximo', 'Mínimo']
    for col in numeric_columns:
        df[col] = df[col].str.replace(',', '.').astype(float)
    
    df.rename(columns={"Vol.": "Volumen"}, inplace=True)
    df['Volumen'] = df['Volumen'].str.replace('M', '').str.replace(',', '.').astype(float) * 1000000
    
    df.rename(columns={"% var.": "Variacion"}, inplace=True)
    df['Variacion'] = df['Variacion'].str.replace('%', '').str.replace(',', '.').astype(float) / 100
    
    df['Año'] = df['Fecha'].dt.year
    df['Mes'] = df['Fecha'].dt.month
    df['Día'] = df['Fecha'].dt.day
    df['DiaSemana'] = df['Fecha'].dt.day_name()
    
    df['Rango'] = df['Máximo'] - df['Mínimo']
    
    df['CierrePosNeg'] = (df['Último'] > df['Apertura']).astype(int)
    
    return df

def cargar_datos(file_path, db):

    collection = db["apple_stocks"]
    
    documentos_existentes = collection.count_documents({})
    
    if documentos_existentes > 0:
        return None
    
    
    df = pd.read_csv(file_path)
    df = procesar_datos(df)
    
    column_mapping = {
        "Vol.": "Volumen",
        "% var.": "Variacion"
    }
    
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)
    
    data = df.to_dict("records")
    result = collection.insert_many(data)
    
    return result

def obtener_todos_los_datos(db):
    collection = db["apple_stocks"]
    
    resultados = list(collection.find({}, {"_id": 0}))
    
    df = pd.DataFrame(resultados)
    
    return df

def generar_datos_para_graficos(df_completo):

    df_ordenado = df_completo.sort_values(by='Fecha')
    
    df_precios = df_ordenado[['Fecha', 'Último', 'Apertura', 'Máximo', 'Mínimo']].copy()
    
    df_volumen = df_ordenado[['Fecha', 'Volumen', 'CierrePosNeg']].copy()
    
    df_variaciones = df_completo[['Variacion']].copy()
    
    primer_precio = df_ordenado.iloc[0]['Apertura']
    ultimo_precio = df_ordenado.iloc[-1]['Último']
    rendimiento_total = ((ultimo_precio - primer_precio) / primer_precio) * 100
    
    dias_positivos = sum(1 for v in df_completo['Variacion'] if v > 0)
    dias_totales = len(df_completo)
    porcentaje_dias_positivos = (dias_positivos / dias_totales) * 100 if dias_totales > 0 else 0
    
    rendimiento_kpi = {
        "rendimientoTotal": rendimiento_total,
        "porcentajeDiasPositivos": porcentaje_dias_positivos
    }
    
    volatilidad = df_completo['Variacion'].abs().mean() * 100
    rango_promedio = df_completo['Rango'].mean()
    rango_porcentual_promedio = (df_completo['Rango'] / df_completo['Apertura']).mean() * 100
    
    volatilidad_kpi = {
        "volatilidad": volatilidad,
        "rangoPromedio": rango_promedio,
        "rangoPorcentualPromedio": rango_porcentual_promedio
    }
    
    return {
        "df_precios": df_precios,
        "df_volumen": df_volumen,
        "df_variaciones": df_variaciones,
        "rendimiento_kpi": rendimiento_kpi,
        "volatilidad_kpi": volatilidad_kpi
    }

def crear_directorio_salida():
    output_dir = "graficos_apple"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

def generar_grafico_precios(df, output_dir):
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(df['Fecha'], df['Último'], 'o-', linewidth=2, markersize=4, color='#1f77b4', 
            label='Precio de cierre (USD)')
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    plt.xticks(rotation=45)
    
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'${x:.2f}'))
    
    ax.set_title('Evolución del Precio de Acción de Apple', fontsize=16, pad=20)
    ax.set_xlabel('Fecha', fontsize=12)
    ax.set_ylabel('Precio (USD)', fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.legend()
    
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'grafico_precios.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

def generar_grafico_volumen(df, output_dir):
    fig, ax = plt.subplots(figsize=(12, 6))
    
    colores = ['#2ca02c' if pos == 1 else '#d62728' for pos in df['CierrePosNeg']]
    
    ax.bar(df['Fecha'], df['Volumen'], color=colores, alpha=0.7, width=1)
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    plt.xticks(rotation=45)
    
    def millones(x, pos):
        return f'{x/1e6:.1f}M'
    
    ax.yaxis.set_major_formatter(FuncFormatter(millones))
    
    ax.set_title('Volumen de Transacciones de Apple', fontsize=16, pad=20)
    ax.set_xlabel('Fecha', fontsize=12)
    ax.set_ylabel('Volumen', fontsize=12)
    
    from matplotlib.patches import Patch
    leyenda_elementos = [
        Patch(facecolor='#2ca02c', edgecolor='#2ca02c', label='Cierre positivo'),
        Patch(facecolor='#d62728', edgecolor='#d62728', label='Cierre negativo')
    ]
    ax.legend(handles=leyenda_elementos)
    
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'grafico_volumen.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

def generar_histograma_variaciones(df, output_dir):
    fig, ax = plt.subplots(figsize=(10, 6))
    
    bins = [-0.03, -0.02, -0.01, 0, 0.01, 0.02, 0.03]
    
    bin_labels = []
    for i in range(len(bins) - 1):
        bin_labels.append(f"{bins[i]*100:.1f}% a {bins[i+1]*100:.1f}%")
    
    n, bins, patches = ax.hist(df['Variacion'], bins=bins, alpha=0.7, edgecolor='black')
    
    for i, patch in enumerate(patches):
        if bins[i] >= 0:
            patch.set_facecolor('#2ca02c')  
        else:
            patch.set_facecolor('#d62728')  
    
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x*100:.1f}%'))
    
    ax.set_title('Distribución de Variaciones Diarias', fontsize=16, pad=20)
    ax.set_xlabel('Variación porcentual', fontsize=12)
    ax.set_ylabel('Número de días', fontsize=12)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'histograma_variaciones.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

def generar_grafico_kpis(rendimiento_kpi, volatilidad_kpi, output_dir):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    rendimiento = rendimiento_kpi.get('rendimientoTotal', 0)
    dias_positivos = rendimiento_kpi.get('porcentajeDiasPositivos', 0)
    
    volatilidad = volatilidad_kpi.get('volatilidad', 0)
    rango_promedio = volatilidad_kpi.get('rangoPromedio', 0)
    
    color_rendimiento = '#2ca02c' if rendimiento >= 0 else '#d62728'
    
    ax1.set_title('Rendimiento Acumulado', fontsize=16, pad=20)
    ax1.text(0.5, 0.5, f"{rendimiento:.2f}%", ha='center', va='center',
             fontsize=30, fontweight='bold', color=color_rendimiento)
    ax1.text(0.5, 0.3, f"Días positivos: {dias_positivos:.2f}%", ha='center', va='center',
             fontsize=14, color='#7f7f7f')
    ax1.axis('off')
    
    ax2.set_title('Volatilidad Promedio', fontsize=16, pad=20)
    ax2.text(0.5, 0.5, f"{volatilidad:.2f}%", ha='center', va='center',
             fontsize=30, fontweight='bold', color='#1f77b4')
    ax2.text(0.5, 0.3, f"Rango promedio: ${rango_promedio:.2f}", ha='center', va='center',
             fontsize=14, color='#7f7f7f')
    ax2.axis('off')
    
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'kpis.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

def generar_grafico_velas(df, output_dir):
    from matplotlib.patches import Rectangle
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    width = 0.6
    
    for i, row in df.iterrows():
        if row['Último'] >= row['Apertura']:
            color = '#2ca02c'  
            bottom = row['Apertura']
            height = row['Último'] - row['Apertura']
        else:
            color = '#d62728'  
            bottom = row['Último']
            height = row['Apertura'] - row['Último']
        
        rect = Rectangle(
            (mdates.date2num(row['Fecha']) - width/2, bottom),
            width, height, 
            facecolor=color, 
            edgecolor='black',
            linewidth=1,
            alpha=0.8
        )
        ax.add_patch(rect)
        
        ax.vlines(
            x=mdates.date2num(row['Fecha']),
            ymin=row['Mínimo'],
            ymax=row['Máximo'],
            color='black',
            linewidth=1
        )
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    plt.xticks(rotation=45)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'${x:.2f}'))
    
    min_price = df['Mínimo'].min() * 0.99
    max_price = df['Máximo'].max() * 1.01
    ax.set_ylim(min_price, max_price)
    
    ax.set_title('Gráfico de Velas - Apple (AAPL)', fontsize=16, pad=20)
    ax.set_xlabel('Fecha', fontsize=12)
    ax.set_ylabel('Precio (USD)', fontsize=12)
    
    from matplotlib.patches import Patch
    leyenda_elementos = [
        Patch(facecolor='#2ca02c', edgecolor='black', label='Alcista (Cierre > Apertura)'),
        Patch(facecolor='#d62728', edgecolor='black', label='Bajista (Cierre < Apertura)')
    ]
    ax.legend(handles=leyenda_elementos, loc='upper left')
    
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'grafico_velas.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)

def generar_todos_los_graficos(db):
    output_dir = crear_directorio_salida()
    
    df_completo = obtener_todos_los_datos(db)
    
    datos_graficos = generar_datos_para_graficos(df_completo)
    
    generar_grafico_precios(datos_graficos["df_precios"], output_dir)
    generar_grafico_volumen(datos_graficos["df_volumen"], output_dir)
    generar_histograma_variaciones(datos_graficos["df_variaciones"], output_dir)
    generar_grafico_kpis(datos_graficos["rendimiento_kpi"], datos_graficos["volatilidad_kpi"], output_dir)
    generar_grafico_velas(datos_graficos["df_precios"], output_dir)
    
def main():
    db = conexion()
    
    csv_file = "apple.csv"
    if os.path.exists(csv_file):
        cargar_datos(csv_file, db)
    else:
        print(f"El archivo {csv_file} no existe, se usarán los datos existentes en MongoDB.")
    
    collection = db["apple_stocks"]
    documentos_existentes = collection.count_documents({})
    
    if documentos_existentes > 0:
        generar_todos_los_graficos(db)
    else:
        print("No hay datos en la colección. No se pueden generar gráficos.")

if __name__ == "__main__":
    main()