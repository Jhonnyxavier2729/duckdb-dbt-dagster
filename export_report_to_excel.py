import polars as pl
import duckdb
import matplotlib.pyplot as plt
import xlsxwriter
from pathlib import Path
from datetime import datetime
import tempfile
import os

# Configurar rutas
PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / "data" / "duckdb" / "ads.duckdb"

# Conectar a DuckDB
conn = duckdb.connect(str(DB_PATH), read_only=True)

# Configurar estilo de gráficos
plt.style.use('seaborn-v0_8-darkgrid')

def load_mart_data(table_name: str) -> pl.DataFrame:
    """Carga datos de un mart desde DuckDB usando Polars"""
    query = f"SELECT * FROM marts.{table_name}"
    df = pl.read_database(query, conn)
    return df

def create_bar_chart(df: pl.DataFrame, title: str, output_path: str):
    """Crea un gráfico de barras horizontal"""
    fig, ax = plt.subplots(figsize=(12, 8))
    
    metrica = df['metrica'].to_list()
    valor = df['valor'].to_list()
    
    bars = ax.barh(metrica, valor)
    
    for i, bar in enumerate(bars):
        bar.set_color(plt.cm.viridis(i / len(bars)))
    
    ax.set_xlabel('Cantidad de Personas', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    
    for i, v in enumerate(valor):
        ax.text(v, i, f' {v:,}', va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

def create_vertical_bar_chart(df: pl.DataFrame, title: str, output_path: str):
    """Crea un gráfico de barras vertical"""
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Filtrar el total si existe
    df_filtered = df.filter(~pl.col('metrica').str.contains('Total'))
    
    metrica = df_filtered['metrica'].to_list()
    valor = df_filtered['valor'].to_list()
    
    # Limpiar etiquetas
    labels = [m.replace('Personas por Género: ', '').replace('Personas por Etnia: ', '') 
              for m in metrica]
    
    # Colores
    colors = ['#8DD3C7', '#FFFFB3', '#FB8072', '#80B1D3', '#FDB462', 
              '#B3DE69', '#FCCDE5', '#D9D9D9', '#BC80BD', '#CCEBC5']
    
    bars = ax.bar(labels, valor, color=colors[:len(valor)])
    
    ax.set_ylabel('Cantidad de Personas', fontsize=12, fontweight='bold')
    ax.set_xlabel('Categoría', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    
    # Agregar valores encima de las barras
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Rotar etiquetas si son largas
    plt.xticks(rotation=45, ha='right')
    
    # Formato del eje Y
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

def create_pie_chart(df: pl.DataFrame, title: str, output_path: str):
    """Crea un gráfico de pastel con leyenda"""
    fig, ax = plt.subplots(figsize=(12, 8))
    
    df_filtered = df.filter(~pl.col('metrica').str.contains('Total'))
    
    metrica = df_filtered['metrica'].to_list()
    valor = df_filtered['valor'].to_list()
    
    # Limpiar etiquetas
    labels = [m.replace('Personas por Género: ', '').replace('Personas por Etnia: ', '') 
              for m in metrica]
    
    # Crear labels con cantidad y porcentaje para la leyenda
    total = sum(valor)
    labels_con_cantidad = [f'{label}: {val:,} ({100*val/total:.2f}%)' for label, val in zip(labels, valor)]
    
    # Colores definidos para asegurar que todas las categorías tengan color
    colors = ['#8DD3C7', '#FFFFB3', '#FB8072', '#80B1D3', '#FDB462', 
              '#B3DE69', '#FCCDE5', '#D9D9D9', '#BC80BD', '#CCEBC5']
    
    # Función para mostrar solo porcentajes > 1%
    def autopct_format(pct):
        return f'{pct:.1f}%' if pct > 1 else ''
    
    # Graficar solo con porcentajes, sin labels en el gráfico
    wedges, texts, autotexts = ax.pie(valor, labels=None, autopct=autopct_format,
                                        startangle=90, colors=colors[:len(valor)],
                                        pctdistance=0.75)
    
    # Formatear porcentajes
    for autotext in autotexts:
        autotext.set_color('black')
        autotext.set_fontsize(12)
        autotext.set_fontweight('bold')
    
    # Agregar leyenda fuera del gráfico
    ax.legend(labels_con_cantidad, loc='center left', bbox_to_anchor=(1, 0, 0.5, 1),
              fontsize=10, frameon=True)
    
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

def write_dataframe_to_worksheet(workbook, worksheet, df_pandas, start_row=0, start_col=0):
    """Escribe un DataFrame de pandas en una hoja de Excel"""
    # Escribir encabezados
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#4472C4',
        'font_color': 'white',
        'border': 1
    })
    
    for col_num, column in enumerate(df_pandas.columns):
        worksheet.write(start_row, start_col + col_num, column, header_format)
    
    # Formato para datos
    data_format = workbook.add_format({'border': 1})
    number_format = workbook.add_format({'border': 1, 'num_format': '#,##0'})
    
    # Escribir datos
    for row_num, row in enumerate(df_pandas.values, start=start_row + 1):
        for col_num, value in enumerate(row):
            if isinstance(value, (int, float)):
                worksheet.write(row_num, start_col + col_num, value, number_format)
            else:
                worksheet.write(row_num, start_col + col_num, value, data_format)
    
    # Ajustar ancho de columnas
    for col_num, column in enumerate(df_pandas.columns):
        if column == 'metrica':
            # Columna de métricas más ancha
            worksheet.set_column(start_col + col_num, start_col + col_num, 60)
        elif column == 'valor':
            # Columna de valores con ancho fijo
            worksheet.set_column(start_col + col_num, start_col + col_num, 18)
        else:
            max_length = max(
                df_pandas[column].astype(str).str.len().max(),
                len(str(column))
            )
            worksheet.set_column(start_col + col_num, start_col + col_num, min(max_length + 2, 50))

print("Cargando datos de marts...")

# Cargar todos los marts
print("\n1. Cargando mart_genero...")
df_genero = load_mart_data('mart_genero')

print("2. Cargando mart_edad...")
df_edad = load_mart_data('mart_edad')

print("3. Cargando mart_etnia...")
df_etnia = load_mart_data('mart_etnia')

print("4. Cargando mart_ciudad...")
df_ciudad = load_mart_data('mart_ciudad')

print("5. Cargando mart_discapacidad...")
df_discapacidad = load_mart_data('mart_discapacidad')

# Cerrar conexión
conn.close()

# Crear nombre de archivo con fecha de hoy
fecha_hoy = datetime.now().strftime('%Y-%m-%d')
report_dir = PROJECT_ROOT / 'data' / 'report'
report_dir.mkdir(parents=True, exist_ok=True)
output_file = report_dir / f'reporte_victimas_{fecha_hoy}.xlsx'

print(f"\nGenerando graficos temporales...")

# Crear directorio temporal para imágenes
temp_dir = tempfile.mkdtemp()

# Generar gráficos
print("  Generando grafico de genero...")
img_genero = os.path.join(temp_dir, 'genero.png')
create_vertical_bar_chart(df_genero, 'Distribucion por Genero', img_genero)

print("  Generando grafico de edad...")
img_edad = os.path.join(temp_dir, 'edad.png')
create_bar_chart(df_edad, 'Distribucion por Grupo de Edad', img_edad)

print("  Generando grafico de etnia...")
img_etnia = os.path.join(temp_dir, 'etnia.png')
create_bar_chart(df_etnia, 'Distribucion por Etnia', img_etnia)

print("  Generando grafico de ciudad (Top 20)...")
img_ciudad = os.path.join(temp_dir, 'ciudad.png')
create_bar_chart(df_ciudad.head(20), 'Top 20 Ciudades', img_ciudad)

print("  Generando grafico de discapacidad...")
img_discapacidad = os.path.join(temp_dir, 'discapacidad.png')
create_pie_chart(df_discapacidad, 'Distribucion con/sin Discapacidad', img_discapacidad)

print(f"\nGenerando archivo Excel: {output_file.name}")

# Crear archivo Excel
workbook = xlsxwriter.Workbook(str(output_file))

# Formato para título
title_format = workbook.add_format({
    'bold': True,
    'font_size': 16,
    'font_color': '#4472C4',
    'align': 'center'
})

# Hoja 1: Género
print("  Agregando hoja Genero...")
worksheet = workbook.add_worksheet("Genero")
worksheet.write('A1', 'DISTRIBUCION POR GENERO', title_format)
worksheet.insert_image('A3', img_genero, {'x_scale': 0.6, 'y_scale': 0.6})
write_dataframe_to_worksheet(workbook, worksheet, df_genero.to_pandas(), start_row=35, start_col=0)

# Hoja 2: Edad
print("  Agregando hoja Edad...")
worksheet = workbook.add_worksheet("Edad")
worksheet.write('A1', 'DISTRIBUCION POR GRUPO DE EDAD', title_format)
worksheet.insert_image('A3', img_edad, {'x_scale': 0.6, 'y_scale': 0.6})
write_dataframe_to_worksheet(workbook, worksheet, df_edad.to_pandas(), start_row=35, start_col=0)

# Hoja 3: Etnia
print("  Agregando hoja Etnia...")
worksheet = workbook.add_worksheet("Etnia")
worksheet.write('A1', 'DISTRIBUCION POR ETNIA', title_format)
worksheet.insert_image('A3', img_etnia, {'x_scale': 0.6, 'y_scale': 0.6})
write_dataframe_to_worksheet(workbook, worksheet, df_etnia.to_pandas(), start_row=35, start_col=0)

# Hoja 4: Ciudad
print("  Agregando hoja Ciudad...")
worksheet = workbook.add_worksheet("Ciudad")
worksheet.write('A1', 'DISTRIBUCION POR CIUDAD (TOP 20)', title_format)
worksheet.insert_image('A3', img_ciudad, {'x_scale': 0.6, 'y_scale': 0.6})
write_dataframe_to_worksheet(workbook, worksheet, df_ciudad.head(20).to_pandas(), start_row=35, start_col=0)

# Hoja 5: Discapacidad
print("  Agregando hoja Discapacidad...")
worksheet = workbook.add_worksheet("Discapacidad")
worksheet.write('A1', 'DISTRIBUCION CON/SIN DISCAPACIDAD', title_format)
worksheet.insert_image('A3', img_discapacidad, {'x_scale': 0.6, 'y_scale': 0.6})
write_dataframe_to_worksheet(workbook, worksheet, df_discapacidad.to_pandas(), start_row=35, start_col=0)

workbook.close()

# Limpiar archivos temporales
print("\nLimpiando archivos temporales...")
for file in [img_genero, img_edad, img_etnia, img_ciudad, img_discapacidad]:
    if os.path.exists(file):
        os.remove(file)
os.rmdir(temp_dir)

print(f"\n[COMPLETO] Archivo Excel con graficos generado exitosamente!")
print(f"Archivo: {output_file}")

