"""Reporting assets - Generate Excel reports with charts"""

import dagster as dg
from dagster_duckdb import DuckDBResource
from pathlib import Path
import os
import polars as pl
import matplotlib.pyplot as plt
import xlsxwriter
from datetime import datetime
import tempfile


# Define paths
BASE_DIR = Path(__file__).parent.parent.parent.parent.parent


# ===== Helper functions for report generation =====

def _load_mart_data(conn, table_name: str) -> pl.DataFrame:
    """Load mart data from DuckDB using Polars"""
    query = f"SELECT * FROM marts.{table_name}"
    df = pl.read_database(query, conn)
    return df


def _create_bar_chart(df: pl.DataFrame, title: str, output_path: str):
    """Create a horizontal bar chart"""
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


def _create_vertical_bar_chart(df: pl.DataFrame, title: str, output_path: str):
    """Create a vertical bar chart"""
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Filter total if exists
    df_filtered = df.filter(~pl.col('metrica').str.contains('Total'))
    
    metrica = df_filtered['metrica'].to_list()
    valor = df_filtered['valor'].to_list()
    
    # Clean labels
    labels = [m.replace('Personas por Género: ', '').replace('Personas por Etnia: ', '') 
              for m in metrica]
    
    # Colors
    colors = ['#8DD3C7', '#FFFFB3', '#FB8072', '#80B1D3', '#FDB462', 
              '#B3DE69', '#FCCDE5', '#D9D9D9', '#BC80BD', '#CCEBC5']
    
    bars = ax.bar(labels, valor, color=colors[:len(valor)])
    
    ax.set_ylabel('Cantidad de Personas', fontsize=12, fontweight='bold')
    ax.set_xlabel('Categoría', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    
    # Add values on top of bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Rotate labels if long
    plt.xticks(rotation=45, ha='right')
    
    # Y-axis format
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def _create_pie_chart(df: pl.DataFrame, title: str, output_path: str):
    """Create a pie chart with legend"""
    fig, ax = plt.subplots(figsize=(12, 8))
    
    df_filtered = df.filter(~pl.col('metrica').str.contains('Total'))
    
    metrica = df_filtered['metrica'].to_list()
    valor = df_filtered['valor'].to_list()
    
    # Clean labels
    labels = [m.replace('Personas por Género: ', '').replace('Personas por Etnia: ', '') 
              for m in metrica]
    
    # Create labels with count and percentage for legend
    total = sum(valor)
    labels_con_cantidad = [f'{label}: {val:,} ({100*val/total:.2f}%)' for label, val in zip(labels, valor)]
    
    # Defined colors to ensure all categories have color
    colors = ['#8DD3C7', '#FFFFB3', '#FB8072', '#80B1D3', '#FDB462', 
              '#B3DE69', '#FCCDE5', '#D9D9D9', '#BC80BD', '#CCEBC5']
    
    # Function to show only percentages > 1%
    def autopct_format(pct):
        return f'{pct:.1f}%' if pct > 1 else ''
    
    # Plot with percentages only, no labels on chart
    wedges, texts, autotexts = ax.pie(valor, labels=None, autopct=autopct_format,
                                        startangle=90, colors=colors[:len(valor)],
                                        pctdistance=0.75)
    
    # Format percentages
    for autotext in autotexts:
        autotext.set_color('black')
        autotext.set_fontsize(12)
        autotext.set_fontweight('bold')
    
    # Add legend outside chart
    ax.legend(labels_con_cantidad, loc='center left', bbox_to_anchor=(1, 0, 0.5, 1),
              fontsize=10, frameon=True)
    
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def _write_dataframe_to_worksheet(workbook, worksheet, df_pandas, start_row=0, start_col=0):
    """Write a pandas DataFrame to an Excel worksheet"""
    # Write headers
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#4472C4',
        'font_color': 'white',
        'border': 1
    })
    
    for col_num, column in enumerate(df_pandas.columns):
        worksheet.write(start_row, start_col + col_num, column, header_format)
    
    # Format for data
    data_format = workbook.add_format({'border': 1})
    number_format = workbook.add_format({'border': 1, 'num_format': '#,##0'})
    
    # Write data
    for row_num, row in enumerate(df_pandas.values, start=start_row + 1):
        for col_num, value in enumerate(row):
            if isinstance(value, (int, float)):
                worksheet.write(row_num, start_col + col_num, value, number_format)
            else:
                worksheet.write(row_num, start_col + col_num, value, data_format)
    
    # Adjust column widths
    for col_num, column in enumerate(df_pandas.columns):
        if column == 'metrica':
            # Wider metrics column
            worksheet.set_column(start_col + col_num, start_col + col_num, 60)
        elif column == 'valor':
            # Fixed width value column
            worksheet.set_column(start_col + col_num, start_col + col_num, 18)
        else:
            max_length = max(
                df_pandas[column].astype(str).str.len().max(),
                len(str(column))
            )
            worksheet.set_column(start_col + col_num, start_col + col_num, min(max_length + 2, 50))


# ===== Asset de Dagster para generar reporte Excel =====

@dg.asset(
    deps=[
        dg.AssetKey(["marts", "mart_genero"]),
        dg.AssetKey(["marts", "mart_edad"]),
        dg.AssetKey(["marts", "mart_etnia"]),
        dg.AssetKey(["marts", "mart_ciudad"]),
        dg.AssetKey(["marts", "mart_discapacidad"])
    ],
    kinds={"polars", "excel"},
    group_name="reporting"
)
def generate_excel_report(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """
    Generate an Excel report with charts from DBT marts.
    Dependencies: requires all marts to be materialized first
    """
    # Configure chart style
    plt.style.use('seaborn-v0_8-darkgrid')
    
    context.log.info("Loading mart data from DuckDB...")
    
    # Connect to DuckDB
    with duckdb.get_connection() as conn:
        # Load all marts
        context.log.info("Loading mart_genero...")
        df_genero = _load_mart_data(conn, 'mart_genero')
        
        context.log.info("Loading mart_edad...")
        df_edad = _load_mart_data(conn, 'mart_edad')
        
        context.log.info("Loading mart_etnia...")
        df_etnia = _load_mart_data(conn, 'mart_etnia')
        
        context.log.info("Loading mart_ciudad...")
        df_ciudad = _load_mart_data(conn, 'mart_ciudad')
        
        context.log.info("Loading mart_discapacidad...")
        df_discapacidad = _load_mart_data(conn, 'mart_discapacidad')
    
    # Create report directory
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    report_dir = BASE_DIR / 'data' / 'report'
    report_dir.mkdir(parents=True, exist_ok=True)
    output_file = report_dir / f'reporte_victimas_{fecha_hoy}.xlsx'
    
    context.log.info("Generating temporary charts...")
    
    # Create temporary directory for images
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Generate charts
        context.log.info("Generating gender chart...")
        img_genero = os.path.join(temp_dir, 'genero.png')
        _create_vertical_bar_chart(df_genero, 'Distribution by Gender', img_genero)
        
        context.log.info("Generating age chart...")
        img_edad = os.path.join(temp_dir, 'edad.png')
        _create_bar_chart(df_edad, 'Distribution by Age Group', img_edad)
        
        context.log.info("Generating ethnicity chart...")
        img_etnia = os.path.join(temp_dir, 'etnia.png')
        _create_bar_chart(df_etnia, 'Distribution by Ethnicity', img_etnia)
        
        context.log.info("Generating city chart (Top 20)...")
        img_ciudad = os.path.join(temp_dir, 'ciudad.png')
        _create_bar_chart(df_ciudad.head(20), 'Top 20 Cities', img_ciudad)
        
        context.log.info("Generating disability chart...")
        img_discapacidad = os.path.join(temp_dir, 'discapacidad.png')
        _create_pie_chart(df_discapacidad, 'Distribution with/without Disability', img_discapacidad)
        
        context.log.info(f"Generating Excel file: {output_file.name}")
        
        # Create Excel file
        workbook = xlsxwriter.Workbook(str(output_file))
        
        # Title format
        title_format = workbook.add_format({
            'bold': True,
            'font_size': 16,
            'font_color': '#4472C4',
            'align': 'center'
        })
        
        # Sheet definitions
        sheets_config = [
            ('Genero', 'DISTRIBUTION BY GENDER', img_genero, df_genero),
            ('Edad', 'DISTRIBUTION BY AGE GROUP', img_edad, df_edad),
            ('Etnia', 'DISTRIBUTION BY ETHNICITY', img_etnia, df_etnia),
            ('Ciudad', 'DISTRIBUTION BY CITY (TOP 20)', img_ciudad, df_ciudad.head(20)),
            ('Discapacidad', 'DISTRIBUTION WITH/WITHOUT DISABILITY', img_discapacidad, df_discapacidad)
        ]
        
        # Create sheets
        for sheet_name, title, img_path, df_data in sheets_config:
            context.log.info(f"Adding sheet {sheet_name}...")
            worksheet = workbook.add_worksheet(sheet_name)
            worksheet.write('A1', title, title_format)
            worksheet.insert_image('A3', img_path, {'x_scale': 0.6, 'y_scale': 0.6})
            _write_dataframe_to_worksheet(workbook, worksheet, df_data.to_pandas(), start_row=35, start_col=0)
        
        workbook.close()
        
        context.log.info("Cleaning temporary files...")
        # Clean temporary files
        for file in [img_genero, img_edad, img_etnia, img_ciudad, img_discapacidad]:
            if os.path.exists(file):
                os.remove(file)
        os.rmdir(temp_dir)
        
        context.log.info(f"Excel file generated successfully: {output_file}")
        
        return dg.MaterializeResult(
            metadata={
                "output_file": dg.MetadataValue.path(str(output_file)),
                "report_date": dg.MetadataValue.text(fecha_hoy),
                "sheets_created": dg.MetadataValue.int(len(sheets_config)),
                "genero_records": dg.MetadataValue.int(len(df_genero)),
                "edad_records": dg.MetadataValue.int(len(df_edad)),
                "etnia_records": dg.MetadataValue.int(len(df_etnia)),
                "ciudad_records": dg.MetadataValue.int(len(df_ciudad)),
                "discapacidad_records": dg.MetadataValue.int(len(df_discapacidad)),
            }
        )
        
    except Exception as e:
        # Clean up on error
        context.log.error(f"Error generating report: {e}")
        if os.path.exists(temp_dir):
            for file in os.listdir(temp_dir):
                os.remove(os.path.join(temp_dir, file))
            os.rmdir(temp_dir)
        raise
