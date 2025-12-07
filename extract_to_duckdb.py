"""
Script de extracción de datos usando DuckDB nativo
Lee archivo CSV con delimitador especial y carga a DuckDB
"""
import duckdb
import time
from pathlib import Path

# Configuración
INPUT_FILE = 'data/raw/entrada.txt'
DATABASE_FILE = 'data/duckdb/ads.duckdb'

def load_csv_to_duckdb():
    """Carga archivo CSV con encoding latin-1 a DuckDB"""
    print("Cargando CSV a DuckDB...")
    
    start_time = time.time()
    
    # Conectar a la base de datos de dbt
    con = duckdb.connect(DATABASE_FILE)
    
    # Crear schema raw si no existe
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    
    # Cargar datos
    sql_query = f"""
        CREATE OR REPLACE TABLE raw.victimas AS
        SELECT *
        FROM read_csv_auto(
            '{INPUT_FILE}',
            delim='»',
            header=True,
            encoding='latin-1',
            ignore_errors=True,
            all_varchar=True
        );
    """
    
    try:
        con.execute(sql_query)
        
        # Obtener estadísticas
        count = con.execute("SELECT COUNT(*) FROM raw.victimas").fetchone()[0]
        columns = con.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='victimas' AND table_schema='raw'").fetchone()[0]
        
        print("Datos cargados exitosamente:")
        print(f"   Registros: {count:,}")
        print(f"   Columnas: {columns}")
        print(f"   Base de datos: {DATABASE_FILE}")
        print(f"   Tabla: raw.victimas")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        con.close()
    
    elapsed = time.time() - start_time
    print(f"Tiempo total: {elapsed:.2f} segundos")


if __name__ == "__main__":
    load_csv_to_duckdb()
