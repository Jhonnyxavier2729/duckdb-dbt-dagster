import duckdb
from pathlib import Path
from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

# Ruta base del proyecto
BASE_PATH = Path(__file__).parent.parent

@asset(
    description="Extrae CSV y carga en DuckDB como tabla raw.victimas"
)
def extract_csv_to_duckdb(context: AssetExecutionContext):
    """
    Carga datos desde CSV a la capa raw de DuckDB
    """
    db_path = BASE_PATH / "dbt_code" / "ads.duckdb"
    csv_path = BASE_PATH / "Notebook" / "input" / "entrada.txt"
    
    context.log.info(f"Conectando a {db_path}")
    conn = duckdb.connect(str(db_path))
    
    context.log.info(f"Cargando datos desde {csv_path}")
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE OR REPLACE TABLE raw.victimas AS 
        SELECT * FROM read_csv_auto(
            ?,
            delim='»',
            header=True,
            encoding='latin-1',
            ignore_errors=True,
            all_varchar=True
        )
    """, [str(csv_path)])
    
    row_count = conn.execute("SELECT COUNT(*) FROM raw.victimas").fetchone()[0]
    conn.close()
    
    context.log.info(f"✅ {row_count} registros cargados en raw.victimas")
    return {"rows": row_count}


@dbt_assets(
    manifest=BASE_PATH / "dbt_code" / "target" / "manifest.json",
)
def dbt_victimas_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Ejecuta todos los modelos dbt: staging -> intermediate -> refined
    """
    yield from dbt.cli(["build"], context=context).stream()
