import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path
import os
import duckdb
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Definir rutas
BASE_DIR = Path(__file__).parent.parent.parent.parent
INPUT_FILE = str(BASE_DIR / "data" / "raw" / "entrada.txt")
DATABASE_FILE = str(BASE_DIR / "data" / "duckdb" / "ads.duckdb")
DBT_PROJECT_PATH = BASE_DIR / "dbt_code"
DBT_MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"

@dg.asset
def load_csv_to_duckdb(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """Load CSV file with latin-1 encoding to DuckDB"""
    context.log.info("Loading CSV to DuckDB...")
 
    
    # Connect to dbt database
    with duckdb.get_connection() as con:
        # Create raw schema if not exists
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        
        # Load data
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
            
            # Get statistics
            count = con.execute("SELECT COUNT(*) FROM raw.victimas").fetchone()[0]
            columns = con.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='victimas' AND table_schema='raw'").fetchone()[0]
            
            context.log.info("Data loaded successfully:")
            context.log.info(f"   Records: {count:,}")
            context.log.info(f"   Columns: {columns}")
            context.log.info(f"   Database: {DATABASE_FILE}")
            
            return dg.MaterializeResult(
                metadata={
                    "records": dg.MetadataValue.int(count),
                    "columns": dg.MetadataValue.int(columns),
                    "database": dg.MetadataValue.path(DATABASE_FILE),
                    "input_file": dg.MetadataValue.path(INPUT_FILE),
                }
            )
            
        except Exception as e:
            context.log.error(f"Error: {e}")
            raise


# DBT Assets - se cargan dinámicamente si existe el manifest
if DBT_MANIFEST_PATH.exists():
    @dbt_assets(
        manifest=DBT_MANIFEST_PATH,
    )
    def dbt_project_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        """
        Ejecuta todos los modelos de DBT como assets de Dagster.
        Dependencias: requiere que load_csv_to_duckdb se ejecute primero
        """
        yield from dbt.cli(["build"], context=context).stream()
else:
    # Si no existe el manifest, crear un placeholder
    @dg.asset(
        deps=[load_csv_to_duckdb]
    )
    def dbt_project_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        """
        Ejecuta todos los modelos de DBT.
        Nota: Primero genera el manifest con: python generate_dbt_manifest.py
        """
        context.log.info("Ejecutando modelos DBT...")
        result = dbt.cli(["build"], context=context)
        return result
    

    
@dg.asset(
    deps=[dbt_project_assets]
)
def export_to_csv(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """Export cleaned data to CSV with pipe delimiter"""
    context.log.info("Exporting data to CSV...")
    
    OUTPUT_FILE = BASE_DIR / "data" / "output" / "victimas_cleaned.csv"
    
    with duckdb.get_connection() as conn:
        # Export to CSV with pipe delimiter |
        conn.execute(f"""
            COPY (SELECT * FROM refined.ref_victimas_cleaned) 
            TO '{OUTPUT_FILE}' 
            (HEADER, DELIMITER '|')
        """)
        
        total = conn.execute("SELECT COUNT(*) FROM refined.ref_victimas_cleaned").fetchone()[0]
    
    context.log.info(f"CSV exported successfully: {total:,} rows")
    
    return dg.MaterializeResult(
        metadata={
            "rows_exported": dg.MetadataValue.int(total),
            "output_file": dg.MetadataValue.path(str(OUTPUT_FILE)),
            "delimiter": dg.MetadataValue.text("|"),
        }
    )


@dg.asset(
    deps=[dbt_project_assets]
)
def load_to_postgres(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """Load cleaned data from DuckDB to PostgreSQL"""
    context.log.info("Loading data to PostgreSQL...")
    
    # Load credentials from environment variables (.env)
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    dbname = os.getenv('POSTGRES_DB')
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    
    if not all([host, port, dbname, user, password]):
        context.log.error("Missing PostgreSQL credentials in .env file")
        raise ValueError("PostgreSQL credentials not configured")
    
    with duckdb.get_connection() as conn:
        # Install and load PostgreSQL extension
        context.log.info("Installing PostgreSQL extension...")
        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")
        
        postgres_conn = f"host={host} port={port} dbname={dbname} user={user} password={password}"
        
        context.log.info(f"Connecting to PostgreSQL: {host}:{port}/{dbname}")
        conn.execute(f"ATTACH '{postgres_conn}' AS postgres_db (TYPE postgres)")
        
        # Create table in PostgreSQL in the public schema
        context.log.info("Creating table in PostgreSQL...")
        conn.execute("""
            CREATE OR REPLACE TABLE postgres_db.public.ref_victimas_cleaned AS 
            SELECT * FROM refined.ref_victimas_cleaned
        """)
        
        total = conn.execute("SELECT COUNT(*) FROM postgres_db.public.ref_victimas_cleaned").fetchone()[0]
    
    context.log.info(f"Data loaded successfully to PostgreSQL: {total:,} rows")
    
    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(total),
            "postgres_host": dg.MetadataValue.text(host),
            "postgres_database": dg.MetadataValue.text(dbname),
            "postgres_table": dg.MetadataValue.text("public.ref_victimas_cleaned"),
        }
    )


