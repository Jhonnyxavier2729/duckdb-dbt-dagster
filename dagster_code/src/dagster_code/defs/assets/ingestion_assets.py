"""Ingestion assets - Load raw data into DuckDB"""

import dagster as dg
from dagster_duckdb import DuckDBResource
from pathlib import Path


# Define paths
BASE_DIR = Path(__file__).parent.parent.parent.parent.parent
INPUT_FILE = str(BASE_DIR / "data" / "raw" / "entrada.txt")
DATABASE_FILE = str(BASE_DIR / "data" / "duckdb" / "ads.duckdb")


@dg.asset(
    group_name="ingestion"
)
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
