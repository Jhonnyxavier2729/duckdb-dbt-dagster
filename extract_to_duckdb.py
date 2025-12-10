"""
Data extraction script using native DuckDB
Reads CSV file with special delimiter and loads to DuckDB
"""
import duckdb
import time
from pathlib import Path

# Configuration
INPUT_FILE = 'data/raw/entrada.txt'
DATABASE_FILE = 'data/duckdb/ads.duckdb'

def load_csv_to_duckdb():
    """Load CSV file with latin-1 encoding to DuckDB"""
    print("Loading CSV to DuckDB...")
    
    start_time = time.time()
    
    # Connect to dbt database
    con = duckdb.connect(DATABASE_FILE)
    
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
        
        print("Data loaded successfully:")
        print(f"   Records: {count:,}")
        print(f"   Columns: {columns}")
        print(f"   Database: {DATABASE_FILE}")
        print(f"   Table: raw.victimas")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        con.close()
    
    elapsed = time.time() - start_time
    print(f"Total time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    load_csv_to_duckdb()
