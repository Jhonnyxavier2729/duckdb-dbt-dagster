import duckdb
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


def export_to_csv():
    """Export cleaned data to CSV with pipe delimiter"""
    print("Exporting data to CSV...")
    
    conn = duckdb.connect('data/duckdb/ads.duckdb', read_only=True)
    
    # Export to CSV with pipe delimiter |
    conn.execute("""
        COPY (SELECT * FROM refined.ref_victimas_cleaned) 
        TO 'data/output/victimas_cleaned.csv' 
        (HEADER, DELIMITER '|')
    """)
    
    total = conn.execute("SELECT COUNT(*) FROM refined.ref_victimas_cleaned").fetchone()[0]
    conn.close()
    
    print(f"CSV exported successfully: output/victimas_cleaned.csv ({total:,} rows)")


def load_to_postgres():
    """Load cleaned data from DuckDB to PostgreSQL"""
    print("\nLoading data to PostgreSQL...")
    
    conn = duckdb.connect('data/duckdb/ads.duckdb')
    
    # Install and load PostgreSQL extension
    conn.execute("INSTALL postgres")
    conn.execute("LOAD postgres")
    
    # Load credentials from environment variables (.env)
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    dbname = os.getenv('POSTGRES_DB')
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    
    postgres_conn = f"host={host} port={port} dbname={dbname} user={user} password={password}"
    
    conn.execute(f"ATTACH '{postgres_conn}' AS postgres_db (TYPE postgres)")
    
    # Create table in PostgreSQL in the public schema
    print("Creating table in PostgreSQL...")
    conn.execute("""
        CREATE OR REPLACE TABLE postgres_db.public.ref_victimas_cleaned AS 
        SELECT * FROM refined.ref_victimas_cleaned
    """)
    
    total = conn.execute("SELECT COUNT(*) FROM postgres_db.public.ref_victimas_cleaned").fetchone()[0]
    conn.close()
    
    print(f"Data loaded successfully to PostgreSQL: {total:,} rows")


def main():
    """Execute CSV export and PostgreSQL load"""
    print("=== Clean Data Export Pipeline ===\n")
    
    # 1. Export to CSV
    export_to_csv()
    
    # 2. Load to PostgreSQL
    load_to_postgres()
    
    print("\n=== Process completed successfully ===")


if __name__ == "__main__":
    main()
