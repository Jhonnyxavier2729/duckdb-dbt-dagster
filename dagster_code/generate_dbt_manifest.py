"""
Script para generar el manifest.json de DBT necesario para Dagster

Uso:
    cd dagster_code
    uv run python generate_dbt_manifest.py
"""
import os
from pathlib import Path

# Rutas
DBT_PROJECT_DIR = Path(__file__).parent / "dbt_code"
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

def generate_manifest():
    """Genera el manifest.json ejecutando dbt parse"""
    print(f"Generando manifest.json...")
    print(f"Directorio DBT: {DBT_PROJECT_DIR}")
    print(f"Manifest esperado en: {MANIFEST_PATH}")
    print()
    
    # Cambiar al directorio DBT
    os.chdir(str(DBT_PROJECT_DIR))
    
    print("Ejecuta el siguiente comando manualmente:")
    print(f"  cd {DBT_PROJECT_DIR}")
    print(f"  dbt parse --profiles-dir .")
    print()
    print("O si dbt no está disponible, usa:")
    print(f"  cd {DBT_PROJECT_DIR}")
    print(f"  uv run --with dbt-duckdb dbt parse --profiles-dir .")
    
if __name__ == "__main__":
    generate_manifest()
