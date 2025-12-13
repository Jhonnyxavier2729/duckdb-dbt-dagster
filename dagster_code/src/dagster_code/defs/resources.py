from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource
from pathlib import Path
import dagster as dg

# Ruta a la base de datos
BASE_DIR = Path(__file__).parent.parent.parent.parent
DATABASE_PATH = BASE_DIR / "data" / "duckdb" / "ads.duckdb"
DBT_PROJECT_PATH = BASE_DIR / "dbt_code"
DBT_PROFILES_PATH = DBT_PROJECT_PATH / "profiles.yml"

# Recurso de DuckDB
database_resource = DuckDBResource(database=str(DATABASE_PATH))

# Recurso de DBT
dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_PATH),
    profiles_dir=str(DBT_PROJECT_PATH),
)


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
            "dbt": dbt_resource,
        }
    )