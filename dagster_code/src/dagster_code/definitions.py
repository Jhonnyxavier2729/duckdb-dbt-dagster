from dagster import Definitions, load_assets_from_modules
from .defs import assets, resources, schedules

# Cargar todos los assets
all_assets = load_assets_from_modules([assets])

# Definir el proyecto
defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": resources.database_resource,
        "dbt": resources.dbt_resource,
    },
    schedules=[schedules.pipeline_schedule],
)
