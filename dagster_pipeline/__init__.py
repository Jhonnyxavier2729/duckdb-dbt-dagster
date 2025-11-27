from dagster import Definitions
from .assets import extract_csv_to_duckdb, dbt_victimas_assets
from .resources import dbt_resource

defs = Definitions(
    assets=[extract_csv_to_duckdb, dbt_victimas_assets],
    resources={
        "dbt": dbt_resource,
    },
)
