"""Assets module for the pipeline"""

from .ingestion_assets import load_csv_to_duckdb
from .dbt_assets import dbt_project_assets
from .reporting_assets import generate_excel_report

__all__ = [
    "load_csv_to_duckdb",
    "dbt_project_assets",
    "generate_excel_report",
]
