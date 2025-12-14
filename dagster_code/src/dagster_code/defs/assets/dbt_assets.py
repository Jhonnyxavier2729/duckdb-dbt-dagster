"""DBT assets - Transform data using DBT models"""

import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from pathlib import Path
from .ingestion_assets import load_csv_to_duckdb


# Define paths
BASE_DIR = Path(__file__).parent.parent.parent.parent.parent
DBT_PROJECT_PATH = BASE_DIR / "dbt_code"
DBT_MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"


# Custom translator to map DBT sources to Dagster assets
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        # For sources, map raw.victimas to load_csv_to_duckdb asset
        if dbt_resource_props["resource_type"] == "source":
            source_name = dbt_resource_props["source_name"]
            name = dbt_resource_props["name"]
            if source_name == "raw" and name == "victimas":
                return dg.AssetKey(["load_csv_to_duckdb"])
        
        # For other assets, use default behavior
        return super().get_asset_key(dbt_resource_props)
    
    def get_group_name(self, dbt_resource_props):
        # Assign groups based on DBT model type
        resource_type = dbt_resource_props.get("resource_type")
        fqn = dbt_resource_props.get("fqn", [])
        
        # fqn contains model path, e.g.: ['dbt_code', 'staging', 'victimas_data', 'stg_victimas_start']
        dbt_models = ["staging", "intermediate", "refined"]
        if len(fqn) > 1:
            folder = fqn[1]  # staging, intermediate, refined, marts
            if folder in dbt_models:
                return "dbt_models"
            elif folder == "marts":
                return "dbt_marts"
        
        # For other cases, use default behavior
        return super().get_group_name(dbt_resource_props)


# DBT Assets - loaded dynamically if manifest exists
if DBT_MANIFEST_PATH.exists():
    @dbt_assets(
        manifest=DBT_MANIFEST_PATH,
        dagster_dbt_translator=CustomDagsterDbtTranslator()
    )
    def dbt_project_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        """
        Runs all DBT models as Dagster assets.
        Dependencies: requires load_csv_to_duckdb to run first
        """
        yield from dbt.cli(["build"], context=context).stream()
else:
    # If manifest doesn't exist, create a placeholder
    @dg.asset(
        deps=[load_csv_to_duckdb]
    )
    def dbt_project_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        """
        Runs all DBT models.
        Note: First generate manifest with: python generate_dbt_manifest.py
        """
        context.log.info("Ejecutando modelos DBT...")
        result = dbt.cli(["build"], context=context)
        return result
