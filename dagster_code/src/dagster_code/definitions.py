from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from .defs import assets, resources, schedules

# Import checks from tests folder
import sys
from pathlib import Path
tests_path = Path(__file__).parent.parent.parent / "tests"
sys.path.insert(0, str(tests_path))
from tests import asset_checks

# Load all assets
all_assets = load_assets_from_modules([assets])

# Load all asset checks
all_checks = load_asset_checks_from_modules([asset_checks])

# Define the project
defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        "duckdb": resources.database_resource,
        "dbt": resources.dbt_resource,
    },
    schedules=[schedules.pipeline_schedule],
)
