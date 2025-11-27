from pathlib import Path
from dagster_dbt import DbtCliResource

BASE_PATH = Path(__file__).parent.parent

dbt_resource = DbtCliResource(
    project_dir=str(BASE_PATH / "dbt_code"),
    profiles_dir=str(Path.home() / ".dbt"),
)
