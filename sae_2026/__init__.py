from pathlib import Path

import dagster
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets
from dagster_duckdb import DuckDBResource

from .assets import ban_geocoding, raw_dpe, raw_dvf, raw_peb

DBT_PROJECT_DIR = Path(__file__).parent.parent / "transform"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)


class RawSourceTranslator(DagsterDbtTranslator):
    """Maps dbt sources (raw.*) to the corresponding Python ingest asset keys,
    and puts all dbt assets in the same group as the Python assets."""

    def get_asset_key(self, dbt_resource_props: dict) -> dagster.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return dagster.AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: dict) -> str:
        return "ingest"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=RawSourceTranslator(),
)
def transform_dbt_assets(context: dagster.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


defs = dagster.Definitions(
    assets=[ban_geocoding, raw_dpe, raw_dvf, raw_peb, transform_dbt_assets],
    resources={
        "database": DuckDBResource(database="dvf.duckdb"),
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
        ),
    },
)
