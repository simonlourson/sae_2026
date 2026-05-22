import dagster
from dagster_duckdb import DuckDBResource

from .assets import raw_dvf

defs = dagster.Definitions(
    assets=[raw_dvf],
    resources={
        "database": DuckDBResource(database="dvf.duckdb"),
    },
)
