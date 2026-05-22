import dagster
from dagster_duckdb import DuckDBResource

from .assets import raw_dvf, raw_peb

defs = dagster.Definitions(
    assets=[raw_dvf, raw_peb],
    resources={
        "database": DuckDBResource(database="dvf.duckdb"),
    },
)
