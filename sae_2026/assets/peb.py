import json
import os
import tempfile

import dagster
import requests
from dagster_duckdb import DuckDBResource

# data.gouv.fr — zone polygon files (marked obsolete but still live)
# Zone A is the most restrictive and is only defined for the largest airports;
# it is not published as a separate national file.
PEB_ZONE_URLS = {
    "B": "https://www.data.gouv.fr/api/1/datasets/r/ea77a7b5-0298-49ed-b3ff-caae3b15d022",
    "C": "https://www.data.gouv.fr/api/1/datasets/r/a7f30166-3319-428e-a08e-700e3c0a3755",
    "D": "https://www.data.gouv.fr/api/1/datasets/r/78087339-b725-4825-a9f7-8d4ef92b2963",
}


@dagster.asset(
    group_name="ingest",
    kinds=["python", "duckdb"],
    description=(
        "PEB (Plan d'Exposition au Bruit) noise zone polygons (zones B, C, D) "
        "around French airports. Source: DGAC via data.gouv.fr."
    ),
    op_tags={"dagster/concurrency_key": "duckdb"},
)
def raw_peb(
    context: dagster.AssetExecutionContext,
    database: DuckDBResource,
) -> dagster.MaterializeResult:
    temp_files = []

    try:
        # Download each zone file and tag features with their zone letter
        all_features = []
        for zone, url in PEB_ZONE_URLS.items():
            context.log.info(f"Fetching zone {zone} from {url} ...")
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            features = response.json().get("features", [])
            for feature in features:
                feature.setdefault("properties", {})["peb_zone"] = zone
            all_features.extend(features)
            context.log.info(f"Zone {zone}: {len(features)} polygons")

        # Write merged GeoJSON to a temp file for DuckDB ST_Read
        with tempfile.NamedTemporaryFile(suffix=".geojson", mode="w", delete=False) as f:
            json.dump({"type": "FeatureCollection", "features": all_features}, f)
            temp_path = f.name
            temp_files.append(temp_path)

        with database.get_connection() as conn:
            conn.execute("INSTALL spatial")
            conn.execute("LOAD spatial")

            conn.execute("DROP TABLE IF EXISTS raw_peb")
            conn.execute(f"CREATE TABLE raw_peb AS SELECT * FROM ST_Read('{temp_path}')")

            row_count = conn.execute("SELECT COUNT(*) FROM raw_peb").fetchone()[0]
            counts_by_zone = conn.execute(
                "SELECT peb_zone, COUNT(*) FROM raw_peb GROUP BY peb_zone ORDER BY peb_zone"
            ).fetchall()

    finally:
        for path in temp_files:
            os.unlink(path)

    return dagster.MaterializeResult(
        metadata={
            "row_count": dagster.MetadataValue.int(row_count),
            **{f"zone_{zone}": dagster.MetadataValue.int(count) for zone, count in counts_by_zone},
        }
    )
