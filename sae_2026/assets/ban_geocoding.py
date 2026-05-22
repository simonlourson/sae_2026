import csv
import io
import time

import dagster
import requests
from dagster_duckdb import DuckDBResource

BAN_URL = "https://api-adresse.data.gouv.fr/search/csv/"
BATCH_SIZE = 10_000

DEPARTMENTS = ["44", "29", "22", "56", "35"]

ban_partitions = dagster.StaticPartitionsDefinition(DEPARTMENTS)


@dagster.asset(
    partitions_def=ban_partitions,
    deps=["stg_dvf_addresses"],
    group_name="ingest",
    kinds=["python", "duckdb"],
    description=(
        "BAN geocoding lookup table: maps each unique DVF address to its "
        "identifiant_ban so DVF transactions can be joined with DPE records."
    ),
    op_tags={"dagster/concurrency_key": "duckdb"},
)
def ban_geocoding(
    context: dagster.AssetExecutionContext,
    database: DuckDBResource,
) -> dagster.MaterializeResult:
    dept = context.partition_key

    with database.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ban_geocoding (
                adresse_numero   VARCHAR,
                adresse_nom_voie VARCHAR,
                code_commune     VARCHAR,
                identifiant_ban  VARCHAR,
                result_score     DOUBLE,
                result_type      VARCHAR,
                result_label     VARCHAR,
                longitude        DOUBLE,
                latitude         DOUBLE
            )
        """)

        conn.execute(
            "DELETE FROM ban_geocoding WHERE LEFT(code_commune, ?) = ?",
            [len(dept), dept],
        )

        addresses = conn.execute("""
            SELECT adresse_numero, adresse_nom_voie, code_commune
            FROM stg_dvf_addresses
            WHERE code_departement = ?
        """, [dept]).fetchall()

    total = len(addresses)
    context.log.info(f"Dept {dept}: {total} unique addresses to geocode")

    inserted = 0
    t0 = time.perf_counter()

    for batch_num, offset in enumerate(range(0, total, BATCH_SIZE), start=1):
        batch = addresses[offset: offset + BATCH_SIZE]

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["adresse_numero", "adresse_nom_voie", "code_commune"])
        for adresse_numero, adresse_nom_voie, code_commune in batch:
            writer.writerow([adresse_numero or "", adresse_nom_voie, code_commune])

        t_req = time.perf_counter()
        response = requests.post(
            BAN_URL,
            files={"data": ("addresses.csv", buf.getvalue().encode("utf-8"), "text/csv")},
            data=[
                ("columns", "adresse_numero"),
                ("columns", "adresse_nom_voie"),
                ("citycode", "code_commune"),
            ],
            timeout=300,
        )
        response.raise_for_status()
        t_network = time.perf_counter() - t_req

        rows = []
        reader = csv.DictReader(io.StringIO(response.text))
        for row in reader:
            rows.append((
                row.get("adresse_numero") or None,
                row.get("adresse_nom_voie") or None,
                row.get("code_commune") or None,
                row.get("result_id") or None,
                float(row["result_score"]) if row.get("result_score") else None,
                row.get("result_type") or None,
                row.get("result_label") or None,
                float(row["longitude"]) if row.get("longitude") else None,
                float(row["latitude"]) if row.get("latitude") else None,
            ))

        with database.get_connection() as conn:
            conn.executemany(
                "INSERT INTO ban_geocoding VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

        inserted += len(rows)
        context.log.info(
            f"Batch {batch_num}/{-(-total // BATCH_SIZE)}: {len(rows)} rows | "
            f"network {t_network:.1f}s | total elapsed {time.perf_counter() - t0:.0f}s"
        )

    return dagster.MaterializeResult(
        metadata={"row_count": dagster.MetadataValue.int(inserted)}
    )
