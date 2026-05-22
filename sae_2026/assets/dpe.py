import time

import dagster
import requests
from dagster_duckdb import DuckDBResource

# DPE v2 — existing buildings, since July 2021
# https://data.ademe.fr/datasets/dpe-v2-logements-existants
DATASET_ID = "meg-83tjwtg8dyz4vv7h1dqe"
BASE_URL = f"https://data.ademe.fr/data-fair/api/v1/datasets/{DATASET_ID}/lines"
PAGE_SIZE = 10_000

DEPARTMENTS = ["44", "29", "22", "56", "35"]

# Columns to keep — full schema has 200+ fields; these cover the join keys and
# the analytical dimensions relevant to the project.
# Two columns with spaces in their original CSV names are excluded:
# "conso_5 usages_par_m2_ef" and "emission_ges_5_usages par_m2" — the API
# rejects them in the select parameter.
COLUMNS = [
    "numero_dpe",
    "date_etablissement_dpe",
    "date_fin_validite_dpe",
    "type_batiment",
    "periode_construction",
    "adresse_ban",
    "adresse_brut",
    "code_postal_ban",
    "code_insee_ban",
    "code_departement_ban",
    "nom_commune_ban",
    "identifiant_ban",
    "coordonnee_cartographique_x_ban",
    "coordonnee_cartographique_y_ban",
    "etiquette_dpe",
    "etiquette_ges",
    "conso_5_usages_par_m2_ep",
    "surface_habitable_immeuble",
    "type_energie_principale_chauffage",
    "type_energie_principale_ecs",
]

SELECT_PARAM = ",".join(COLUMNS)

dpe_partitions = dagster.StaticPartitionsDefinition(DEPARTMENTS)


@dagster.asset(
    partitions_def=dpe_partitions,
    group_name="ingest",
    kinds=["python", "duckdb"],
    description=(
        "DPE (Diagnostic de Performance Énergétique) for existing buildings, "
        "since July 2021. Source: ADEME via data.ademe.fr."
    ),
    op_tags={"dagster/concurrency_key": "duckdb"},
)
def raw_dpe(
    context: dagster.AssetExecutionContext,
    database: DuckDBResource,
) -> dagster.MaterializeResult:
    dept = context.partition_key

    with database.get_connection() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS raw_dpe (
                numero_dpe                      VARCHAR,
                date_etablissement_dpe          DATE,
                date_fin_validite_dpe           DATE,
                type_batiment                   VARCHAR,
                periode_construction            VARCHAR,
                adresse_ban                     VARCHAR,
                adresse_brut                    VARCHAR,
                code_postal_ban                 VARCHAR,
                code_insee_ban                  VARCHAR,
                code_departement_ban            VARCHAR,
                nom_commune_ban                 VARCHAR,
                identifiant_ban                 VARCHAR,
                coordonnee_cartographique_x_ban DOUBLE,
                coordonnee_cartographique_y_ban DOUBLE,
                etiquette_dpe                   VARCHAR,
                etiquette_ges                   VARCHAR,
                conso_5_usages_par_m2_ep        DOUBLE,
                surface_habitable_immeuble      DOUBLE,
                type_energie_principale_chauffage VARCHAR,
                type_energie_principale_ecs     VARCHAR
            )
        """)

        conn.execute(
            "DELETE FROM raw_dpe WHERE code_departement_ban = ?",
            [dept],
        )

        # Cursor-based JSON pagination with select= to reduce payload ~10x
        url = f"{BASE_URL}?size={PAGE_SIZE}&qs=code_departement_ban%3A{dept}&select={SELECT_PARAM}"
        total_inserted = 0
        page = 0
        t0 = time.perf_counter()

        while url:
            t_page = time.perf_counter()
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            data = response.json()
            t_network = time.perf_counter() - t_page

            rows = [
                tuple(r.get(col) for col in COLUMNS)
                for r in data.get("results", [])
            ]

            if rows:
                t_ins = time.perf_counter()
                conn.executemany(
                    f"INSERT INTO raw_dpe VALUES ({','.join(['?'] * len(COLUMNS))})",
                    rows,
                )
                t_insert = time.perf_counter() - t_ins
                total_inserted += len(rows)
                context.log.info(
                    f"Dept {dept} — page {page}: {len(rows)} rows | "
                    f"network {t_network:.1f}s | insert {t_insert:.1f}s | "
                    f"total {time.perf_counter() - t0:.0f}s"
                )

            url = data.get("next")
            page += 1

    context.log.info(f"Dept {dept} — done: {total_inserted} rows")

    return dagster.MaterializeResult(
        metadata={"row_count": dagster.MetadataValue.int(total_inserted)}
    )
