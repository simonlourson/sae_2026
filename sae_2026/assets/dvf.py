import dagster
from dagster_duckdb import DuckDBResource

YEARS = ["2022", "2023", "2024", "2025"]
DEPARTMENTS = ["44", "29", "22", "56", "35"]

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"

dvf_partitions = dagster.MultiPartitionsDefinition({
    "year": dagster.StaticPartitionsDefinition(YEARS),
    "department": dagster.StaticPartitionsDefinition(DEPARTMENTS),
})


@dagster.asset(
    partitions_def=dvf_partitions,
    group_name="ingest",
    kinds=["python", "duckdb"],
    description="Raw DVF transactions, geo-enriched by Etalab (lat/lng included).",
    op_tags={"dagster/concurrency_key": "duckdb"},
)
def raw_dvf(
    context: dagster.AssetExecutionContext,
    database: DuckDBResource,
) -> dagster.MaterializeResult:
    keys = context.partition_key.keys_by_dimension
    year = keys["year"]
    dept = keys["department"]
    url = f"{BASE_URL}/{year}/departements/{dept}.csv.gz"

    with database.get_connection() as conn:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_dvf (
                id_mutation                  VARCHAR,
                date_mutation                DATE,
                numero_disposition           INTEGER,
                nature_mutation              VARCHAR,
                valeur_fonciere              DOUBLE,
                adresse_numero               VARCHAR,
                adresse_suffixe              VARCHAR,
                adresse_nom_voie             VARCHAR,
                adresse_code_voie            VARCHAR,
                code_postal                  VARCHAR,
                code_commune                 VARCHAR,
                nom_commune                  VARCHAR,
                code_departement             VARCHAR,
                ancien_code_commune          VARCHAR,
                ancien_nom_commune           VARCHAR,
                id_parcelle                  VARCHAR,
                ancien_id_parcelle           VARCHAR,
                numero_volume                VARCHAR,
                lot1_numero                  VARCHAR,
                lot1_surface_carrez          DOUBLE,
                lot2_numero                  VARCHAR,
                lot2_surface_carrez          DOUBLE,
                lot3_numero                  VARCHAR,
                lot3_surface_carrez          DOUBLE,
                lot4_numero                  VARCHAR,
                lot4_surface_carrez          DOUBLE,
                lot5_numero                  VARCHAR,
                lot5_surface_carrez          DOUBLE,
                nombre_lots                  INTEGER,
                code_type_local              VARCHAR,
                type_local                   VARCHAR,
                surface_reelle_bati          DOUBLE,
                nombre_pieces_principales    INTEGER,
                code_nature_culture          VARCHAR,
                nature_culture               VARCHAR,
                code_nature_culture_speciale VARCHAR,
                nature_culture_speciale      VARCHAR,
                surface_terrain              DOUBLE,
                longitude                    DOUBLE,
                latitude                     DOUBLE
            )
        """)

        # Delete existing rows for this partition before inserting (idempotency)
        conn.execute(
            "DELETE FROM raw_dvf WHERE code_departement = ? AND year(date_mutation) = ?",
            [dept, int(year)],
        )

        conn.execute(f"""
            INSERT INTO raw_dvf
            SELECT * FROM read_csv(
                '{url}',
                compression = 'gzip',
                header = true,
                dateformat = '%Y-%m-%d',
                nullstr = '',
                quote = '"'
            )
        """)

        row_count = conn.execute(
            "SELECT COUNT(*) FROM raw_dvf WHERE code_departement = ? AND year(date_mutation) = ?",
            [dept, int(year)],
        ).fetchone()[0]

    return dagster.MaterializeResult(
        metadata={"row_count": dagster.MetadataValue.int(row_count)}
    )
