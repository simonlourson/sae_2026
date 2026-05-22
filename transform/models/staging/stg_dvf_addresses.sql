{{ config(materialized='table') }}

-- Unique addresses from DVF, normalized for BAN geocoding.
-- Materialized as a table (not a view) so the deduplication runs once
-- and the ban_geocoding Python asset reads a pre-built result.
-- This is the input to the ban_geocoding Dagster asset.
SELECT DISTINCT
    adresse_numero,
    adresse_nom_voie,
    code_commune,
    code_departement
FROM {{ source('raw', 'raw_dvf') }}
WHERE adresse_nom_voie IS NOT NULL
