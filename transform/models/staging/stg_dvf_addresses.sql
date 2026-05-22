-- Unique addresses from DVF, normalized for BAN geocoding.
-- This is the input to the ban_geocoding Dagster asset.
SELECT DISTINCT
    adresse_numero,
    adresse_nom_voie,
    code_commune,
    code_departement
FROM {{ source('raw', 'raw_dvf') }}
WHERE adresse_nom_voie IS NOT NULL
