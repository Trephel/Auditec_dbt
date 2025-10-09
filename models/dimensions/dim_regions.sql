{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_region'
) }}

-- 1️⃣ Source de données brute
WITH source_region AS (
    SELECT
        cf_1977id AS region_id,                 
        cf_1977 AS nom_region,               
        color AS code_region,            
        presence AS presence, 
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_1977') }}
),

-- 2️⃣ Suppression des doublons
dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY region_id ORDER BY date_integration DESC) AS rn
    FROM source_region
),
filtered AS (
    SELECT *
    FROM dedup
    WHERE rn = 1
),

-- 3️⃣ Nettoyage et standardisation
cleaned AS (
    SELECT
        -- ✅ Clé unique avec fallback
        COALESCE(region_id, CONCAT('UNK_', ROW_NUMBER() OVER (ORDER BY nom_region))) AS region_id,

        -- ✅ Nom de région nettoyé
        CASE 
            WHEN nom_region IS NULL OR TRIM(nom_region) = '' THEN 'Région Inconnue'
            ELSE INITCAP(TRIM(nom_region))
        END AS nom_region,

        -- ✅ Code couleur (supprime les caractères spéciaux inutiles)
        CASE 
            WHEN code_region IS NULL OR TRIM(code_region) = '' THEN 'Non Défini'
            ELSE UPPER(TRIM(REPLACE(code_region, '#', '')))
        END AS code_region,

        -- ✅ Présence (statut normalisé)
        CASE 
            WHEN LOWER(TRIM(presence)) IN ('oui', 'yes', 'true', '1') THEN 'Présente'
            WHEN LOWER(TRIM(presence)) IN ('non', 'no', 'false', '0') THEN 'Absente'
            ELSE 'Inconnue'
        END AS presence,

        -- ✅ Métadonnées
        date_integration,
        source_system
    FROM filtered
)

-- 4️⃣ Résultat final
SELECT *
FROM cleaned
