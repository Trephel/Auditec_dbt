{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_centre'
) }}

WITH source_centre AS (
    SELECT
        CAST(succursalesid AS VARCHAR) AS succursales_id,                
        cf_2009 AS nom_succursales,               
        cf_2011 AS region_succursales,                           
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_succursalescf') }}
),

-- 1️⃣ Déduplication : garde une seule ligne par succursale (la plus récente)
dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY succursales_id ORDER BY date_integration DESC) AS rn
    FROM source_centre
),
filtered_dedup AS (
    SELECT *
    FROM dedup
    WHERE rn = 1
),

-- 2️⃣ Nettoyage et normalisation
cleaned AS (
    SELECT
        -- ID : s’il manque, on génère un identifiant temporaire
        COALESCE(succursales_id, CONCAT('UNK_', ROW_NUMBER() OVER (ORDER BY nom_succursales))) AS succursales_id,

        -- Nom de succursale : suppression des espaces inutiles et normalisation
        CASE 
            WHEN nom_succursales IS NULL OR TRIM(nom_succursales) = '' THEN 'Succursale Inconnue'
            ELSE INITCAP(TRIM(nom_succursales))
        END AS nom_succursales,

        -- Région : nettoyage et remplacement des valeurs manquantes
        CASE 
            WHEN region_succursales IS NULL OR TRIM(region_succursales) = '' THEN 'Region Inconnue'
            ELSE INITCAP(TRIM(region_succursales))
        END AS region_succursales,

        -- Métadonnées
        date_integration,
        source_system
    FROM filtered_dedup
)

-- 3️⃣ Résultat final
SELECT *
FROM cleaned
