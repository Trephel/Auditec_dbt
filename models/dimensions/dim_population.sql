{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_population'
) }}

-- 1️⃣ Table  CRC
WITH audio AS (
    SELECT
        objectifcrcid AS pop_id,
        cf_2089 AS libelle,
        'CRC' AS type_population,
        cf_2089 AS annee,
        --cf_2738 AS date_integration,
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_objectifcrccf') }}
),

-- 2️⃣ Table  AUDIO
crc AS (
    SELECT
        objectifaudioid AS pop_id,
        cf_1621 AS libelle,
        'Audio' AS type_population,
        cf_1569 AS annee,
         --cf_2734 AS date_integration,
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_objectifaudiocf') }}
),

-- 3️⃣ Table VM
vm AS (
    SELECT
        objectifvmid AS pop_id,
        cf_2196 AS libelle,
        'VM' AS type_population,
        cf_2200 AS annee,
        --cf_2740 AS date_integration,
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_objectifvmcf') }}
),


-- 5️⃣ Union de toutes les populations
union_all AS (
    SELECT * FROM audio
    UNION ALL
    SELECT * FROM crc
    UNION ALL
    SELECT * FROM vm

),

-- 6️⃣ Élimination des doublons éventuels
final AS (
    SELECT DISTINCT
        pop_id,
        libelle AS nom_population,
        type_population,
        annee,
        --date_integration,
        source_system
    FROM union_all
)

-- 7️⃣ Résultat final
SELECT *
FROM final
