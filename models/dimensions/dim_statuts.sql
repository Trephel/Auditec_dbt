{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_statut'
) }}

-- 1️⃣ Source brute
WITH source_statuts AS (
    SELECT
        cf_1361id AS statut_id,                
        cf_1361 AS nom_statut,               
        color AS code_color,             
        presence AS presence, 
        sortorderid AS sortorderid,              
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_1361') }}
),

-- 2️⃣ Suppression des doublons
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY statut_id ORDER BY date_integration DESC) AS rn
    FROM source_statuts
),
filtered AS (
    SELECT *
    FROM dedup
    WHERE rn = 1
),

-- 3️⃣ Nettoyage et standardisation
cleaned AS (
    SELECT
        -- ✅ Clé unique
        COALESCE(statut_id, CONCAT('UNK_', ROW_NUMBER() OVER (ORDER BY nom_statut))) AS statut_id,

        -- ✅ Nom statut nettoyé
        CASE
            WHEN nom_statut IS NULL OR TRIM(nom_statut) = '' THEN 'Statut Inconnu'
            ELSE INITCAP(TRIM(nom_statut))
        END AS nom_statut,

        -- ✅ Code couleur
        CASE
            WHEN code_color IS NULL OR TRIM(code_color) = '' THEN 'Non Défini'
            ELSE UPPER(TRIM(REPLACE(code_color, '#', '')))
        END AS code_color,

        -- ✅ Présence
        CASE
            WHEN LOWER(TRIM(presence)) IN ('oui', 'yes', 'true', '1') THEN 'Présent'
            WHEN LOWER(TRIM(presence)) IN ('non', 'no', 'false', '0') THEN 'Absent'
            ELSE 'Inconnu'
        END AS presence,

        sortorderid,
        date_integration,
        source_system
    FROM filtered
)

-- 4️⃣ Résultat final
SELECT *
FROM cleaned
