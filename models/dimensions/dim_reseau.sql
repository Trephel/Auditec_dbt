{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_reseau'
) }}

-- 1️⃣ Source de données brute
WITH source_reseau AS (
    SELECT
        CAST(cf_2777id AS VARCHAR) AS reseau_id,                
        cf_2777 AS nom_reseau,               
        color AS code_color,             
        presence AS presence, 
        sortorderid AS sortorderid,              
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_2777') }}
),

-- 2️⃣ Suppression des doublons
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY reseau_id ORDER BY date_integration DESC) AS rn
    FROM source_reseau
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
        COALESCE(reseau_id, CONCAT('UNK_', ROW_NUMBER() OVER (ORDER BY nom_reseau))) AS reseau_id,

        -- ✅ Nom réseau nettoyé
        CASE
            WHEN nom_reseau IS NULL OR TRIM(nom_reseau) = '' THEN 'Réseau Inconnu'
            ELSE INITCAP(TRIM(nom_reseau))
        END AS nom_reseau,

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
