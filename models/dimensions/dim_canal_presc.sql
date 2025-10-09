{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_canal_presc'
) }}

WITH source_canal_prescr AS (
    SELECT
        cf_2801id AS canal_presc_id,                
        cf_2801 AS nom_canal_presc,               
        color AS code_color,             
        presence AS presence, 
        sortorderid AS ordre_tri,              
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_2801') }}
),

-- 1️⃣ Nettoyage des doublons
-- Si plusieurs lignes ont le même identifiant canal_presc_id, on garde la plus récente
dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY canal_presc_id ORDER BY date_integration DESC) AS rn
    FROM source_canal_prescr
),
filtered_dedup AS (
    SELECT *
    FROM dedup
    WHERE rn = 1
),

-- 2️⃣ Traitement des valeurs manquantes
cleaned AS (
    SELECT
        -- Remplacer les ID manquants par une valeur par défaut (ou ignorer)
        COALESCE(canal_presc_id, CONCAT('UNK_', ROW_NUMBER() OVER (ORDER BY nom_canal_presc))) AS canal_presc_id,

        -- Nettoyage du nom du canal
        CASE 
            WHEN nom_canal_presc IS NULL OR TRIM(nom_canal_presc) = '' THEN 'Inconnu'
            ELSE INITCAP(TRIM(nom_canal_presc))
        END AS nom_canal_presc,

        -- Nettoyage du code couleur
        CASE 
            WHEN code_color IS NULL OR TRIM(code_color) = '' THEN '#FFFFFF'
            ELSE UPPER(TRIM(code_color))
        END AS code_color,

        -- Normalisation de la présence
        CASE 
            WHEN LOWER(TRIM(presence)) IN ('oui', 'yes', 'true', '1') THEN 'Oui'
            WHEN LOWER(TRIM(presence)) IN ('non', 'no', 'false', '0') THEN 'Non'
            ELSE 'Inconnu'
        END AS presence,

        -- Valeur par défaut pour ordre_tri si manquant
        COALESCE(ordre_tri, 9999) AS ordre_tri,

        date_integration,
        source_system
    FROM filtered_dedup
)

SELECT *
FROM cleaned
