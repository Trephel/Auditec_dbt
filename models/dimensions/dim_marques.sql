{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_marques'
) }}

-- 1️⃣ Extraction des données sources
WITH source_marque AS (
    SELECT
        cf_2647id AS marque_id,                
        cf_2647 AS nom_marque,               
        color AS code_color,             
        presence AS presence, 
        sortorderid AS ordre_tri,              
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_2647') }}
),

-- 2️⃣ Déduplication : garde la dernière ligne par marque_id
dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY marque_id ORDER BY date_integration DESC) AS rn
    FROM source_marque
),
filtered_dedup AS (
    SELECT *
    FROM dedup
    WHERE rn = 1
),

-- 3️⃣ Nettoyage et standardisation
cleaned AS (
    SELECT
        -- ID de la marque (fallback si null)
        COALESCE(marque_id, CONCAT('UNK_', ROW_NUMBER() OVER (ORDER BY nom_marque))) AS marque_id,

        -- Nom de marque nettoyé et standardisé
        CASE 
            WHEN nom_marque IS NULL OR TRIM(nom_marque) = '' THEN 'Marque Inconnue'
            ELSE INITCAP(TRIM(nom_marque))
        END AS nom_marque,

        -- Couleur nettoyée
        CASE 
            WHEN code_color IS NULL OR TRIM(code_color) = '' THEN 'Non Défini'
            ELSE TRIM(code_color)
        END AS code_color,

        -- Présence
        CASE 
            WHEN presence IS NULL OR TRIM(presence) = '' THEN 'Inconnue'
            ELSE INITCAP(TRIM(presence))
        END AS presence,

        -- Ordre de tri
        COALESCE(ordre_tri, 9999) AS ordre_tri,

        -- Métadonnées
        date_integration,
        source_system
    FROM filtered_dedup
)

-- 4️⃣ Résultat final
SELECT *
FROM cleaned
