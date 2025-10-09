{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_categorie'
) }}

WITH source_categorie AS (
    SELECT
        cf_1823id AS categorie_id,                
        cf_1823 AS nom_categorie,               
        color AS code_color,             
        presence AS presence, 
        sortorderid AS ordre_tri,              
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_1823') }}
),

-- 1️⃣ Déduplication : on garde la ligne la plus récente pour chaque catégorie
dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY categorie_id ORDER BY date_integration DESC) AS rn
    FROM source_categorie
),
filtered_dedup AS (
    SELECT *
    FROM dedup
    WHERE rn = 1
),

-- 2️⃣ Nettoyage et normalisation
cleaned AS (
    SELECT
        -- ID : si manquant, on génère un identifiant de substitution
        COALESCE(categorie_id, CONCAT('UNK_', ROW_NUMBER() OVER (ORDER BY nom_categorie))) AS categorie_id,

        -- Nom de catégorie : nettoyage, remplacement des valeurs vides par "Inconnue"
        CASE 
            WHEN nom_categorie IS NULL OR TRIM(nom_categorie) = '' THEN 'Inconnue'
            ELSE INITCAP(TRIM(nom_categorie))
        END AS nom_categorie,

        -- Code couleur : mise en majuscule et valeur par défaut (#FFFFFF)
        CASE 
            WHEN code_color IS NULL OR TRIM(code_color) = '' THEN '#FFFFFF'
            ELSE UPPER(TRIM(code_color))
        END AS code_color,

        -- Présence : standardisation des valeurs en "Oui", "Non", "Inconnue"
        CASE 
            WHEN LOWER(TRIM(presence)) IN ('oui', 'yes', 'true', '1') THEN 'Oui'
            WHEN LOWER(TRIM(presence)) IN ('non', 'no', 'false', '0') THEN 'Non'
            ELSE 'Inconnue'
        END AS presence,

        -- Ordre de tri : remplacement des valeurs manquantes
        COALESCE(ordre_tri, 9999) AS ordre_tri,

        -- Métadonnées
        date_integration,
        source_system
    FROM filtered_dedup
)

-- 3️⃣ Résultat final
SELECT *
FROM cleaned
