{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_prescripteur'
) }}

-- 1️⃣ Source : table des comptes
WITH source_account AS (
    SELECT
        accountid,
        "account_name",
        "website",
        "phone",
        "account_no"
    FROM {{ ref('stg_vtiger_account') }}
),

-- 2️⃣ Source complémentaire : table custom fields (accountscf)
source_accountscf AS (
    SELECT *
    FROM {{ ref('stg_vtiger_accountscf') }}
),

-- 3️⃣ Jointure entre les deux
joined AS (
    SELECT
        a.accountid AS prescripteur_id,
        a."account_name" AS nom_prescripteur,
        a."website" AS site_web,
        a."phone" AS telephone_principal,
        a."account_no" AS assigne_a,

        cf."succursale" AS succursale,                 
        cf."ca_annuel" AS ca_annuel,             
        cf."modif_par" AS modifie_par,  

        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM source_account a
    LEFT JOIN source_accountscf cf
        ON a.accountid = cf.accountid
),

-- 4️⃣ Suppression des doublons sur la clé naturelle
dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY prescripteur_id ORDER BY date_integration DESC) AS rn
    FROM joined
),
filtered AS (
    SELECT *
    FROM dedup
    WHERE rn = 1
),

-- 5️⃣ Nettoyage et standardisation des données
cleaned AS (
    SELECT
        -- ID (fallback si manquant)
        COALESCE(prescripteur_id, CONCAT('UNK_', ROW_NUMBER() OVER (ORDER BY nom_prescripteur))) AS prescripteur_id,

        -- Nom du prescripteur
        CASE 
            WHEN nom_prescripteur IS NULL OR TRIM(nom_prescripteur) = '' THEN 'Prescripteur Inconnu'
            ELSE INITCAP(TRIM(nom_prescripteur))
        END AS nom_prescripteur,

        -- Téléphone
        CASE 
            WHEN telephone_principal IS NULL OR TRIM(telephone_principal) = '' THEN 'Non Renseigné'
            ELSE TRIM(telephone_principal)
        END AS telephone_principal,

        -- Site web
        CASE 
            WHEN site_web IS NULL OR TRIM(site_web) = '' THEN 'Non Disponible'
            ELSE LOWER(TRIM(site_web))
        END AS site_web,

        -- Succursale
        CASE 
            WHEN succursale IS NULL OR TRIM(succursale) = '' THEN 'Inconnue'
            ELSE INITCAP(TRIM(succursale))
        END AS succursale,

        -- Chiffre d’affaires annuel
        COALESCE(NULLIF(TRIM(ca_annuel), ''), 'Non Spécifié') AS ca_annuel,

        -- Dernier modificateur
        CASE 
            WHEN modifie_par IS NULL OR TRIM(modifie_par) = '' THEN 'Inconnu'
            ELSE INITCAP(TRIM(modifie_par))
        END AS modifie_par,

        -- Assigné à
        COALESCE(assigne_a, 'N/A') AS assigne_a,

        -- Métadonnées
        date_integration,
        source_system
    FROM filtered
)

-- 6️⃣ Résultat final
SELECT *
FROM cleaned
