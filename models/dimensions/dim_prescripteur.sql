{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_prescripteur'
) }}

-- Table brute Airbyte (normalisée automatiquement)
WITH source_account AS (
    SELECT
        accountid,
        "account_name",
        "website",
        "phone",
        "account_no"
    FROM {{ ref('stg_vtiger_account') }}
),

-- Table de staging (traitée via dbt)
source_accountscf AS (
    SELECT *
    FROM {{ ref('stg_vtiger_accountscf') }}
),

-- Jointure logique entre les deux tables
joined AS (
    SELECT
        a.accountid AS prescripteur_id,
        a."account_name" AS nom_prescripteur,
        a."website" AS site_web,
        a."phone" AS telephone_principal,
        a."account_no" AS assigne_a,

        -- Champs personnalisés depuis ACCOUNTSCF
        cf."succursale" AS city,                 
        cf."ca_annuel" AS ca_annuel,             
        cf."modif_par" AS date_dernier_patient,  

        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM source_account a
    LEFT JOIN source_accountscf cf
        ON a.accountid = cf.accountid
)

SELECT *
FROM joined
