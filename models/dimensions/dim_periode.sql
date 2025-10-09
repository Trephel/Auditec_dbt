{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_periode'
) }}

WITH base AS (
    SELECT DISTINCT
        "date_vente" AS date_id,
        EXTRACT(DAY FROM "date_vente") AS jour,
        EXTRACT(MONTH FROM "date_vente") AS mois,
        EXTRACT(YEAR FROM "date_vente") AS annee,
        TO_CHAR("date_vente", 'YYYY-MM') AS annee_mois,
        TO_CHAR("date_vente", 'YYYY-"T"Q') AS trimestre,
        DATEADD(MONTH, -1, "date_vente") AS date_moins_1_mois,
        DATEADD(YEAR, -1, "date_vente") AS date_moins_1_an,
        DATE_TRUNC('MONTH', "date_vente") AS debut_mois,
        DATE_TRUNC('YEAR', "date_vente") AS debut_annee,
        CURRENT_TIMESTAMP() AS date_integration
    FROM {{ ref('stg_vtiger_ventescf') }}
)

SELECT *
FROM base
