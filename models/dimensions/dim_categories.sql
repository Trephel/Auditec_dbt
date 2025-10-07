{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_categorie'
) }}

WITH source_categorie AS (
    SELECT
        cf_1823id AS region_id,                
        cf_1823 AS nom_region,               
        color AS code_color,             
        presence AS presence, 
        sortorderid AS sortorderid,              
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_1823') }}
)

SELECT *
FROM source_categorie
