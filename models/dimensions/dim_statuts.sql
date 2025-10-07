{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_statut'
) }}

WITH source_statuts AS (
    SELECT
        cf_1361id AS region_id,                
        cf_1361 AS nom_region,               
        color AS code_color,             
        presence AS presence, 
        sortorderid AS sortorderid,              
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_1361') }}
)

SELECT *
FROM source_statuts
