{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_reseau'
) }}

WITH source_reseau AS (
    SELECT
        cf_2777id AS region_id,                
        cf_2777 AS nom_region,               
        color AS code_color,             
        presence AS presence, 
        sortorderid AS sortorderid,              
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_2777') }}
)

SELECT *
FROM source_reseau
