{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_marques'
) }}

WITH source_marque AS (
    SELECT
        cf_2647id AS region_id,                
        cf_2647 AS nom_region,               
        color AS code_color,             
        presence AS presence, 
        sortorderid AS sortorderid,              
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_2647') }}
)

SELECT *
FROM source_marque
