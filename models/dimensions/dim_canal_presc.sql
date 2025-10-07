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
)

SELECT *
FROM source_canal_prescr
