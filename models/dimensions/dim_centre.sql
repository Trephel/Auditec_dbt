{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_centre'
) }}

WITH centre AS (
    SELECT
        succursalesid AS succursales_id,                
        cf_2009 AS nom_succursales,               
        cf_2011 AS region_succursales,                           
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_succursalescf') }}
)

SELECT *
FROM centre
