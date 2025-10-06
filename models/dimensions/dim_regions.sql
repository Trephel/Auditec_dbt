{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_region'
) }}

WITH source_succursales AS (
    SELECT
        "region" AS nom_region,
        "SUCCURSALESID" AS region_id,
        "delai_transport_prevu" AS delai_transport,
        
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ ref('stg_vtiger_succursalescf') }}
)

SELECT *
FROM source_succursales
