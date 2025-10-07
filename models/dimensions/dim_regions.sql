{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_region'
) }}

WITH source_region AS (
    SELECT
        cf_1977id AS region_id,                 -- identifiant unique
        cf_1977 AS nom_region,               -- nom ou libellé de la région
        color AS code_region,            
        presence AS presence, 
      
        CURRENT_TIMESTAMP() AS date_integration,
        'Vtiger' AS source_system
    FROM {{ source('raw_data', 'vtiger_cf_1977') }}
)

SELECT *
FROM source_region
