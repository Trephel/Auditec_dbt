{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='stg_vtiger_ventescf2'
) }}

with cleaned as (
    {{ clean_nulls_and_trim('stg_vtiger_ventescf') }}
)

select
    distinct *,
    CURRENT_TIMESTAMP() as date_integration,
    'Vtiger' as source_system
from cleaned
where ventesid is not null
