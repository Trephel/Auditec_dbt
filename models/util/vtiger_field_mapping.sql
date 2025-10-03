select
    tablename,
    columnname,
    regexp_replace(lower(fieldlabel), '[^a-z0-9]+', '_') as clean_fieldlabel
from {{ source('raw_data', 'vtiger_field') }}
where tablename like 'vtiger_%'