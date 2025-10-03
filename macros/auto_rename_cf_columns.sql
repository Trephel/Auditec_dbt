{% macro auto_rename_cf_columns(source_name, table_name, id_column) %}
  {%- set cf_columns = [] -%}
  {%- set clean_names = [] -%}
  
  {%- for col in dbt_utils.get_column_values(
        table=ref('vtiger_field_mapping'),
        column='columnname',
        where="tablename='{}'".format(table_name)
      ) -%}
    {% if col|lower != id_column|lower %}

      {%- do cf_columns.append(col) -%}
      {%- set raw_clean_label = dbt_utils.get_column_values(
            table=ref('vtiger_field_mapping'),
            column='clean_fieldlabel',
            where="tablename='{}' and columnname='{}'".format(table_name, col)
        )[0] -%}

      {%- set clean_label = raw_clean_label
            | lower
            | replace(" ", "_")
            | replace("-", "_")
            | replace("é", "e")
            | replace("è", "e")
            | replace("ê", "e")
            | replace("à", "a")
            | replace("ç", "c")
      -%}

      {# ✅ Vérification des doublons : si déjà présent, on ajoute un suffixe basé sur loop.index #}
      {%- if clean_label in clean_names -%}
        {%- set clean_label = clean_label ~ "_" ~ loop.index -%}
      {%- endif -%}

      {%- do clean_names.append(clean_label) -%}
    {%- endif -%}
  {%- endfor -%}

  select
      {{ id_column }}
      {%- for col, clean in zip(cf_columns, clean_names) %}
        , {{ col }} as "{{ clean }}"
      {%- endfor %}
  from {{ source(source_name, table_name) }}
{% endmacro %}
