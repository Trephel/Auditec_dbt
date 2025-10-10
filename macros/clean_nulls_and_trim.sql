{% macro clean_nulls_and_trim(source_table) %}
    {% set columns = adapter.get_columns_in_relation(ref(source_table)) %}

    select
    {% for col in columns %}
        {% if col.data_type in ('TEXT', 'VARCHAR', 'STRING') %}
            coalesce(nullif(trim("{{ col.name }}"), ''), 'Inconnu') as "{{ col.name }}"
        {% elif col.data_type in ('NUMBER', 'FLOAT', 'INT', 'DECIMAL') %}
            coalesce("{{ col.name }}", 0) as "{{ col.name }}"
        {% elif 'DATE' in col.data_type %}
            coalesce("{{ col.name }}", to_date('1900-01-01')) as "{{ col.name }}"
        {% else %}
            "{{ col.name }}"
        {% endif %}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
    from {{ ref(source_table) }}
{% endmacro %}
