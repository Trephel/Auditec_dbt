{% macro safe_divide(numerator, denominator) %}
    CASE 
        WHEN {{ denominator }} IS NULL OR {{ denominator }} = 0 THEN NULL
        ELSE (CAST({{ numerator }} AS FLOAT) / CAST({{ denominator }} AS FLOAT))
    END
{% endmacro %}
