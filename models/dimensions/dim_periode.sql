{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='dim_periode'
) }}

WITH date_spine AS (

    -- Génère une série de dates entre 2020 et 2030
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}

),

final AS (
    SELECT
        date_day AS date_id,                            -- identifiant unique (clé primaire)
        date_day AS date_complete,                      -- date complète
        EXTRACT(year FROM date_day) AS annee,
        EXTRACT(month FROM date_day) AS mois,
        EXTRACT(day FROM date_day) AS jour,
        EXTRACT(quarter FROM date_day) AS trimestre,
        TO_CHAR(date_day, 'YYYY-MM') AS mois_annee,
        TO_CHAR(date_day, 'YYYY-"T"Q') AS trimestre_annee,
        TO_CHAR(date_day, 'YYYY') AS annee_texte,
        TO_CHAR(date_day, 'Month') AS nom_mois,
        TO_CHAR(date_day, 'Day') AS nom_jour,
        WEEK(date_day) AS semaine_annee,
        CASE
            WHEN EXTRACT(quarter FROM date_day) = 1 THEN 'T1'
            WHEN EXTRACT(quarter FROM date_day) = 2 THEN 'T2'
            WHEN EXTRACT(quarter FROM date_day) = 3 THEN 'T3'
            WHEN EXTRACT(quarter FROM date_day) = 4 THEN 'T4'
        END AS trimestre_label,
        CASE
            WHEN EXTRACT(month FROM date_day) BETWEEN 1 AND 3 THEN 'H1'
            WHEN EXTRACT(month FROM date_day) BETWEEN 4 AND 6 THEN 'H1'
            WHEN EXTRACT(month FROM date_day) BETWEEN 7 AND 9 THEN 'H2'
            ELSE 'H2'
        END AS semestre_label,
        CURRENT_TIMESTAMP() AS date_integration,
        'DBT_GENERATED' AS source_system
    FROM date_spine
)

SELECT * FROM final
