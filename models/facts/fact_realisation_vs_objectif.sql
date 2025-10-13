{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='fact_realisation_vs_objectif'
) }}

-- 1️⃣ VENTES : agrégées par région, centre, mois et année
WITH ventes AS (
    SELECT
        v.region_id,
        v.succursales_id,
        d_vente.annee,
        d_vente.mois,
        SUM(TRY_TO_DOUBLE(v.montant)) AS montant_realise,
        SUM(TRY_TO_DOUBLE(v.quantite)) AS quantite_realisee
    FROM {{ ref('fact_ventes') }} v
    LEFT JOIN {{ ref('dim_periode') }} d_vente 
        ON v.date_vente_id = d_vente.date_id
    GROUP BY
        v.region_id, v.succursales_id,
        d_vente.annee, d_vente.mois
),

-- 2️⃣ OBJECTIFS : unpivot sur la table CRM
objectifs_source AS (
    SELECT
        CAST("annee" AS VARCHAR) AS "annee",
        CAST("janvier_ca" AS VARCHAR) AS "janvier_ca",
        CAST("janvier_vo" AS VARCHAR) AS "janvier_vo",
        CAST("fevrier_ca" AS VARCHAR) AS "fevrier_ca",
        CAST("fevrier_vo" AS VARCHAR) AS "fevrier_vo",
        CAST("mars_ca" AS VARCHAR) AS "mars_ca",
        CAST("mars_vo" AS VARCHAR) AS "mars_vo",
        CAST("avril_ca" AS VARCHAR) AS "avril_ca",
        CAST("avril_vo" AS VARCHAR) AS "avril_vo",
        CAST("mai_ca" AS VARCHAR) AS "mai_ca",
        CAST("mai_vo" AS VARCHAR) AS "mai_vo",
        CAST("juin_ca" AS VARCHAR) AS "juin_ca",
        CAST("juin_vo" AS VARCHAR) AS "juin_vo",
        CAST("juillet_ca" AS VARCHAR) AS "juillet_ca",
        CAST("juillet_vo" AS VARCHAR) AS "juillet_vo",
        CAST("aout_ca" AS VARCHAR) AS "aout_ca",
        CAST("aout_vo" AS VARCHAR) AS "aout_vo",
        CAST("septembre_ca" AS VARCHAR) AS "septembre_ca",
        CAST("septembre_vo" AS VARCHAR) AS "septembre_vo",
        CAST("octobre_ca" AS VARCHAR) AS "octobre_ca",
        CAST("octobre_vo" AS VARCHAR) AS "octobre_vo",
        CAST("novembre_ca" AS VARCHAR) AS "novembre_ca",
        CAST("novembre_vo" AS VARCHAR) AS "novembre_vo",
        CAST("decembre_ca" AS VARCHAR) AS "decembre_ca",
        CAST("decembre_vo" AS VARCHAR) AS "decembre_vo",
        "sujet"
    FROM {{ ref('stg_vtiger_objectifsuccursalecf') }}
    UNPIVOT(val FOR col IN (
        "janvier_ca", "janvier_vo",
        "fevrier_ca", "fevrier_vo",
        "mars_ca", "mars_vo",
        "avril_ca", "avril_vo",
        "mai_ca", "mai_vo",
        "juin_ca", "juin_vo",
        "juillet_ca", "juillet_vo",
        "aout_ca", "aout_vo",
        "septembre_ca", "septembre_vo",
        "octobre_ca", "octobre_vo",
        "novembre_ca", "novembre_vo",
        "decembre_ca", "decembre_vo"
    ))
),


-- 3️⃣ Nettoyage et enrichissement des objectifs
objectifs_unpivot AS (
    SELECT
        CAST(o."annee" AS INT) AS annee,
        c.succursales_id,
        r.region_id,
        CASE
            WHEN col LIKE 'janvier_%' THEN '01'
            WHEN col LIKE 'fevrier_%' THEN '02'
            WHEN col LIKE 'mars_%' THEN '03'
            WHEN col LIKE 'avril_%' THEN '04'
            WHEN col LIKE 'mai_%' THEN '05'
            WHEN col LIKE 'juin_%' THEN '06'
            WHEN col LIKE 'juillet_%' THEN '07'
            WHEN col LIKE 'aout_%' THEN '08'
            WHEN col LIKE 'septembre_%' THEN '09'
            WHEN col LIKE 'octobre_%' THEN '10'
            WHEN col LIKE 'novembre_%' THEN '11'
            WHEN col LIKE 'decembre_%' THEN '12'
        END AS mois,
        CASE WHEN col LIKE '%_ca' THEN TRY_TO_DOUBLE(TO_VARCHAR(val)) ELSE NULL END AS objectif_montant,
        CASE WHEN col LIKE '%_vo' THEN TRY_TO_DOUBLE(TO_VARCHAR(val)) ELSE NULL END AS objectif_quantite

    FROM objectifs_source o
    LEFT JOIN {{ ref('dim_regions') }} r 
        ON INITCAP(TRIM(o."sujet")) = INITCAP(TRIM(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c 
        ON INITCAP(TRIM(o."sujet")) = INITCAP(TRIM(c.nom_succursales))
),

-- 4️⃣ AGRÉGATION DES OBJECTIFS
objectifs AS (
    SELECT
        region_id,
        succursales_id,
        annee,
        mois,
        SUM(COALESCE(objectif_montant, 0)) AS objectif_montant,
        SUM(COALESCE(objectif_quantite, 0)) AS objectif_quantite
    FROM objectifs_unpivot
    GROUP BY region_id, succursales_id, annee, mois
),

-- 5️⃣ COMPARAISON RÉALISATION VS OBJECTIF
comparaison AS (
    SELECT
        v.region_id,
        v.succursales_id,
        v.annee,
        v.mois,
        v.montant_realise,
        o.objectif_montant,
        ROUND(
            CASE WHEN o.objectif_montant > 0
                THEN (v.montant_realise / o.objectif_montant) * 100
                ELSE NULL END, 2
        ) AS taux_realisation_montant,
        v.quantite_realisee,
        o.objectif_quantite,
        ROUND(
            CASE WHEN o.objectif_quantite > 0
                THEN (v.quantite_realisee / o.objectif_quantite) * 100
                ELSE NULL END, 2
        ) AS taux_realisation_quantite,
        CURRENT_TIMESTAMP() AS date_integration,
        'DBT_Calcul' AS source_system
    FROM ventes v
    LEFT JOIN objectifs o
        ON v.region_id = o.region_id
        AND v.succursales_id = o.succursales_id
        AND v.annee = o.annee
        AND v.mois = o.mois
)

SELECT * FROM comparaison