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
        SUM(TO_DOUBLE(v.montant)) AS montant_realise,
        SUM(TO_DOUBLE(v.quantite)) AS quantite_realisee
    FROM {{ ref('fact_ventes') }} v
    LEFT JOIN {{ ref('dim_periode') }} d_vente 
        ON v.date_vente_id = d_vente.date_id
    GROUP BY
        v.region_id, v.succursales_id,
        d_vente.annee, d_vente.mois
),

-- 2️⃣ OBJECTIFS : unpivot des mois
objectifs_unpivot AS (
    SELECT r.region_id, c.succursales_id AS succursales_id, o."annee", '01' AS mois, o."janvier_ca" AS objectif_montant, o."janvier_vo" AS objectif_quantite
    FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '02', o."fevrier_ca", o."fevrier_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '03', o."mars_ca", o."mars_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '04', o."avril_ca", o."avril_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '05', o."mai_ca", o."mai_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '06', o."juin_ca", o."juin_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '07', o."juillet_ca", o."juillet_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '08', o."aout_ca", o."aout_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '09', o."septembre_ca", o."septembre_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '10', o."octobre_ca", o."octobre_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '11', o."novembre_ca", o."novembre_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
    UNION ALL
    SELECT r.region_id, c.succursales_id, o."annee", '12', o."decembre_ca", o."decembre_vo" FROM {{ ref('stg_vtiger_objectifsuccursalecf') }} o
    LEFT JOIN {{ ref('dim_regions') }} r ON initcap(trim(o."sujet")) = initcap(trim(r.nom_region))
    LEFT JOIN {{ ref('dim_centre') }} c ON initcap(trim(o."sujet")) = initcap(trim(c.nom_succursales))
),

-- 3️⃣ AGRÉGATION DES OBJECTIFS
objectifs AS (
    SELECT
        region_id,
        succursales_id,
        "annee",
        mois,
        SUM(COALESCE(TO_DOUBLE(objectif_montant), 0)) AS objectif_montant,
        SUM(COALESCE(TO_DOUBLE(objectif_quantite), 0)) AS objectif_quantite
    FROM objectifs_unpivot
    GROUP BY region_id, succursales_id, "annee", mois
),

-- 4️⃣ COMPARAISON RÉALISATION VS OBJECTIF
comparaison AS (
    SELECT
        v.region_id,
        v.succursales_id,
        v.annee,
        v.mois,
        v.montant_realise,
        v.quantite_realisee,
        o.objectif_montant,
        o.objectif_quantite,
        ROUND(
            CASE 
                WHEN COALESCE(o.objectif_montant, 0) > 0 
                THEN (v.montant_realise / o.objectif_montant) * 100
                ELSE 0 
            END, 2
        ) AS taux_realisation_montant,
        ROUND(
            CASE 
                WHEN COALESCE(o.objectif_quantite, 0) > 0 
                THEN (v.quantite_realisee / o.objectif_quantite) * 100
                ELSE 0 
            END, 2
        ) AS taux_realisation_quantite,
        CURRENT_TIMESTAMP() AS date_integration,
        'DBT_Calcul' AS source_system
    FROM ventes v
    LEFT JOIN objectifs o
        ON v.region_id = o.region_id
        AND v.succursales_id = o.succursales_id
        AND v.annee = o."annee"
        AND v.mois = o.mois
)

SELECT * FROM comparaison
