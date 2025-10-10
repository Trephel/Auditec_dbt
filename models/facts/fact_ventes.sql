{{ 
    config(
        materialized='table', 
        schema='AIRBYTE_SCHEMA_AUDI', 
        alias='fact_ventes' 
    ) 
}} 

with ventes_stg as (
    select * from {{ ref('stg_vtiger_ventescf2') }}
)

select
    v.ventesid as vente_id,

    -- Clés des dimensions
    r.region_id,
    ce.succursales_id,
    ca.canal_presc_id,
    re.reseau_id,
    m.marque_id,
    s.statut_id,
    p.pop_id,
    cat.categorie_id,
    pr.prescripteur_id,

    -- Mesures
    v."quantite" as quantite,
    v."montant" as montant,
    v."perte" as perte,
    v."nom_complet" as nom_complet,

    -- Dates
    d_vente.date_id as date_vente_id,
    d_livraison.date_id as date_livraison_id,
    d_confirmation.date_id as date_confirmation_id,

from ventes_stg v

-- JOINTURES DIMENSIONS

left join {{ ref('dim_regions') }} r
    on initcap(trim(v."region")) = initcap(trim(r.nom_region))

left join {{ ref('dim_centre') }} ce
    on initcap(trim(v."succursale")) = initcap(trim(ce.nom_succursales))

left join {{ ref('dim_canal_presc') }} ca
    on initcap(trim(v."canal_de_prescription")) = initcap(trim(ca.nom_canal_presc))

left join {{ ref('dim_marques') }} m
    on initcap(trim(v."marque")) = initcap(trim(m.nom_marque))

left join {{ ref('dim_statuts') }} s
    on initcap(trim(v."statut")) = initcap(trim(s.nom_statut))

left join {{ ref('dim_reseau') }} re
    on initcap(trim(v."reseau")) = initcap(trim(re.nom_reseau))

left join {{ ref('dim_population') }} p
    on initcap(trim(v."pay_1")) = initcap(trim(p.nom_population))

left join {{ ref('dim_categories') }} cat
    on initcap(trim(v."categorie_medecin")) = initcap(trim(cat.nom_categorie))

left join {{ ref('dim_prescripteur') }} pr
    on initcap(trim(v."cr_par")) = initcap(trim(pr.nom_prescripteur))

-- DATES
left join {{ ref('dim_periode') }} d_vente
    on to_date(v."date_vente") = d_vente.date_id

left join {{ ref('dim_periode') }} d_livraison
    on to_date(v."date_livraison") = d_livraison.date_id

left join {{ ref('dim_periode') }} d_confirmation
    on to_date(v."date_confirmation") = d_confirmation.date_id

where v.ventesid is not null
