{{ config(
    materialized='table',
    schema='AIRBYTE_SCHEMA_AUDI',
    alias='fact_ventes'
) }}

with ventes_stg as (
    select * from {{ ref('stg_vtiger_ventescf') }}
)

select
    v.ventesid as vente_id,

    -- Clés des dimensions
    r.region_id,
    c.succursales_id,
    ca.canal_presc_id,
    re.reseau_id,
    m.marque_id,
    s.statut_id,

    -- Mesures
    v."quantite" as quantite,
    v."montant" as montant,
    v."perte" as perte,

    -- Dates
    d_vente.date_id as date_vente_id,
    d_livraison.date_id as date_livraison_id,
    d_confirmation.date_id as date_confirmation_id,
    d_creation.date_id as date_creation_id

from ventes_stg v

-- REGION
left join {{ ref('dim_regions') }} r 
       on initcap(trim(v."region")) = initcap(trim(r.region_id))

-- CENTRE
left join {{ ref('dim_centre') }} c 
       on initcap(trim(v."succursale")) = initcap(trim(c.succursales_id))

-- CANAL
left join {{ ref('dim_canal_presc') }} ca 
       on initcap(trim(v."canal_de_prescription")) = initcap(trim(ca.canal_presc_id))

-- MARQUE
left join {{ ref('dim_marques') }} m 
       on initcap(trim(v."marque")) = initcap(trim(m.marque_id))

-- STATUT
left join {{ ref('dim_statuts') }} s 
       on initcap(trim(v."statut")) = initcap(trim(s.statut_id))

-- RESEAU
left join {{ ref('dim_reseau') }} re 
       on initcap(trim(v."reseau")) = initcap(trim(re.reseau_id))

-- POPULATION
left join {{ ref('dim_population') }} p 
       on initcap(trim(v."pay_1")) = initcap(trim(p.pop_id))

-- CATEGORIE
left join {{ ref('dim_categories') }} c 
       on initcap(trim(v."tel")) = initcap(trim(c.categorie_id))

-- PRESCRIPEUR
left join {{ ref('dim_prescripteur') }}  p
       on initcap(trim(v."mois")) = initcap(trim(p.prescripteur_id))

-- DATES
left join {{ ref('dim_periode') }} d_vente 
       on v."date_vente" = d_vente.date_complete

left join {{ ref('dim_periode') }} d_livraison 
       on v."date_livraison" = d_livraison.date_complete

left join {{ ref('dim_periode') }} d_confirmation 
       on v."date_confirmation" = d_confirmation.date_complete

left join {{ ref('dim_periode') }} d_creation 
       on d_creation.date_complete = d_creation.date_complete  

where v.ventesid is not null
