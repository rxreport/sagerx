-- stg_fda_shortages__shortages.sql

with

fda_shortages as (

    select * from {{ source('fda_shortages', 'fda_shortages') }}

),

shortages as (

    select
        generic_name,
        status,
        update_type,
        to_date(update_date, 'MM/DD/YYYY') as update_date,
        to_date(initial_posting_date, 'MM/DD/YYYY') as initial_posting_date,
        package_ndc,
        company_name,
        presentation,
        contact_info,
        therapeutic_category::jsonb as therapeutic_category,
        dosage_form,
        availability,
        shortage_reason,
        related_info,
        related_info_link,
        to_date(discontinued_date, 'MM/DD/YYYY') as discontinued_date,
        to_date(change_date, 'MM/DD/YYYY') as change_date,
        resolved_note,
        openfda
    from fda_shortages

)

select
    *
from shortages
