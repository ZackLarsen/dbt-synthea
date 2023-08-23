with source as (

    select * from {{ ref('raw_encounters') }}

),

renamed as (

    select
        id as encounter,
        start as encounter_start_time,
        stop as encounter_stop_time,
        patient,
        organization,
        provider,
        payer as encounter_payer,
        payer_coverage as encounter_payer_coverage,
        encounterclass as encounter_class,
        code as encounter_code,
        description as encounter_description,
        base_encounter_cost,
        total_claim_cost as total_encounter_cost,
        reasoncode as encounter_diag_code,
        reasondescription as encounter_diag_description

    from source
    qualify row_number() over (partition by id) = 1

)

select * from renamed
