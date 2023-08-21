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
        payer,
        encounterclass as encounter_class,
        code,
        description,
        base_encounter_cost,
        total_claim_cost,
        payer_coverage

    from source
    qualify row_number() over (partition by id) = 1

)

select * from renamed
