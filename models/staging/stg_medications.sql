with source as (
    
    select * from raw_medications

),

renamed as (

    select
        start as medication_start_time,
        stop as medication_end_time,
        patient,
        payer as medication_payer,
        encounter as medication_encounter,
        code as medication_code,
        description as medication_description,
        dispenses,
        payer_coverage as medication_payer_coverage,
        base_cost as base_medication_cost,
        totalcost as total_medication_cost,
        reasoncode as medication_diag_code,
        reasondescription as medication_diag_description

    from source
    qualify row_number() over (partition by encounter) = 1
)

select * from renamed
