with source as (
    
    select * from {{ ref('raw_medications') }}

),

renamed as (

    select
        start as medication_start_time,
        stop as medication_end_time,
        patient,
        payer,
        encounter,
        code,
        description,
        base_cost,
        payer_coverage,
        dispenses,
        totalcost as total_cost,
        reasoncode as reason_code,
        reasondescription as reason_description

    from source
    qualify row_number() over (partition by encounter) = 1
)

select * from renamed
