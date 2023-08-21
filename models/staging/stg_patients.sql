with source as (

    select * from {{ ref('raw_patients') }}

),

renamed as (
    
    select
        id as patient,
        try_cast(birthdate as date) as birth_date,
        try_cast(deathdate as date) as death_date,
        case
            when death_date is null then year(age(birth_date))
            else year(age(death_date, birth_date))
            end as age,
        ssn,
        drivers as drivers_license_no,
        passport as passport_no,
        first as first_name,
        last as last_name,
        marital as marital_status,
        race,
        ethnicity,
        gender,
        address,
        city,
        state,
        zip,
        county,
        lon,
        lat,
        round(healthcare_expenses, 2) as healthcare_expenses,
        round(healthcare_coverage, 2) as healthcare_coverage

    from source
    qualify row_number() over (partition by id) = 1

)

select * from renamed
