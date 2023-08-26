with patients as (

    select * from {{ ref('stg_patients') }}

),

medications as (

    select * from {{ ref('stg_medications') }}

),

encounters as (

    select * from {{ ref('stg_encounters') }}

),

patient_encounters as (

    select
        patients.patient,
        patients.first_name,
        patients.last_name,
        patients.birth_date,
        patients.death_date,
        patients.age,
        encounters.encounter,
        encounters.encounter_start_time,
        encounters.encounter_stop_time,
        encounters.encounter_class,
        encounters.encounter_diag_code,
        encounters.encounter_diag_description,
        encounters.base_encounter_cost,
        encounters.total_encounter_cost,
        encounters.encounter_payer,
        encounters.encounter_payer_coverage,
        patients.healthcare_expenses,
        patients.healthcare_coverage,
    
    from patients
    
    left join encounters
        on patients.patient = encounters.patient

),

patient_medications as (

    select
        patients.patient,
        patients.first_name,
        patients.last_name,
        patients.birth_date,
        patients.death_date,
        patients.age,
        medications.medication_start_time,
        medications.medication_end_time,
        medications.medication_code,
        medications.medication_description,
        medications.medication_diag_code,
        medications.medication_diag_description,
        medications.dispenses,
        medications.medication_payer,
        medications.medication_payer_coverage,
        medications.base_medication_cost,
        medications.total_medication_cost,
        patients.healthcare_expenses,
        patients.healthcare_coverage

    from patients

    left join medications
        on patients.patient = medications.patient

),

final as (

    select
        patients.patient,
        patients.first_name,
        patients.last_name,
        patients.birth_date,
        patients.death_date,
        patients.age,
        patient_encounters.encounter,
        patient_encounters.encounter_start_time,
        patient_encounters.encounter_stop_time,
        patient_encounters.encounter_class,
        patient_encounters.encounter_diag_code,
        patient_encounters.encounter_diag_description,
        patient_encounters.base_encounter_cost,
        patient_encounters.total_encounter_cost,
        patient_encounters.encounter_payer,
        patient_encounters.encounter_payer_coverage,
        patient_medications.medication_start_time,
        patient_medications.medication_end_time,
        patient_medications.medication_code,
        patient_medications.medication_description,
        patient_medications.medication_diag_code,
        patient_medications.medication_diag_description,
        patient_medications.dispenses,
        patient_medications.medication_payer,
        patient_medications.medication_payer_coverage,
        patient_medications.base_medication_cost,
        patient_medications.total_medication_cost,
        patients.healthcare_expenses,
        patients.healthcare_coverage

    from patients

    left join patient_encounters
        on patients.patient = patient_encounters.patient

    left join patient_medications
        on patients.patient = patient_medications.patient

)

select * from final
