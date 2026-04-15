/*=============================================================================
  AUSTRALIAN IMMUNISATION REGISTER (AIR) — SNOWFLAKE DEMO
  Script 2: Silver Layer — Dynamic Tables
  
  Shows: Declarative data pipelines with ZERO orchestration.
         No Spark jobs, no Airflow, no notebooks to schedule.
         Snowflake handles all the refresh logic automatically.
         
  Key differentiator vs Databricks:
  - No cluster spin-up time
  - No Delta Lake configuration
  - No Unity Catalog setup overhead
  - Just SQL — Dynamic Tables handle incremental refresh automatically
=============================================================================*/

USE DATABASE AIR_DEMO;
USE WAREHOUSE AIR_DEMO_WH;

-- ============================================================================
-- SILVER: Cleansed Patients
-- Handles: date parsing, standardisation, NULL handling, trimming
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE SILVER.PATIENTS
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    REFRESH_MODE = INCREMENTAL
    COMMENT = 'Cleansed patient demographics — parsed dates, standardised fields'
AS
SELECT
    patient_id,
    medicare_number,
    INITCAP(TRIM(first_name))                                       AS first_name,
    INITCAP(TRIM(last_name))                                        AS last_name,
    TRIM(first_name) || ' ' || TRIM(last_name)                      AS full_name,
    TRY_TO_DATE(date_of_birth, 'YYYY-MM-DD')                        AS date_of_birth,
    CASE WHEN TRY_TO_DATE(date_of_birth, 'YYYY-MM-DD') IS NULL 
         THEN TRUE ELSE FALSE END                                    AS dob_parse_failed,
    UPPER(TRIM(gender))                                              AS gender,
    COALESCE(NULLIF(TRIM(indigenous_status), ''), 'Not Stated')      AS indigenous_status,
    CASE WHEN indigenous_status IN ('Aboriginal', 'Torres Strait Islander', 'Both')
         THEN TRUE ELSE FALSE END                                    AS is_indigenous,
    UPPER(TRIM(state))                                               AS state,
    TRIM(postcode)                                                   AS postcode,
    TRIM(address_line1)                                              AS address_line1,
    INITCAP(TRIM(suburb))                                            AS suburb,
    TRIM(phone)                                                      AS phone,
    LOWER(TRIM(email))                                               AS email,
    _loaded_at,
    _source_file
FROM AIR_DEMO.BRONZE.RAW_PATIENTS
WHERE patient_id IS NOT NULL;


-- ============================================================================
-- SILVER: Cleansed Providers
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE SILVER.PROVIDERS
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    REFRESH_MODE = INCREMENTAL
    COMMENT = 'Cleansed provider reference data'
AS
SELECT
    provider_id,
    UPPER(TRIM(provider_number))                                    AS provider_number,
    INITCAP(TRIM(provider_type))                                    AS provider_type,
    INITCAP(TRIM(practice_name))                                    AS practice_name,
    INITCAP(TRIM(provider_first_name))                              AS provider_first_name,
    INITCAP(TRIM(provider_last_name))                               AS provider_last_name,
    TRIM(provider_first_name) || ' ' || TRIM(provider_last_name)    AS provider_full_name,
    UPPER(TRIM(state))                                              AS state,
    TRIM(postcode)                                                  AS postcode,
    TRIM(phone)                                                     AS phone,
    _loaded_at,
    _source_file
FROM AIR_DEMO.BRONZE.RAW_PROVIDERS
WHERE provider_id IS NOT NULL;


-- ============================================================================
-- SILVER: Cleansed & Deduplicated Vaccinations
-- Handles: date parsing, deduplication, dose validation
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE SILVER.VACCINATIONS
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    REFRESH_MODE = INCREMENTAL
    COMMENT = 'Cleansed, deduplicated vaccination events'
AS
WITH deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id, antigen, dose_number, 
                         TRY_TO_DATE(administration_date, 'YYYY-MM-DD')
            ORDER BY _loaded_at DESC
        ) AS _rn
    FROM AIR_DEMO.BRONZE.RAW_VACCINATIONS
    WHERE vaccination_id IS NOT NULL
      AND patient_id IS NOT NULL
)
SELECT
    vaccination_id,
    patient_id,
    provider_id,
    INITCAP(TRIM(vaccine_brand))                                     AS vaccine_brand,
    TRIM(antigen)                                                    AS antigen,
    TRY_CAST(dose_number AS INTEGER)                                 AS dose_number,
    UPPER(TRIM(batch_number))                                        AS batch_number,
    TRY_TO_DATE(administration_date, 'YYYY-MM-DD')                   AS administration_date,
    CASE WHEN TRY_TO_DATE(administration_date, 'YYYY-MM-DD') IS NULL 
         THEN TRUE ELSE FALSE END                                    AS admin_date_parse_failed,
    TRY_TO_DATE(reporting_date, 'YYYY-MM-DD')                        AS reporting_date,
    DATEDIFF('day', TRY_TO_DATE(administration_date, 'YYYY-MM-DD'),
                    TRY_TO_DATE(reporting_date, 'YYYY-MM-DD'))       AS reporting_lag_days,
    INITCAP(TRIM(administration_site))                               AS administration_site,
    INITCAP(TRIM(route))                                             AS route,
    UPPER(TRIM(nip_funded))                                          AS nip_funded,
    UPPER(TRIM(school_program))                                      AS school_program,
    NULLIF(TRIM(vial_serial_number), '')                             AS vial_serial_number,
    _loaded_at,
    _source_file
FROM deduplicated
WHERE _rn = 1;


-- ============================================================================
-- SILVER: Enriched Vaccination Events (joined with patient & provider)
-- This is the main fact table for analytics
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE SILVER.VACCINATION_EVENTS
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    REFRESH_MODE = INCREMENTAL
    COMMENT = 'Enriched vaccination events — joined with patient demographics and provider details'
AS
SELECT
    v.vaccination_id,
    v.patient_id,
    v.provider_id,
    v.vaccine_brand,
    v.antigen,
    v.dose_number,
    v.batch_number,
    v.administration_date,
    v.reporting_date,
    v.reporting_lag_days,
    v.administration_site,
    v.route,
    v.nip_funded,
    v.school_program,
    v.vial_serial_number,
    p.first_name          AS patient_first_name,
    p.last_name           AS patient_last_name,
    p.date_of_birth       AS patient_dob,
    p.gender              AS patient_gender,
    p.indigenous_status   AS patient_indigenous_status,
    p.is_indigenous       AS patient_is_indigenous,
    p.state               AS patient_state,
    p.postcode            AS patient_postcode,
    pr.provider_type,
    pr.practice_name,
    pr.provider_full_name,
    pr.state              AS provider_state
FROM SILVER.VACCINATIONS v
JOIN SILVER.PATIENTS p  ON v.patient_id = p.patient_id
JOIN SILVER.PROVIDERS pr ON v.provider_id = pr.provider_id
WHERE v.administration_date IS NOT NULL;
