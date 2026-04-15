/*=============================================================================
  AUSTRALIAN IMMUNISATION REGISTER (AIR) — SNOWFLAKE DEMO
  Script 3: Gold Layer — Analytics-Ready Dynamic Tables
  
  Shows: Snowflake computes complex coverage metrics declaratively.
         These Dynamic Tables auto-refresh — no scheduling needed.
         Compare this to maintaining Spark jobs + Delta tables + Airflow DAGs.

  All Dynamic Tables use INCREMENTAL refresh — no non-deterministic
  functions (CURRENT_DATE, CURRENT_TIMESTAMP) in the SELECT list.
  Age calculations are performed at query time in the Streamlit app.
=============================================================================*/

USE DATABASE AIR_DEMO;
USE WAREHOUSE AIR_DEMO_WH;

-- ============================================================================
-- GOLD: Immunisation Coverage by State & Indigenous Status
-- The core metric that Health departments track
-- Age grouping moved to query time to preserve incremental refresh
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.COVERAGE_BY_STATE
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    REFRESH_MODE = INCREMENTAL
    COMMENT = 'Vaccination coverage rates by state and indigenous status'
AS
WITH patient_vax_counts AS (
    SELECT
        ve.patient_id,
        ve.patient_state                AS state,
        ve.patient_is_indigenous        AS is_indigenous,
        COUNT(DISTINCT ve.antigen)      AS distinct_antigens_received,
        COUNT(*)                        AS total_doses
    FROM SILVER.VACCINATION_EVENTS ve
    GROUP BY 1, 2, 3
),
state_totals AS (
    SELECT
        p.state,
        p.is_indigenous,
        COUNT(*)                                                        AS total_patients,
        SUM(COALESCE(pvc.total_doses, 0))                              AS total_doses_administered,
        COUNT(CASE WHEN COALESCE(pvc.distinct_antigens_received, 0) >= 5 
                   THEN 1 END)                                         AS patients_5plus_antigens,
        COUNT(CASE WHEN COALESCE(pvc.distinct_antigens_received, 0) >= 3 
                   THEN 1 END)                                         AS patients_3plus_antigens,
        COUNT(CASE WHEN COALESCE(pvc.total_doses, 0) = 0 
                   THEN 1 END)                                         AS patients_no_vaccination
    FROM SILVER.PATIENTS p
    LEFT JOIN patient_vax_counts pvc ON p.patient_id = pvc.patient_id
    WHERE p.state IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    state,
    is_indigenous,
    total_patients,
    total_doses_administered,
    patients_5plus_antigens,
    patients_3plus_antigens,
    patients_no_vaccination,
    ROUND(patients_3plus_antigens * 100.0 / NULLIF(total_patients, 0), 2) AS coverage_rate_pct,
    ROUND(patients_no_vaccination * 100.0 / NULLIF(total_patients, 0), 2) AS unvaccinated_rate_pct,
    ROUND(total_doses_administered * 1.0 / NULLIF(total_patients, 0), 2)  AS avg_doses_per_patient
FROM state_totals;


-- ============================================================================
-- GOLD: Monthly Vaccination Trends
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.MONTHLY_VACCINATION_TRENDS
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    REFRESH_MODE = INCREMENTAL
    COMMENT = 'Monthly vaccination volumes and trends'
AS
SELECT
    DATE_TRUNC('month', ve.administration_date)  AS month,
    ve.patient_state                             AS state,
    ve.antigen,
    ve.nip_funded,
    COUNT(*)                                     AS doses_administered,
    COUNT(DISTINCT ve.patient_id)                AS unique_patients,
    COUNT(DISTINCT ve.provider_id)               AS active_providers,
    AVG(ve.reporting_lag_days)                   AS avg_reporting_lag_days
FROM SILVER.VACCINATION_EVENTS ve
WHERE ve.administration_date >= '2021-01-01'
GROUP BY 1, 2, 3, 4;


-- ============================================================================
-- GOLD: Provider Performance
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.PROVIDER_PERFORMANCE
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    REFRESH_MODE = INCREMENTAL
    COMMENT = 'Provider-level vaccination activity and timeliness'
AS
SELECT
    ve.provider_id,
    ve.practice_name,
    ve.provider_type,
    ve.provider_state                            AS state,
    COUNT(*)                                     AS total_doses_administered,
    COUNT(DISTINCT ve.patient_id)                AS unique_patients_served,
    COUNT(DISTINCT ve.antigen)                   AS distinct_vaccines_given,
    MIN(ve.administration_date)                  AS first_vaccination_date,
    MAX(ve.administration_date)                  AS last_vaccination_date,
    AVG(ve.reporting_lag_days)                   AS avg_reporting_lag_days,
    COUNT(CASE WHEN ve.reporting_lag_days > 10 
               THEN 1 END)                       AS late_reports_count,
    ROUND(COUNT(CASE WHEN ve.nip_funded = 'Y' THEN 1 END) * 100.0 
          / NULLIF(COUNT(*), 0), 2)              AS nip_funded_pct
FROM SILVER.VACCINATION_EVENTS ve
GROUP BY 1, 2, 3, 4;


-- ============================================================================
-- GOLD: Data Quality Summary
-- Shows the value of the bronze→silver cleansing pipeline
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.DATA_QUALITY_SUMMARY
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    REFRESH_MODE = INCREMENTAL
    COMMENT = 'Data quality metrics across the pipeline'
AS
SELECT
    'PATIENTS' AS source_table,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN dob_parse_failed THEN 1 END) AS date_parse_failures,
    COUNT(CASE WHEN first_name IS NULL OR last_name IS NULL THEN 1 END) AS missing_names,
    COUNT(CASE WHEN medicare_number IS NULL THEN 1 END) AS missing_medicare,
    ROUND(COUNT(CASE WHEN dob_parse_failed THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS date_failure_pct
FROM SILVER.PATIENTS

UNION ALL

SELECT
    'VACCINATIONS',
    COUNT(*),
    COUNT(CASE WHEN admin_date_parse_failed THEN 1 END),
    0,
    0,
    ROUND(COUNT(CASE WHEN admin_date_parse_failed THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
FROM SILVER.VACCINATIONS;


-- ============================================================================
-- GOLD: Childhood Coverage Milestones (1yr, 2yr, 5yr — matching AIR reporting)
-- Uses date_of_birth ranges instead of age_months to avoid CURRENT_DATE()
-- Children's DOB ranges are static — a child born on 2024-04-15 is always
-- in the "1 Year" cohort if DOB is between 12-14 months before a fixed
-- reference point. We use administration_date as the temporal anchor.
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.CHILDHOOD_COVERAGE_MILESTONES
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    REFRESH_MODE = INCREMENTAL
    COMMENT = 'Childhood immunisation coverage at 1yr, 2yr, 5yr milestones — matches AIR national reporting'
AS
WITH vax_summary AS (
    SELECT
        ve.patient_id,
        ve.patient_state                           AS state,
        ve.patient_is_indigenous                   AS is_indigenous,
        ve.patient_dob                             AS date_of_birth,
        COUNT(DISTINCT ve.antigen)                 AS antigens_received,
        COUNT(*)                                   AS total_doses,
        MAX(ve.administration_date)                AS last_vax_date
    FROM SILVER.VACCINATION_EVENTS ve
    WHERE ve.patient_dob IS NOT NULL
    GROUP BY 1, 2, 3, 4
),
with_milestone AS (
    SELECT
        patient_id,
        state,
        is_indigenous,
        date_of_birth,
        antigens_received,
        total_doses,
        CASE
            WHEN last_vax_date BETWEEN DATEADD('month', 12, date_of_birth) 
                                    AND DATEADD('month', 24, date_of_birth) THEN '1 Year'
            WHEN last_vax_date BETWEEN DATEADD('month', 24, date_of_birth) 
                                    AND DATEADD('month', 60, date_of_birth) THEN '2 Years'
            WHEN last_vax_date >= DATEADD('month', 60, date_of_birth) THEN '5 Years'
        END AS milestone
    FROM vax_summary
)
SELECT
    milestone,
    state,
    is_indigenous,
    COUNT(*) AS total_children,
    COUNT(CASE WHEN antigens_received >= 4 THEN 1 END) AS fully_immunised,
    ROUND(COUNT(CASE WHEN antigens_received >= 4 THEN 1 END) * 100.0 
          / NULLIF(COUNT(*), 0), 2) AS coverage_rate_pct,
    95.0 AS target_rate_pct,
    ROUND(COUNT(CASE WHEN antigens_received >= 4 THEN 1 END) * 100.0 
          / NULLIF(COUNT(*), 0), 2) - 95.0 AS gap_to_target_pct
FROM with_milestone
WHERE milestone IS NOT NULL
GROUP BY 1, 2, 3;
