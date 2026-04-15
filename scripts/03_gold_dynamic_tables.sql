/*=============================================================================
  AUSTRALIAN IMMUNISATION REGISTER (AIR) — SNOWFLAKE DEMO
  Script 3: Gold Layer — Analytics-Ready Dynamic Tables
  
  Shows: Snowflake computes complex coverage metrics declaratively.
         These Dynamic Tables auto-refresh — no scheduling needed.
         Compare this to maintaining Spark jobs + Delta tables + Airflow DAGs.
=============================================================================*/

USE DATABASE AIR_DEMO;
USE WAREHOUSE AIR_DEMO_WH;

-- ============================================================================
-- GOLD: Immunisation Coverage by State & Age Group
-- The core metric that Health departments track
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.COVERAGE_BY_STATE_AGE
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    COMMENT = 'Vaccination coverage rates by state and age group'
AS
WITH patient_vax_counts AS (
    SELECT
        ve.patient_id,
        ve.patient_state                AS state,
        ve.patient_age_group            AS age_group,
        ve.patient_is_indigenous        AS is_indigenous,
        COUNT(DISTINCT ve.antigen)      AS distinct_antigens_received,
        COUNT(*)                        AS total_doses
    FROM SILVER.VACCINATION_EVENTS ve
    GROUP BY 1, 2, 3, 4
),
state_age_totals AS (
    SELECT
        p.state,
        p.age_group,
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
    WHERE p.state IS NOT NULL AND p.age_group IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT
    state,
    age_group,
    is_indigenous,
    total_patients,
    total_doses_administered,
    patients_5plus_antigens,
    patients_3plus_antigens,
    patients_no_vaccination,
    ROUND(patients_3plus_antigens * 100.0 / NULLIF(total_patients, 0), 2) AS coverage_rate_pct,
    ROUND(patients_no_vaccination * 100.0 / NULLIF(total_patients, 0), 2) AS unvaccinated_rate_pct,
    ROUND(total_doses_administered * 1.0 / NULLIF(total_patients, 0), 2)  AS avg_doses_per_patient,
    CURRENT_TIMESTAMP()                                                    AS _refreshed_at
FROM state_age_totals;


-- ============================================================================
-- GOLD: Monthly Vaccination Trends
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.MONTHLY_VACCINATION_TRENDS
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
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
    AVG(ve.reporting_lag_days)                   AS avg_reporting_lag_days,
    CURRENT_TIMESTAMP()                          AS _refreshed_at
FROM SILVER.VACCINATION_EVENTS ve
WHERE ve.administration_date >= '2021-01-01'
GROUP BY 1, 2, 3, 4;


-- ============================================================================
-- GOLD: Provider Performance
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.PROVIDER_PERFORMANCE
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
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
          / NULLIF(COUNT(*), 0), 2)              AS nip_funded_pct,
    CURRENT_TIMESTAMP()                          AS _refreshed_at
FROM SILVER.VACCINATION_EVENTS ve
GROUP BY 1, 2, 3, 4;


-- ============================================================================
-- GOLD: Data Quality Summary
-- Shows the value of the bronze→silver cleansing pipeline
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.DATA_QUALITY_SUMMARY
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    COMMENT = 'Data quality metrics across the pipeline'
AS
SELECT
    'PATIENTS' AS source_table,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN dob_parse_failed THEN 1 END) AS date_parse_failures,
    COUNT(CASE WHEN first_name IS NULL OR last_name IS NULL THEN 1 END) AS missing_names,
    COUNT(CASE WHEN medicare_number IS NULL THEN 1 END) AS missing_medicare,
    ROUND(COUNT(CASE WHEN dob_parse_failed THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS date_failure_pct,
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM SILVER.PATIENTS

UNION ALL

SELECT
    'VACCINATIONS',
    COUNT(*),
    COUNT(CASE WHEN admin_date_parse_failed THEN 1 END),
    0,
    0,
    ROUND(COUNT(CASE WHEN admin_date_parse_failed THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2),
    CURRENT_TIMESTAMP()
FROM SILVER.VACCINATIONS;


-- ============================================================================
-- GOLD: Childhood Coverage Milestones (1yr, 2yr, 5yr — matching AIR reporting)
-- ============================================================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.CHILDHOOD_COVERAGE_MILESTONES
    TARGET_LAG = '1 hour'
    WAREHOUSE = AIR_DEMO_WH
    COMMENT = 'Childhood immunisation coverage at 1yr, 2yr, 5yr milestones — matches AIR national reporting'
AS
WITH children AS (
    SELECT
        p.patient_id,
        p.state,
        p.is_indigenous,
        p.date_of_birth,
        p.age_months
    FROM SILVER.PATIENTS p
    WHERE p.date_of_birth IS NOT NULL
      AND p.age_months BETWEEN 12 AND 72
),
vax_by_milestone AS (
    SELECT
        c.patient_id,
        c.state,
        c.is_indigenous,
        c.age_months,
        CASE
            WHEN c.age_months BETWEEN 12 AND 14 THEN '1 Year'
            WHEN c.age_months BETWEEN 24 AND 26 THEN '2 Years'
            WHEN c.age_months BETWEEN 60 AND 62 THEN '5 Years'
        END AS milestone,
        COUNT(DISTINCT v.antigen) AS antigens_received,
        COUNT(*) AS total_doses
    FROM children c
    LEFT JOIN SILVER.VACCINATIONS v 
        ON c.patient_id = v.patient_id
        AND v.administration_date <= DATEADD('month', 
            CASE
                WHEN c.age_months BETWEEN 12 AND 14 THEN 12
                WHEN c.age_months BETWEEN 24 AND 26 THEN 24
                WHEN c.age_months BETWEEN 60 AND 62 THEN 60
            END, c.date_of_birth)
    WHERE c.age_months BETWEEN 12 AND 14
       OR c.age_months BETWEEN 24 AND 26
       OR c.age_months BETWEEN 60 AND 62
    GROUP BY 1, 2, 3, 4, 5
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
          / NULLIF(COUNT(*), 0), 2) - 95.0 AS gap_to_target_pct,
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM vax_by_milestone
WHERE milestone IS NOT NULL
GROUP BY 1, 2, 3;
