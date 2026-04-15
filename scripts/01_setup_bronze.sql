/*=============================================================================
  AUSTRALIAN IMMUNISATION REGISTER (AIR) — SNOWFLAKE DEMO
  Script 1: Environment Setup & Bronze Layer

  Shows: Snowflake simplicity — single SQL script sets up everything.
         No clusters to provision, no Spark config, no infra management.

  HOW TO RUN:
    1. Open a Snowsight worksheet
    2. Paste this entire script
    3. Click "Run All" (or select all → Run)
=============================================================================*/

-- 1. CREATE WAREHOUSE (XS is all we need for 750K rows — cost effective!)
CREATE WAREHOUSE IF NOT EXISTS AIR_DEMO_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'AIR Demo — XS warehouse handles 750K+ rows with ease';

USE WAREHOUSE AIR_DEMO_WH;

-- 2. CREATE DATABASE & SCHEMAS (medallion architecture)
CREATE DATABASE IF NOT EXISTS AIR_DEMO;
USE DATABASE AIR_DEMO;

CREATE SCHEMA IF NOT EXISTS BRONZE  COMMENT = 'Raw ingested data — as received from source systems';
CREATE SCHEMA IF NOT EXISTS SILVER  COMMENT = 'Cleansed, conformed, deduplicated data';
CREATE SCHEMA IF NOT EXISTS GOLD    COMMENT = 'Analytics-ready aggregates and coverage metrics';
CREATE SCHEMA IF NOT EXISTS APP     COMMENT = 'Streamlit application';

-- 3. FILE FORMAT — one format handles all our CSVs
CREATE OR REPLACE FILE FORMAT BRONZE.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- 4. INTERNAL STAGE — CSVs will be uploaded here via Snowsight UI
CREATE OR REPLACE STAGE BRONZE.AIR_LANDING
    FILE_FORMAT = BRONZE.CSV_FORMAT
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Landing zone for AIR source CSV files';

-- 5. BRONZE TABLES — raw, as-is from source (all STRING to preserve source fidelity)
USE SCHEMA BRONZE;

CREATE OR REPLACE TABLE RAW_PATIENTS (
    patient_id          STRING,
    medicare_number     STRING,
    first_name          STRING,
    last_name           STRING,
    date_of_birth       STRING,
    gender              STRING,
    indigenous_status   STRING,
    state               STRING,
    postcode            STRING,
    address_line1       STRING,
    suburb              STRING,
    phone               STRING,
    email               STRING,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        STRING
);

CREATE OR REPLACE TABLE RAW_PROVIDERS (
    provider_id         STRING,
    provider_number     STRING,
    provider_type       STRING,
    practice_name       STRING,
    provider_first_name STRING,
    provider_last_name  STRING,
    state               STRING,
    postcode            STRING,
    phone               STRING,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        STRING
);

CREATE OR REPLACE TABLE RAW_VACCINATIONS (
    vaccination_id      STRING,
    patient_id          STRING,
    provider_id         STRING,
    vaccine_brand       STRING,
    antigen             STRING,
    dose_number         STRING,
    batch_number        STRING,
    administration_date STRING,
    reporting_date      STRING,
    administration_site STRING,
    route               STRING,
    nip_funded          STRING,
    school_program      STRING,
    vial_serial_number  STRING,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        STRING
);

/*=============================================================================
  STOP HERE — Upload the 3 CSV files to the stage before continuing.

  In Snowsight:
    1. Navigate to Data > Databases > AIR_DEMO > BRONZE > Stages > AIR_LANDING
    2. Click "+ Files" button
    3. Upload: patients.csv, providers.csv, vaccinations.csv
    4. Then come back and run the COPY INTO statements below
=============================================================================*/

COPY INTO RAW_PATIENTS (patient_id, medicare_number, first_name, last_name, date_of_birth,
                        gender, indigenous_status, state, postcode, address_line1, suburb, phone, email)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
    FROM @AIR_LANDING
)
PATTERN = '.*patients.*'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO RAW_PROVIDERS (provider_id, provider_number, provider_type, practice_name,
                         provider_first_name, provider_last_name, state, postcode, phone)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9
    FROM @AIR_LANDING
)
PATTERN = '.*providers.*'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO RAW_VACCINATIONS (vaccination_id, patient_id, provider_id, vaccine_brand, antigen,
                            dose_number, batch_number, administration_date, reporting_date,
                            administration_site, route, nip_funded, school_program, vial_serial_number)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
    FROM @AIR_LANDING
)
PATTERN = '.*vaccinations.*'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Quick validation — you should see ~100K patients, ~2K providers, ~751K vaccinations
SELECT 'RAW_PATIENTS' AS table_name, COUNT(*) AS row_count FROM RAW_PATIENTS
UNION ALL
SELECT 'RAW_PROVIDERS', COUNT(*) FROM RAW_PROVIDERS
UNION ALL
SELECT 'RAW_VACCINATIONS', COUNT(*) FROM RAW_VACCINATIONS;
