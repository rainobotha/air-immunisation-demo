@echo off
REM =============================================================================
REM  AUSTRALIAN IMMUNISATION REGISTER — SNOWFLAKE DEMO
REM  Windows deployment script (no Python required)
REM
REM  Prerequisites:
REM    - Snowflake CLI (snow) installed: https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation
REM    - Active Snowflake connection (run: snow connection list)
REM    - Pre-generated CSV files in the data\ folder
REM
REM  Usage:
REM    deploy.bat
REM =============================================================================

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
set DATA_DIR=%SCRIPT_DIR%data
set SCRIPTS_DIR=%SCRIPT_DIR%scripts
set STREAMLIT_DIR=%SCRIPT_DIR%streamlit

echo ============================================
echo   AIR Demo — Snowflake Deployment (Windows)
echo ============================================
echo.

REM --- Verify CSV files exist ---
if not exist "%DATA_DIR%\patients.csv" (
    echo ERROR: %DATA_DIR%\patients.csv not found.
    echo Please ensure the pre-generated CSV files are in the data\ folder.
    exit /b 1
)
if not exist "%DATA_DIR%\providers.csv" (
    echo ERROR: %DATA_DIR%\providers.csv not found.
    exit /b 1
)
if not exist "%DATA_DIR%\vaccinations.csv" (
    echo ERROR: %DATA_DIR%\vaccinations.csv not found.
    exit /b 1
)
echo [✓] CSV files found in data\ folder
echo.

REM --- Step 1: Create warehouse, database, schemas, stage ---
echo [1/5] Setting up Snowflake environment...
snow sql -q "CREATE WAREHOUSE IF NOT EXISTS AIR_DEMO_WH WAREHOUSE_SIZE='X-SMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE COMMENT='AIR Demo — XS warehouse handles 750K+ rows with ease';"
snow sql -q "USE WAREHOUSE AIR_DEMO_WH;"
snow sql -q "CREATE DATABASE IF NOT EXISTS AIR_DEMO;"
snow sql -q "CREATE SCHEMA IF NOT EXISTS AIR_DEMO.BRONZE COMMENT='Raw ingested data';"
snow sql -q "CREATE SCHEMA IF NOT EXISTS AIR_DEMO.SILVER COMMENT='Cleansed, conformed, deduplicated data';"
snow sql -q "CREATE SCHEMA IF NOT EXISTS AIR_DEMO.GOLD COMMENT='Analytics-ready aggregates';"
snow sql -q "CREATE SCHEMA IF NOT EXISTS AIR_DEMO.APP COMMENT='Streamlit application';"
snow sql -q "CREATE OR REPLACE FILE FORMAT AIR_DEMO.BRONZE.CSV_FORMAT TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1 NULL_IF=('') EMPTY_FIELD_AS_NULL=TRUE ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE;"
snow sql -q "CREATE OR REPLACE STAGE AIR_DEMO.BRONZE.AIR_LANDING FILE_FORMAT=AIR_DEMO.BRONZE.CSV_FORMAT COMMENT='Landing zone for AIR source CSV files';"
echo   Done.
echo.

REM --- Step 2: Upload CSVs to stage ---
echo [2/5] Uploading CSV files to Snowflake stage...
snow sql -q "PUT file://%DATA_DIR%\patients.csv @AIR_DEMO.BRONZE.AIR_LANDING AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
snow sql -q "PUT file://%DATA_DIR%\providers.csv @AIR_DEMO.BRONZE.AIR_LANDING AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
snow sql -q "PUT file://%DATA_DIR%\vaccinations.csv @AIR_DEMO.BRONZE.AIR_LANDING AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
echo   Done.
echo.

REM --- Step 3: Create bronze tables and load data ---
echo [3/5] Creating bronze tables and loading data...
snow sql -f "%SCRIPTS_DIR%\01_setup_bronze.sql"
echo   Done.
echo.

REM --- Step 4: Create silver and gold dynamic tables ---
echo [4/5] Creating Dynamic Tables (silver + gold layers)...
snow sql -f "%SCRIPTS_DIR%\02_silver_dynamic_tables.sql"
snow sql -f "%SCRIPTS_DIR%\03_gold_dynamic_tables.sql"
echo   Done.
echo.

REM --- Step 5: Deploy Streamlit app ---
echo [5/5] Deploying Streamlit dashboard...
snow sql -q "CREATE OR REPLACE STAGE AIR_DEMO.APP.STREAMLIT_STAGE DIRECTORY=(ENABLE=TRUE) COMMENT='Streamlit app files';"
snow sql -q "PUT file://%STREAMLIT_DIR%\streamlit_app.py @AIR_DEMO.APP.STREAMLIT_STAGE/air_dashboard AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
snow sql -q "CREATE OR REPLACE STREAMLIT AIR_DEMO.APP.AIR_DASHBOARD ROOT_LOCATION='@AIR_DEMO.APP.STREAMLIT_STAGE/air_dashboard' MAIN_FILE='streamlit_app.py' QUERY_WAREHOUSE=AIR_DEMO_WH COMMENT='Australian Immunisation Register — Coverage Analytics Dashboard';"
echo   Done.
echo.

REM --- Step 6: Trigger initial refresh ---
echo [Bonus] Triggering initial Dynamic Table refresh...
snow sql -q "ALTER DYNAMIC TABLE AIR_DEMO.SILVER.PATIENTS REFRESH;"
snow sql -q "ALTER DYNAMIC TABLE AIR_DEMO.SILVER.PROVIDERS REFRESH;"
snow sql -q "ALTER DYNAMIC TABLE AIR_DEMO.SILVER.VACCINATIONS REFRESH;"
snow sql -q "ALTER DYNAMIC TABLE AIR_DEMO.SILVER.VACCINATION_EVENTS REFRESH;"
snow sql -q "ALTER DYNAMIC TABLE AIR_DEMO.GOLD.COVERAGE_BY_STATE_AGE REFRESH;"
snow sql -q "ALTER DYNAMIC TABLE AIR_DEMO.GOLD.MONTHLY_VACCINATION_TRENDS REFRESH;"
snow sql -q "ALTER DYNAMIC TABLE AIR_DEMO.GOLD.PROVIDER_PERFORMANCE REFRESH;"
snow sql -q "ALTER DYNAMIC TABLE AIR_DEMO.GOLD.DATA_QUALITY_SUMMARY REFRESH;"
snow sql -q "ALTER DYNAMIC TABLE AIR_DEMO.GOLD.CHILDHOOD_COVERAGE_MILESTONES REFRESH;"
echo   Done.
echo.

echo ============================================
echo   Deployment complete!
echo ============================================
echo.
echo   Database:    AIR_DEMO
echo   Warehouse:   AIR_DEMO_WH (X-Small)
echo   Schemas:     BRONZE, SILVER, GOLD, APP
echo   Streamlit:   AIR_DEMO.APP.AIR_DASHBOARD
echo.
echo   Open the Streamlit app in Snowsight:
echo   Projects ^> Streamlit ^> AIR_DASHBOARD
echo.

endlocal
