/*=============================================================================
  AUSTRALIAN IMMUNISATION REGISTER (AIR) — SNOWFLAKE DEMO
  Script 4: Deploy Streamlit App to Snowflake

  Shows: Streamlit in Snowflake — no separate app server needed.
         The entire analytics stack runs inside Snowflake.

  HOW TO RUN:
    1. Run the SQL below to create the stage
    2. Upload streamlit_app.py to the stage (see instructions in script)
    3. Run the CREATE STREAMLIT statement
=============================================================================*/

USE DATABASE AIR_DEMO;
USE WAREHOUSE AIR_DEMO_WH;

CREATE SCHEMA IF NOT EXISTS APP COMMENT = 'Streamlit application';

CREATE OR REPLACE STAGE APP.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Streamlit app files';

/*=============================================================================
  STOP HERE — Upload the Streamlit app file to the stage.

  In Snowsight:
    1. Navigate to Data > Databases > AIR_DEMO > APP > Stages > STREAMLIT_STAGE
    2. Click "+ Files" button
    3. Upload: streamlit_app.py
    4. Then come back and run the CREATE STREAMLIT statement below
=============================================================================*/

CREATE OR REPLACE STREAMLIT APP.AIR_DASHBOARD
    ROOT_LOCATION = '@AIR_DEMO.APP.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = AIR_DEMO_WH
    COMMENT = 'Australian Immunisation Register — Coverage Analytics Dashboard';
