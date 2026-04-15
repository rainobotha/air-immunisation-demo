# Australian Immunisation Register — Snowflake Demo

> **Medallion Architecture · Dynamic Tables · Zero Orchestration**
>
> A complete data pipeline demo using synthetic AIR data — from raw CSV ingestion through to an interactive Streamlit dashboard — running entirely inside Snowflake. No clusters, no Spark, no Airflow, no separate BI tool.

![Architecture](architecture.html)

---

## What's in the box

| Folder | Contents |
|--------|----------|
| `data/` | Pre-generated synthetic CSV files (~100K patients, ~2K providers, ~751K vaccinations) |
| `scripts/` | SQL scripts for bronze/silver/gold layers and Streamlit deployment |
| `streamlit/` | Streamlit in Snowflake dashboard app |
| `architecture.html` | Architecture diagram (open in browser) |

---

## Deployment Guide (Snowsight only — no CLI required)

Everything runs from the Snowsight web UI. No installs, no command line, no Python.

### Step 1 — Setup environment & bronze tables

1. Log into [Snowsight](https://app.snowflake.com)
2. Open **Projects > Worksheets > + Worksheet**
3. Open the file **`scripts/01_setup_bronze.sql`** and paste the contents into the worksheet
4. Click **Run All** (⌘+Shift+Enter / Ctrl+Shift+Enter)
5. This creates the warehouse, database, schemas, stage, and bronze tables
6. **Stop when you see the "STOP HERE" comment** — you need to upload files first

### Step 2 — Upload CSV files

1. In the left sidebar, navigate to **Data > Databases > AIR_DEMO > BRONZE > Stages > AIR_LANDING**
2. Click the **+ Files** button
3. Upload all 3 files from the `data/` folder:
   - `patients.csv` (~18 MB)
   - `providers.csv` (~275 KB)
   - `vaccinations.csv` (~170 MB — may take a minute)
4. Go back to your worksheet and **run the COPY INTO statements** (the part after the "STOP HERE" comment)
5. The validation query at the bottom should show:
   - `RAW_PATIENTS`: ~100,000 rows
   - `RAW_PROVIDERS`: ~2,000 rows
   - `RAW_VACCINATIONS`: ~751,000 rows

### Step 3 — Create silver Dynamic Tables

1. Open a new worksheet (or clear the current one)
2. Paste the contents of **`scripts/02_silver_dynamic_tables.sql`**
3. Click **Run All**
4. This creates 4 Dynamic Tables that automatically cleanse, deduplicate, and enrich the raw data

### Step 4 — Create gold Dynamic Tables

1. Paste the contents of **`scripts/03_gold_dynamic_tables.sql`**
2. Click **Run All**
3. This creates 5 analytics-ready Dynamic Tables (coverage rates, trends, provider performance, data quality)

### Step 5 — Trigger initial refresh

1. Paste the contents of **`scripts/05_manual_refresh.sql`**
2. Click **Run All**
3. This forces all 9 Dynamic Tables to refresh immediately (otherwise they refresh within the 1-hour target lag)
4. The verification query shows row counts for every table

### Step 6 — Deploy the Streamlit app

1. Open a new worksheet
2. Paste the contents of **`scripts/04_deploy_streamlit.sql`**
3. Run the **first part** (up to the "STOP HERE" comment) to create the Streamlit stage
4. Navigate to **Data > Databases > AIR_DEMO > APP > Stages > STREAMLIT_STAGE**
5. Click **+ Files** and upload **`streamlit/streamlit_app.py`**
6. Go back to the worksheet and run the **CREATE STREAMLIT** statement
7. Navigate to **Projects > Streamlit** — you should see **AIR_DASHBOARD**
8. Click to open it

---

## What you'll see

The Streamlit app has 5 tabs:

| Tab | What it shows |
|-----|---------------|
| **Overview** | KPI cards, vaccinations by state & age group, top 10 vaccines |
| **Coverage** | Immunisation coverage vs 95% national target, filterable by state/age/Indigenous status |
| **Trends** | Monthly dose volumes, unique patients, antigen trends, reporting lag |
| **Providers** | Provider activity, timeliness, NIP-funded rates, top providers |
| **Data Quality** | Pipeline architecture, parse failure rates, Dynamic Table refresh status |

---

## Why Snowflake (vs Databricks)

This demo is designed to highlight what you **don't** need with Snowflake:

| Snowflake | Databricks |
|-----------|------------|
| X-Small warehouse, auto-suspend 60s | Cluster provisioning + spin-up time |
| Dynamic Tables (declarative SQL) | Spark jobs + Delta Live Tables + Airflow |
| `COPY INTO` from stage | Complex ingestion pipelines |
| Streamlit in Snowflake | Separate BI tool (Tableau, Power BI, etc.) |
| ~$0.50 for this entire demo | Cluster costs + storage + compute |
| Pure SQL — no Spark, no notebooks | PySpark + notebook scheduling |

---

## Cleanup

To remove everything:

```sql
DROP STREAMLIT IF EXISTS AIR_DEMO.APP.AIR_DASHBOARD;
DROP DATABASE IF EXISTS AIR_DEMO;
DROP WAREHOUSE IF EXISTS AIR_DEMO_WH;
```

Or run **`scripts/99_cleanup.sql`**.

---

## Architecture

Open **`architecture.html`** in a browser to see the full pipeline diagram:

**Source** (3 CSV files) → **Bronze** (raw tables) → **Silver** (4 Dynamic Tables) → **Gold** (5 Dynamic Tables) → **Streamlit in Snowflake** (5-tab dashboard)

All running on a single X-Small warehouse with zero orchestration.
