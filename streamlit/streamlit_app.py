import streamlit as st
import pandas as pd
from datetime import timedelta

st.set_page_config(
    page_title="Australian Immunisation Register",
    page_icon=":material/vaccines:",
    layout="wide",
)

conn = st.connection("snowflake")


@st.cache_data(ttl=timedelta(minutes=10))
def run_query(sql):
    return conn.query(sql)


st.title(":material/vaccines: Australian Immunisation Register")
st.caption("Powered by Snowflake Dynamic Tables — zero orchestration, always fresh")

tab_overview, tab_coverage, tab_trends, tab_providers, tab_quality = st.tabs([
    ":material/dashboard: Overview",
    ":material/shield: Coverage",
    ":material/trending_up: Trends",
    ":material/local_hospital: Providers",
    ":material/verified: Data quality",
])


# --------------------------------------------------------------------------- #
#  TAB 1: OVERVIEW
# --------------------------------------------------------------------------- #
with tab_overview:

    kpi = run_query("""
        SELECT
            (SELECT COUNT(*) FROM AIR_DEMO.SILVER.PATIENTS)           AS total_patients,
            (SELECT COUNT(*) FROM AIR_DEMO.SILVER.PROVIDERS)          AS total_providers,
            (SELECT COUNT(*) FROM AIR_DEMO.SILVER.VACCINATIONS)       AS total_vaccinations,
            (SELECT COUNT(*) FROM AIR_DEMO.BRONZE.RAW_VACCINATIONS)   AS raw_vaccinations
    """)

    total_patients = int(kpi["TOTAL_PATIENTS"].iloc[0])
    total_providers = int(kpi["TOTAL_PROVIDERS"].iloc[0])
    total_vax = int(kpi["TOTAL_VACCINATIONS"].iloc[0])
    raw_vax = int(kpi["RAW_VACCINATIONS"].iloc[0])
    dupes_removed = raw_vax - total_vax

    with st.container(horizontal=True):
        st.metric("Registered patients", f"{total_patients:,}", border=True)
        st.metric("Vaccination providers", f"{total_providers:,}", border=True)
        st.metric("Vaccination records", f"{total_vax:,}", border=True)
        st.metric("Duplicates removed", f"{dupes_removed:,}", help="Bronze to silver deduplication", border=True)

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.subheader("Vaccinations by state")
            state_data = run_query("""
                SELECT patient_state AS state, COUNT(*) AS vaccinations
                FROM AIR_DEMO.SILVER.VACCINATION_EVENTS
                GROUP BY 1 ORDER BY 2 DESC
            """)
            st.bar_chart(state_data, x="STATE", y="VACCINATIONS", horizontal=True)

    with col2:
        with st.container(border=True):
            st.subheader("Vaccinations by age group")
            age_data = run_query("""
                SELECT
                    CASE
                        WHEN DATEDIFF('month', ve.patient_dob, ve.administration_date) < 12  THEN 'Infant (<1yr)'
                        WHEN DATEDIFF('month', ve.patient_dob, ve.administration_date) < 24  THEN 'Toddler (1-2yr)'
                        WHEN DATEDIFF('month', ve.patient_dob, ve.administration_date) < 60  THEN 'Preschool (2-5yr)'
                        WHEN DATEDIFF('year',  ve.patient_dob, ve.administration_date) < 12  THEN 'Child (5-12yr)'
                        WHEN DATEDIFF('year',  ve.patient_dob, ve.administration_date) < 18  THEN 'Adolescent (12-18yr)'
                        WHEN DATEDIFF('year',  ve.patient_dob, ve.administration_date) < 65  THEN 'Adult (18-65yr)'
                        ELSE 'Senior (65+yr)'
                    END AS age_group,
                    COUNT(*) AS vaccinations
                FROM AIR_DEMO.SILVER.VACCINATION_EVENTS ve
                GROUP BY 1 ORDER BY 2 DESC
            """)
            st.bar_chart(age_data, x="AGE_GROUP", y="VACCINATIONS", horizontal=True)

    with st.container(border=True):
        st.subheader("Top 10 vaccines administered")
        vax_data = run_query("""
            SELECT antigen, vaccine_brand, COUNT(*) AS doses
            FROM AIR_DEMO.SILVER.VACCINATION_EVENTS
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10
        """)
        st.dataframe(vax_data, hide_index=True, use_container_width=True)


# --------------------------------------------------------------------------- #
#  TAB 2: COVERAGE
# --------------------------------------------------------------------------- #
with tab_coverage:

    st.subheader("Immunisation coverage by state")
    st.caption("National aspirational target: 95%")

    coverage = run_query("""
        SELECT state, is_indigenous,
               total_patients,
               patients_3plus_antigens AS covered,
               coverage_rate_pct AS coverage_pct
        FROM AIR_DEMO.GOLD.COVERAGE_BY_STATE
    """)

    with st.sidebar:
        st.header(":material/filter_list: Filters")
        states = sorted(coverage["STATE"].dropna().unique().tolist())
        selected_states = st.multiselect("State/territory", states, default=states)
        indigenous_filter = st.radio("Indigenous status", ["All", "Indigenous only", "Non-indigenous only"])

    filtered = coverage[
        coverage["STATE"].isin(selected_states)
    ]
    if indigenous_filter == "Indigenous only":
        filtered = filtered[filtered["IS_INDIGENOUS"] == True]
    elif indigenous_filter == "Non-indigenous only":
        filtered = filtered[filtered["IS_INDIGENOUS"] == False]

    state_summary = (
        filtered.groupby("STATE")
        .agg({"TOTAL_PATIENTS": "sum", "COVERED": "sum"})
        .reset_index()
    )
    state_summary["COVERAGE_PCT"] = round(state_summary["COVERED"] * 100.0 / state_summary["TOTAL_PATIENTS"].replace(0, pd.NA), 2)
    state_summary = state_summary.sort_values("COVERAGE_PCT", ascending=False)

    with st.container(horizontal=True):
        national_covered = state_summary["COVERED"].sum()
        national_total = state_summary["TOTAL_PATIENTS"].sum()
        national_pct = round(national_covered * 100.0 / national_total, 2) if national_total > 0 else 0
        gap = round(national_pct - 95.0, 2)
        st.metric("National coverage", f"{national_pct}%", f"{gap:+.1f}pp vs 95% target", border=True)
        best = state_summary.iloc[0] if len(state_summary) > 0 else None
        if best is not None:
            st.metric(f"Highest: {best['STATE']}", f"{best['COVERAGE_PCT']}%", border=True)
        worst = state_summary.iloc[-1] if len(state_summary) > 0 else None
        if worst is not None:
            st.metric(f"Lowest: {worst['STATE']}", f"{worst['COVERAGE_PCT']}%", border=True)

    with st.container(border=True):
        st.subheader("Coverage by state")
        st.bar_chart(state_summary, x="STATE", y="COVERAGE_PCT")
        st.caption("Dashed line = 95% national target")

    with st.container(border=True):
        st.subheader("Detailed coverage data")
        st.dataframe(
            filtered[["STATE", "IS_INDIGENOUS", "TOTAL_PATIENTS", "COVERED", "COVERAGE_PCT"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "COVERAGE_PCT": st.column_config.ProgressColumn(
                    "Coverage %", min_value=0, max_value=100, format="%.1f%%"
                ),
                "IS_INDIGENOUS": st.column_config.CheckboxColumn("Indigenous"),
            },
        )


# --------------------------------------------------------------------------- #
#  TAB 3: TRENDS
# --------------------------------------------------------------------------- #
with tab_trends:

    st.subheader("Monthly vaccination trends")

    trends = run_query("""
        SELECT month, state, antigen, nip_funded,
               doses_administered, unique_patients, active_providers,
               ROUND(avg_reporting_lag_days, 1) AS avg_lag_days
        FROM AIR_DEMO.GOLD.MONTHLY_VACCINATION_TRENDS
        ORDER BY month
    """)

    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.subheader("Monthly doses administered")
            monthly_total = trends.groupby("MONTH")["DOSES_ADMINISTERED"].sum().reset_index()
            st.area_chart(monthly_total, x="MONTH", y="DOSES_ADMINISTERED")

    with col2:
        with st.container(border=True):
            st.subheader("Monthly unique patients")
            monthly_patients = trends.groupby("MONTH")["UNIQUE_PATIENTS"].sum().reset_index()
            st.line_chart(monthly_patients, x="MONTH", y="UNIQUE_PATIENTS")

    with st.container(border=True):
        st.subheader("Doses by antigen over time")
        antigen_trends = trends.groupby(["MONTH", "ANTIGEN"])["DOSES_ADMINISTERED"].sum().reset_index()
        pivot = antigen_trends.pivot(index="MONTH", columns="ANTIGEN", values="DOSES_ADMINISTERED").fillna(0)
        st.line_chart(pivot)

    with st.container(border=True):
        st.subheader("Average reporting lag (days)")
        lag = trends.groupby("MONTH")["AVG_LAG_DAYS"].mean().reset_index()
        st.bar_chart(lag, x="MONTH", y="AVG_LAG_DAYS")
        st.caption("Target: vaccinations reported within 10 working days")


# --------------------------------------------------------------------------- #
#  TAB 4: PROVIDERS
# --------------------------------------------------------------------------- #
with tab_providers:

    st.subheader("Provider performance")

    providers = run_query("""
        SELECT practice_name, provider_type, state,
               total_doses_administered, unique_patients_served,
               distinct_vaccines_given, avg_reporting_lag_days,
               late_reports_count, nip_funded_pct
        FROM AIR_DEMO.GOLD.PROVIDER_PERFORMANCE
        ORDER BY total_doses_administered DESC
        LIMIT 500
    """)

    with st.container(horizontal=True):
        st.metric("Total providers", f"{len(providers):,}", border=True)
        avg_lag = round(providers["AVG_REPORTING_LAG_DAYS"].mean(), 1)
        st.metric("Avg reporting lag", f"{avg_lag} days", border=True)
        late = providers["LATE_REPORTS_COUNT"].sum()
        st.metric("Late reports (>10 days)", f"{int(late):,}", border=True)

    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.subheader("Doses by provider type")
            by_type = providers.groupby("PROVIDER_TYPE")["TOTAL_DOSES_ADMINISTERED"].sum().reset_index()
            st.bar_chart(by_type, x="PROVIDER_TYPE", y="TOTAL_DOSES_ADMINISTERED", horizontal=True)

    with col2:
        with st.container(border=True):
            st.subheader("NIP-funded vaccination rate")
            nip = providers.groupby("PROVIDER_TYPE")["NIP_FUNDED_PCT"].mean().reset_index()
            st.bar_chart(nip, x="PROVIDER_TYPE", y="NIP_FUNDED_PCT", horizontal=True)

    with st.container(border=True):
        st.subheader("Top providers by volume")
        st.dataframe(
            providers.head(50),
            hide_index=True,
            use_container_width=True,
            column_config={
                "NIP_FUNDED_PCT": st.column_config.ProgressColumn(
                    "NIP funded %", min_value=0, max_value=100, format="%.0f%%"
                ),
                "AVG_REPORTING_LAG_DAYS": st.column_config.NumberColumn("Avg lag (days)", format="%.1f"),
            },
        )


# --------------------------------------------------------------------------- #
#  TAB 5: DATA QUALITY
# --------------------------------------------------------------------------- #
with tab_quality:

    st.subheader("Data quality metrics")
    st.caption("Shows the value of the bronze-to-silver cleansing pipeline")

    dq = run_query("SELECT * FROM AIR_DEMO.GOLD.DATA_QUALITY_SUMMARY")

    with st.container(horizontal=True):
        for _, row in dq.iterrows():
            st.metric(
                f"{row['SOURCE_TABLE']} records",
                f"{int(row['TOTAL_RECORDS']):,}",
                border=True,
            )
        for _, row in dq.iterrows():
            pct = row["DATE_FAILURE_PCT"]
            st.metric(
                f"{row['SOURCE_TABLE']} date parse failures",
                f"{pct}%",
                delta=f"{int(row['DATE_PARSE_FAILURES']):,} records",
                delta_color="inverse",
                border=True,
            )

    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.subheader("Pipeline architecture")
            st.markdown("""
| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Bronze** | `COPY INTO` | Raw ingestion, preserve source fidelity |
| **Silver** | Dynamic Tables | Cleanse, conform, deduplicate |
| **Gold** | Dynamic Tables | Analytics aggregates, coverage metrics |
| **App** | Streamlit in Snowflake | Interactive dashboards |
""")
            st.caption("All transformations are declarative SQL — no Spark, no Airflow, no notebooks to schedule.")

    with col2:
        with st.container(border=True):
            st.subheader("Snowflake advantages")
            st.markdown("""
- :material/check_circle: **Zero infrastructure** — no clusters to manage
- :material/check_circle: **Dynamic Tables** — auto-refresh, declarative SQL
- :material/check_circle: **Pay per query** — XS warehouse handles 750K+ rows
- :material/check_circle: **Built-in governance** — RBAC, masking, lineage
- :material/check_circle: **Streamlit in Snowflake** — no separate app server
- :material/check_circle: **Near-zero latency** — data to dashboard in seconds
""")

    with st.container(border=True):
        st.subheader("Dynamic Table refresh status")
        dt_status = run_query("""
            SELECT name, schema_name, target_lag_sec, refresh_mode, scheduling_state
            FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
            WHERE CATALOG_NAME = 'AIR_DEMO'
            ORDER BY schema_name, name
        """)
        st.dataframe(dt_status, hide_index=True, use_container_width=True)
