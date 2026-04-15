import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Australian Immunisation Register",
    page_icon="💉",
    layout="wide",
)

session = get_active_session()


@st.cache_data(ttl=600)
def run_query(sql):
    return session.sql(sql).to_pandas()


st.title("💉 Australian Immunisation Register")
st.caption("Powered by Snowflake Dynamic Tables — zero orchestration, always fresh")

tab_overview, tab_coverage, tab_trends, tab_providers, tab_quality = st.tabs([
    "📊 Overview",
    "🛡️ Coverage",
    "📈 Trends",
    "🏥 Providers",
    "✅ Data quality",
])


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

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Registered patients", f"{total_patients:,}")
    k2.metric("Vaccination providers", f"{total_providers:,}")
    k3.metric("Vaccination records", f"{total_vax:,}")
    k4.metric("Duplicates removed", f"{dupes_removed:,}", help="Bronze to silver deduplication")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Vaccinations by state")
        state_data = run_query("""
            SELECT patient_state AS state, COUNT(*) AS vaccinations
            FROM AIR_DEMO.SILVER.VACCINATION_EVENTS
            GROUP BY 1 ORDER BY 2 DESC
        """)
        st.bar_chart(state_data.set_index("STATE"))

    with col2:
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
        st.bar_chart(age_data.set_index("AGE_GROUP"))

    st.markdown("---")

    st.subheader("Top 10 vaccines administered")
    vax_data = run_query("""
        SELECT antigen, vaccine_brand, COUNT(*) AS doses
        FROM AIR_DEMO.SILVER.VACCINATION_EVENTS
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10
    """)
    st.dataframe(vax_data, use_container_width=True)


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
        st.header("🔍 Filters")
        states = sorted(coverage["STATE"].dropna().unique().tolist())
        selected_states = st.multiselect("State/territory", states, default=states)
        indigenous_filter = st.radio("Indigenous status", ["All", "Indigenous only", "Non-indigenous only"])

    filtered = coverage[coverage["STATE"].isin(selected_states)]
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

    m1, m2, m3 = st.columns(3)
    national_covered = state_summary["COVERED"].sum()
    national_total = state_summary["TOTAL_PATIENTS"].sum()
    national_pct = round(national_covered * 100.0 / national_total, 2) if national_total > 0 else 0
    gap = round(national_pct - 95.0, 2)
    m1.metric("National coverage", f"{national_pct}%", f"{gap:+.1f}pp vs 95% target")
    best = state_summary.iloc[0] if len(state_summary) > 0 else None
    if best is not None:
        m2.metric(f"Highest: {best['STATE']}", f"{best['COVERAGE_PCT']}%")
    worst = state_summary.iloc[-1] if len(state_summary) > 0 else None
    if worst is not None:
        m3.metric(f"Lowest: {worst['STATE']}", f"{worst['COVERAGE_PCT']}%")

    st.markdown("---")

    st.subheader("Coverage by state")
    st.bar_chart(state_summary.set_index("STATE")["COVERAGE_PCT"])
    st.caption("Target: 95% national coverage")

    st.markdown("---")

    st.subheader("Detailed coverage data")
    st.dataframe(filtered[["STATE", "IS_INDIGENOUS", "TOTAL_PATIENTS", "COVERED", "COVERAGE_PCT"]], use_container_width=True)


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
        st.subheader("Monthly doses administered")
        monthly_total = trends.groupby("MONTH")["DOSES_ADMINISTERED"].sum().reset_index()
        st.area_chart(monthly_total.set_index("MONTH"))

    with col2:
        st.subheader("Monthly unique patients")
        monthly_patients = trends.groupby("MONTH")["UNIQUE_PATIENTS"].sum().reset_index()
        st.line_chart(monthly_patients.set_index("MONTH"))

    st.markdown("---")

    st.subheader("Doses by antigen over time")
    antigen_trends = trends.groupby(["MONTH", "ANTIGEN"])["DOSES_ADMINISTERED"].sum().reset_index()
    pivot = antigen_trends.pivot(index="MONTH", columns="ANTIGEN", values="DOSES_ADMINISTERED").fillna(0)
    st.line_chart(pivot)

    st.markdown("---")

    st.subheader("Average reporting lag (days)")
    lag = trends.groupby("MONTH")["AVG_LAG_DAYS"].mean().reset_index()
    st.bar_chart(lag.set_index("MONTH"))
    st.caption("Target: vaccinations reported within 10 working days")


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

    p1, p2, p3 = st.columns(3)
    p1.metric("Total providers", f"{len(providers):,}")
    avg_lag = round(providers["AVG_REPORTING_LAG_DAYS"].mean(), 1)
    p2.metric("Avg reporting lag", f"{avg_lag} days")
    late = providers["LATE_REPORTS_COUNT"].sum()
    p3.metric("Late reports (>10 days)", f"{int(late):,}")

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Doses by provider type")
        by_type = providers.groupby("PROVIDER_TYPE")["TOTAL_DOSES_ADMINISTERED"].sum().reset_index()
        st.bar_chart(by_type.set_index("PROVIDER_TYPE"))

    with col2:
        st.subheader("NIP-funded vaccination rate")
        nip = providers.groupby("PROVIDER_TYPE")["NIP_FUNDED_PCT"].mean().reset_index()
        st.bar_chart(nip.set_index("PROVIDER_TYPE"))

    st.markdown("---")

    st.subheader("Top providers by volume")
    st.dataframe(providers.head(50), use_container_width=True)


with tab_quality:

    st.subheader("Data quality metrics")
    st.caption("Shows the value of the bronze-to-silver cleansing pipeline")

    dq = run_query("SELECT * FROM AIR_DEMO.GOLD.DATA_QUALITY_SUMMARY")

    d1, d2, d3, d4 = st.columns(4)
    for i, (_, row) in enumerate(dq.iterrows()):
        [d1, d2][i].metric(
            f"{row['SOURCE_TABLE']} records",
            f"{int(row['TOTAL_RECORDS']):,}",
        )
    for i, (_, row) in enumerate(dq.iterrows()):
        pct = row["DATE_FAILURE_PCT"]
        [d3, d4][i].metric(
            f"{row['SOURCE_TABLE']} date failures",
            f"{pct}%",
            delta=f"{int(row['DATE_PARSE_FAILURES']):,} records",
            delta_color="inverse",
        )

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
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
        st.subheader("Snowflake advantages")
        st.markdown("""
- ✅ **Zero infrastructure** — no clusters to manage
- ✅ **Dynamic Tables** — auto-refresh, declarative SQL
- ✅ **Pay per query** — XS warehouse handles 750K+ rows
- ✅ **Built-in governance** — RBAC, masking, lineage
- ✅ **Streamlit in Snowflake** — no separate app server
- ✅ **Near-zero latency** — data to dashboard in seconds
""")

    st.markdown("---")

    st.subheader("Dynamic Table refresh status")
    session.sql("SHOW DYNAMIC TABLES IN DATABASE AIR_DEMO").collect()
    dt_status = session.sql("""
        SELECT "name", "schema_name", "target_lag", "refresh_mode", "scheduling_state", "rows"
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        ORDER BY "schema_name", "name"
    """).to_pandas()
    st.dataframe(dt_status, use_container_width=True)
