"""
StreamLake - OTT Analytics Dashboard (Streamlit)

Interactive ad-hoc analytics over the Snowflake Gold schema.
Complement to the Grafana BI dashboard: Grafana is always-on executive BI,
Streamlit is interactive exploration with filters, drill-downs, and CSV downloads.

Run locally:
    streamlit run dashboard/analytics.py

Deployed to Streamlit Community Cloud at <URL filled in after deploy>.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

# ---- Page config ----
st.set_page_config(
    page_title="StreamLake OTT Analytics",
    page_icon="ðŸŽ¬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---- Snowflake connection ----
# Uses st.connection("snowflake") which reads .streamlit/secrets.toml locally
# or Streamlit Cloud's secrets UI in deployment.
conn = st.connection("snowflake")


@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    """Run a Snowflake query and cache for 5 minutes."""
    return conn.query(sql, ttl=300)


# ---- Header ----
st.title("StreamLake OTT Analytics")
st.caption(
    "Interactive analytics over the Snowflake Gold layer. "
    "Data refreshed every 5 minutes."
)

# ---- Section 1: KPI cards ----
st.header("Overview")

kpi_sql = """
SELECT
    COUNT(*) AS TOTAL_SESSIONS,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
    SUM(EVENT_COUNT) AS TOTAL_EVENTS,
    ROUND(AVG(MAX_WATCH_SECONDS), 1) AS AVG_WATCH_SEC
FROM GOLD.USER_SESSION_SUMMARY
"""
kpi = run_query(kpi_sql).iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Sessions", f"{int(kpi.TOTAL_SESSIONS):,}")
c2.metric("Unique Users", f"{int(kpi.UNIQUE_USERS):,}")
c3.metric("Total Events", f"{int(kpi.TOTAL_EVENTS):,}")
c4.metric("Avg Watch (sec)", f"{kpi.AVG_WATCH_SEC:.1f}")

st.divider()

# ---- Section 2: Watch time by country (with filter) ----
st.header("Watch Time by Country")

country_df = run_query("""
    SELECT
        COUNTRY_CODE,
        SUM(TOTAL_WATCH_SECONDS) AS WATCH_TIME
    FROM GOLD.DAILY_WATCH_TIME_BY_COUNTRY
    GROUP BY COUNTRY_CODE
    ORDER BY WATCH_TIME DESC
""")

available_countries = country_df["COUNTRY_CODE"].tolist()
selected = st.multiselect(
    "Filter countries (leave empty to show all)",
    options=available_countries,
    default=[],
    key="country_filter",
)

display_df = country_df if not selected else country_df[country_df["COUNTRY_CODE"].isin(selected)]

fig_country = px.bar(
    display_df,
    x="COUNTRY_CODE",
    y="WATCH_TIME",
    title=None,
    labels={"COUNTRY_CODE": "Country", "WATCH_TIME": "Watch Time (seconds)"},
    color_discrete_sequence=["#2ecc71"],
    text="WATCH_TIME",
)
fig_country.update_traces(textposition="outside")
fig_country.update_layout(height=400, showlegend=False)
st.plotly_chart(fig_country, use_container_width=True)

st.divider()

# ---- Section 3: Content leaderboard ----
st.header("Top Content")

top_n = st.slider("How many top titles to show?", min_value=5, max_value=50, value=10, step=5)

content_df = run_query(f"""
  SELECT
        CONTENT_ID,
        SUM(TOTAL_WATCH_SECONDS) AS WATCH_TIME
    FROM GOLD.TOP_CONTENT_BY_WATCH_TIME
    GROUP BY CONTENT_ID
    ORDER BY WATCH_TIME DESC
    LIMIT {top_n}
""")

fig_content = px.bar(
    content_df,
    x="WATCH_TIME",
    y="CONTENT_ID",
    orientation="h",
    labels={"WATCH_TIME": "Watch Time (seconds)", "CONTENT_ID": "Content ID"},
    color_discrete_sequence=["#3498db"],
    text="WATCH_TIME",
)
fig_content.update_traces(textposition="outside")
fig_content.update_layout(height=max(400, top_n * 30), showlegend=False, yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig_content, use_container_width=True)

st.divider()

# ---- Section 4: Device mix ----
st.header("Device Mix")

device_df = run_query("""
    SELECT
        DEVICE_TYPE,
        SUM(EVENT_COUNT) AS EVENTS
    FROM GOLD.DAILY_DEVICE_MIX
    GROUP BY DEVICE_TYPE
    ORDER BY EVENTS DESC
""")

c_left, c_right = st.columns([2, 1])

with c_left:
    fig_device = px.pie(
        device_df,
        values="EVENTS",
        names="DEVICE_TYPE",
        title=None,
        hole=0.35,
    )
    fig_device.update_layout(height=400)
    st.plotly_chart(fig_device, use_container_width=True)

with c_right:
    st.subheader("Breakdown")
    st.dataframe(
        device_df.rename(columns={"DEVICE_TYPE": "Device", "EVENTS": "Events"}),
        hide_index=True,
        use_container_width=True,
    )

st.divider()

# ---- Section 5: Session explorer ----
st.header("Session Explorer")
st.caption("Drill into individual sessions with search and CSV download.")

sessions_df = run_query("""
    SELECT
        SESSION_ID,
        USER_ID,
        COUNTRY_CODE,
        DEVICE_TYPE,
        CONTENT_TYPE,
        EVENT_COUNT,
        MAX_WATCH_SECONDS,
        SESSION_START,
        SESSION_END
    FROM GOLD.USER_SESSION_SUMMARY
    ORDER BY SESSION_START DESC
""")

search = st.text_input("Search by user ID or session ID", "")
if search:
    mask = (
        sessions_df["USER_ID"].str.contains(search, case=False, na=False)
        | sessions_df["SESSION_ID"].str.contains(search, case=False, na=False)
    )
    filtered = sessions_df[mask]
else:
    filtered = sessions_df

st.write(f"**{len(filtered):,}** sessions matched.")

st.dataframe(filtered, hide_index=True, use_container_width=True, height=400)

st.download_button(
    label="Download filtered sessions as CSV",
    data=filtered.to_csv(index=False).encode("utf-8"),
    file_name="streamlake_sessions.csv",
    mime="text/csv",
)

# ---- Footer ----
st.divider()
st.caption(
    "StreamLake â€” Real-Time Streaming Data Lakehouse Â· "
    "Kafka â†’ Spark â†’ Delta Lake â†’ Snowflake â†’ Streamlit Â· "
    "[github.com/SatvikSPandey/streamlake](https://github.com/SatvikSPandey/streamlake)"
)