import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import timedelta
import os

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Signature Fitness Dashboard",
    page_icon="📊",
    layout="wide"
)

# --------------------------------------------------
# PASSWORD CHECK
# --------------------------------------------------
def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    st.markdown(
        """
        <div style="max-width:700px;margin:80px auto 0 auto;padding:36px;border:1px solid #e7eaf0;
                    border-radius:24px;background:#ffffff;box-shadow:0 10px 30px rgba(0,0,0,.05);">
            <h1 style="margin:0 0 8px 0;">Signature Fitness Dashboard</h1>
            <p style="color:#667085;font-size:16px;margin:0 0 24px 0;">
                Private internal dashboard
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("<div style='max-width:700px;margin:0 auto;'>", unsafe_allow_html=True)
    entered_password = st.text_input("Enter password", type="password")

    if st.button("Log in", use_container_width=True):
        expected_password = st.secrets["app_password"]
        if entered_password == expected_password:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password.")

    st.markdown("</div>", unsafe_allow_html=True)
    return False


if not check_password():
    st.stop()

# --------------------------------------------------
# STYLING
# --------------------------------------------------
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.6rem;
        padding-bottom: 2rem;
        max-width: 1450px;
    }

    .sf-hero {
        background: linear-gradient(135deg, #000000 0%, #111111 65%, #1a1a1a 100%);
        padding: 34px 36px;
        border-radius: 28px;
        color: white;
        min-height: 180px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        box-shadow: 0 16px 36px rgba(0, 0, 0, 0.22);
        border: 1px solid #1f1f1f;
    }

    .sf-logo-inline {
        display: flex;
        align-items: center;
        justify-content: center;
        min-height: 220px;
        padding-right: 8px;
    }

    .sf-hero-title {
        font-size: 44px;
        font-weight: 800;
        margin-bottom: 10px;
        letter-spacing: -0.03em;
        color: #ffffff;
    }

    .sf-hero-sub {
        font-size: 16px;
        opacity: 0.92;
        margin-bottom: 0;
        color: #f3f4f6;
    }

    .sf-kpi {
        background: #ffffff;
        border: 1px solid #ececec;
        border-radius: 20px;
        padding: 18px 18px 14px 18px;
        box-shadow: 0 8px 24px rgba(0,0,0,.05);
        min-height: 115px;
    }

    .sf-kpi-label {
        color: #667085;
        font-size: 13px;
        margin-bottom: 8px;
    }

    .sf-kpi-value {
        font-size: 34px;
        font-weight: 800;
        color: #101828;
        line-height: 1.1;
    }

    .sf-card-title {
        font-size: 20px;
        font-weight: 700;
        color: #101828;
        margin-bottom: 4px;
    }

    .sf-card-sub {
        font-size: 13px;
        color: #667085;
        margin-bottom: 10px;
    }

    .sf-filter-wrap {
        background: #ffffff;
        border: 1px solid #ececec;
        border-radius: 20px;
        padding: 18px 18px 8px 18px;
        box-shadow: 0 8px 24px rgba(0,0,0,.04);
        margin-bottom: 14px;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid #e7eaf0;
        border-radius: 18px;
        overflow: hidden;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --------------------------------------------------
# BIGQUERY CLIENT
# --------------------------------------------------
@st.cache_resource
def get_bigquery_client():
    if os.path.exists("service-account.json"):
        credentials = service_account.Credentials.from_service_account_file(
            "service-account.json"
        )
    elif "gcp_service_account" in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"])
        )
    else:
        raise Exception(
            "No BigQuery credentials found. Either add service-account.json locally "
            "or add [gcp_service_account] to secrets."
        )

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    return client


client = get_bigquery_client()

# --------------------------------------------------
# QUERY HELPERS
# --------------------------------------------------
@st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
    return client.query(query).to_dataframe()

@st.cache_data(ttl=600)
def run_query_with_dates(query: str, start_date, end_date) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()

def format_ampm(dt_value):
    if pd.isna(dt_value):
        return ""
    ts = pd.to_datetime(dt_value)
    return ts.strftime("%m/%d/%Y %I:%M %p").lstrip("0").replace("/0", "/")

# --------------------------------------------------
# FULL DATA RANGE
# --------------------------------------------------
full_range_df = run_query("""
SELECT
  MIN(checkin_datetime) AS min_dt,
  MAX(checkin_datetime) AS max_dt
FROM `sigfit.checkInData.checkin_clean`
""")

full_min_dt = pd.to_datetime(full_range_df.iloc[0]["min_dt"])
full_max_dt = pd.to_datetime(full_range_df.iloc[0]["max_dt"])

full_min_date = full_min_dt.date()
full_max_date = full_max_dt.date()

# --------------------------------------------------
# HEADER
# --------------------------------------------------
logo_col, hero_col = st.columns([1.15, 5], vertical_alignment="center")

with logo_col:
    if os.path.exists("SFCheckIn/sfLogo.png"):
        st.markdown("<div style='margin-top:-25px;'>", unsafe_allow_html=True)
        st.image("SFCheckIn/sfLogo.png", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

with hero_col:
    st.markdown(
        """
        <div class="sf-hero">
            <div class="sf-hero-title">Signature Fitness Dashboard</div>
            <div class="sf-hero-sub">Member check-in intelligence</div>
        </div>
        """,
        unsafe_allow_html=True
    )

# --------------------------------------------------
# FILTERS
# --------------------------------------------------
st.markdown('<div class="sf-filter-wrap">', unsafe_allow_html=True)
st.subheader("Report Filters")

f1, f2 = st.columns([2, 3])

with f1:
    preset = st.radio(
        "Date View",
        ["Full Data", "Last 30 Days", "Last 90 Days", "Last 6 Months", "Custom Range"],
        horizontal=False
    )

if preset == "Full Data":
    selected_start = full_min_date
    selected_end = full_max_date
elif preset == "Last 30 Days":
    selected_end = full_max_date
    selected_start = max(full_min_date, full_max_date - timedelta(days=29))
elif preset == "Last 90 Days":
    selected_end = full_max_date
    selected_start = max(full_min_date, full_max_date - timedelta(days=89))
elif preset == "Last 6 Months":
    selected_end = full_max_date
    selected_start = max(full_min_date, full_max_date - timedelta(days=182))
else:
    selected_start, selected_end = f2.date_input(
        "Choose custom date range",
        value=(full_min_date, full_max_date),
        min_value=full_min_date,
        max_value=full_max_date
    )
    if selected_start > selected_end:
        st.error("Start date cannot be after end date.")
        st.stop()

with f2:
    if preset != "Custom Range":
        st.date_input(
            "Visible data range",
            value=(selected_start, selected_end),
            min_value=full_min_date,
            max_value=full_max_date,
            disabled=True
        )

st.caption(
    f"Full dataset coverage: **{format_ampm(full_min_dt)}** to **{format_ampm(full_max_dt)}**"
)
st.caption(
    f"Selected range: **{selected_start.strftime('%m/%d/%Y')}** to **{selected_end.strftime('%m/%d/%Y')}**"
)
st.markdown('</div>', unsafe_allow_html=True)

# --------------------------------------------------
# KPI QUERY
# --------------------------------------------------
kpis = run_query_with_dates("""
SELECT
  COUNT(*) AS total_checkins,
  COUNT(DISTINCT agreement_number) AS unique_members,
  ROUND(COUNT(*) / COUNT(DISTINCT agreement_number), 2) AS avg_visits_per_member,
  MIN(checkin_datetime) AS first_checkin_found,
  MAX(checkin_datetime) AS last_checkin_found
FROM `sigfit.checkInData.checkin_clean`
WHERE DATE(checkin_datetime) BETWEEN @start_date AND @end_date
""", selected_start, selected_end)

# --------------------------------------------------
# CHART/TABLE QUERIES
# --------------------------------------------------
monthly = run_query_with_dates("""
SELECT
  FORMAT_DATETIME('%Y-%m', checkin_datetime) AS month,
  COUNT(*) AS checkins
FROM `sigfit.checkInData.checkin_clean`
WHERE DATE(checkin_datetime) BETWEEN @start_date AND @end_date
GROUP BY month
ORDER BY month
""", selected_start, selected_end)

day_of_week = run_query_with_dates("""
SELECT
  CASE EXTRACT(DAYOFWEEK FROM checkin_datetime)
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS day_name,
  COUNT(*) AS checkins
FROM `sigfit.checkInData.checkin_clean`
WHERE DATE(checkin_datetime) BETWEEN @start_date AND @end_date
GROUP BY day_name
""", selected_start, selected_end)

hour_of_day = run_query_with_dates("""
SELECT
  EXTRACT(HOUR FROM checkin_datetime) AS hour_24,
  COUNT(*) AS checkins
FROM `sigfit.checkInData.checkin_clean`
WHERE DATE(checkin_datetime) BETWEEN @start_date AND @end_date
GROUP BY hour_24
ORDER BY hour_24
""", selected_start, selected_end)

gender_split = run_query_with_dates("""
SELECT
  COALESCE(gender, 'Unknown') AS gender,
  COUNT(*) AS checkins
FROM `sigfit.checkInData.checkin_clean`
WHERE DATE(checkin_datetime) BETWEEN @start_date AND @end_date
GROUP BY gender
ORDER BY checkins DESC
""", selected_start, selected_end)

engagement = run_query_with_dates("""
WITH member_visits AS (
  SELECT
    agreement_number,
    COUNT(*) AS visits
  FROM `sigfit.checkInData.checkin_clean`
  WHERE DATE(checkin_datetime) BETWEEN @start_date AND @end_date
  GROUP BY agreement_number
)
SELECT
  CASE
    WHEN visits = 1 THEN '1 Visit'
    WHEN visits BETWEEN 2 AND 4 THEN '2-4 Visits'
    WHEN visits BETWEEN 5 AND 9 THEN '5-9 Visits'
    WHEN visits BETWEEN 10 AND 19 THEN '10-19 Visits'
    ELSE '20+ Visits'
  END AS engagement_bucket,
  COUNT(*) AS member_count
FROM member_visits
GROUP BY engagement_bucket
""", selected_start, selected_end)

top_members = run_query_with_dates("""
SELECT
  agreement_number,
  ANY_VALUE(member_name) AS member_name,
  COUNT(*) AS visits,
  MIN(checkin_datetime) AS first_visit,
  MAX(checkin_datetime) AS last_visit,
  ANY_VALUE(gender) AS gender
FROM `sigfit.checkInData.checkin_clean`
WHERE DATE(checkin_datetime) BETWEEN @start_date AND @end_date
GROUP BY agreement_number
ORDER BY visits DESC
LIMIT 25
""", selected_start, selected_end)

inactive_members = run_query_with_dates("""
SELECT
  agreement_number,
  ANY_VALUE(member_name) AS member_name,
  COUNT(*) AS visits,
  MAX(checkin_datetime) AS last_visit,
  DATETIME_DIFF(CURRENT_DATETIME(), MAX(checkin_datetime), DAY) AS days_inactive,
  ANY_VALUE(gender) AS gender
FROM `sigfit.checkInData.checkin_clean`
WHERE DATE(checkin_datetime) BETWEEN @start_date AND @end_date
GROUP BY agreement_number
ORDER BY days_inactive DESC
LIMIT 25
""", selected_start, selected_end)

heatmap = run_query_with_dates("""
SELECT
  CASE EXTRACT(DAYOFWEEK FROM checkin_datetime)
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS day_name,
  EXTRACT(HOUR FROM checkin_datetime) AS hour_24,
  COUNT(*) AS checkins
FROM `sigfit.checkInData.checkin_clean`
WHERE DATE(checkin_datetime) BETWEEN @start_date AND @end_date
GROUP BY day_name, hour_24
""", selected_start, selected_end)

# --------------------------------------------------
# FORMAT DISPLAY FIELDS
# --------------------------------------------------
if not top_members.empty:
    top_members["first_visit"] = pd.to_datetime(top_members["first_visit"]).dt.strftime("%m/%d/%Y %I:%M %p")
    top_members["last_visit"] = pd.to_datetime(top_members["last_visit"]).dt.strftime("%m/%d/%Y %I:%M %p")

if not inactive_members.empty:
    inactive_members["last_visit"] = pd.to_datetime(inactive_members["last_visit"]).dt.strftime("%m/%d/%Y %I:%M %p")

# --------------------------------------------------
# KPI SECTION
# --------------------------------------------------
if not kpis.empty:
    row = kpis.iloc[0]

    total_checkins = f"{int(row['total_checkins']):,}"
    unique_members = f"{int(row['unique_members']):,}"
    avg_visits = f"{row['avg_visits_per_member']}"
    first_checkin = format_ampm(row["first_checkin_found"])
    last_checkin = format_ampm(row["last_checkin_found"])

    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        st.markdown(
            f"""
            <div class="sf-kpi">
                <div class="sf-kpi-label">Total Check-Ins</div>
                <div class="sf-kpi-value">{total_checkins}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    with c2:
        st.markdown(
            f"""
            <div class="sf-kpi">
                <div class="sf-kpi-label">Unique Members</div>
                <div class="sf-kpi-value">{unique_members}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    with c3:
        st.markdown(
            f"""
            <div class="sf-kpi">
                <div class="sf-kpi-label">Avg Visits / Member</div>
                <div class="sf-kpi-value">{avg_visits}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    with c4:
        st.markdown(
            f"""
            <div class="sf-kpi">
                <div class="sf-kpi-label">First Check-In in Range</div>
                <div class="sf-kpi-value" style="font-size:20px;">{first_checkin}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    with c5:
        st.markdown(
            f"""
            <div class="sf-kpi">
                <div class="sf-kpi-label">Last Check-In in Range</div>
                <div class="sf-kpi-value" style="font-size:20px;">{last_checkin}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

st.markdown("<div style='height:18px;'></div>", unsafe_allow_html=True)

# --------------------------------------------------
# CHARTS ROW 1
# --------------------------------------------------
left, right = st.columns([2, 1])

with left:
    st.markdown('<div class="sf-card-title">Monthly Trend</div>', unsafe_allow_html=True)
    st.markdown('<div class="sf-card-sub">Overall gym traffic over time</div>', unsafe_allow_html=True)

    fig_monthly = px.line(
        monthly,
        x="month",
        y="checkins",
        markers=True
    )
    fig_monthly.update_traces(line=dict(color="#2563eb", width=4), marker=dict(color="#2563eb", size=9))
    fig_monthly.update_layout(height=410, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_monthly, use_container_width=True)

with right:
    st.markdown('<div class="sf-card-title">Gender Split</div>', unsafe_allow_html=True)
    st.markdown('<div class="sf-card-sub">Check-in volume by gender</div>', unsafe_allow_html=True)

    fig_gender = px.pie(
        gender_split,
        names="gender",
        values="checkins",
        hole=0.5,
        color_discrete_sequence=["#2563eb", "#60a5fa", "#93c5fd", "#1e3a8a"]
    )
    fig_gender.update_layout(height=410, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_gender, use_container_width=True)

st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

# --------------------------------------------------
# CHARTS ROW 2
# --------------------------------------------------
c1, c2, c3 = st.columns(3)

with c1:
    st.markdown('<div class="sf-card-title">Day of Week</div>', unsafe_allow_html=True)
    st.markdown('<div class="sf-card-sub">Best days to plan and staff around</div>', unsafe_allow_html=True)

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    if not day_of_week.empty:
        day_of_week["sort_order"] = day_of_week["day_name"].apply(lambda x: day_order.index(x) if x in day_order else 99)
        day_of_week = day_of_week.sort_values("sort_order")

    fig_day = px.bar(day_of_week, x="day_name", y="checkins", color_discrete_sequence=["#2563eb"])
    fig_day.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_day, use_container_width=True)

with c2:
    st.markdown('<div class="sf-card-title">Hour of Day</div>', unsafe_allow_html=True)
    st.markdown('<div class="sf-card-sub">Daily traffic pressure points</div>', unsafe_allow_html=True)

    if not hour_of_day.empty:
        hour_of_day["hour_label"] = pd.to_datetime(hour_of_day["hour_24"], unit="h").dt.strftime("%I %p").str.lstrip("0")

    fig_hour = px.bar(hour_of_day, x="hour_label", y="checkins", color_discrete_sequence=["#1d4ed8"])
    fig_hour.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_hour, use_container_width=True)

with c3:
    st.markdown('<div class="sf-card-title">Engagement Buckets</div>', unsafe_allow_html=True)
    st.markdown('<div class="sf-card-sub">How frequently members are checking in</div>', unsafe_allow_html=True)

    bucket_order = ["1 Visit", "2-4 Visits", "5-9 Visits", "10-19 Visits", "20+ Visits"]
    if not engagement.empty:
        engagement["sort_order"] = engagement["engagement_bucket"].apply(lambda x: bucket_order.index(x) if x in bucket_order else 99)
        engagement = engagement.sort_values("sort_order")

    fig_eng = px.bar(engagement, x="engagement_bucket", y="member_count", color_discrete_sequence=["#2563eb"])
    fig_eng.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_eng, use_container_width=True)

st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

# --------------------------------------------------
# TABLES
# --------------------------------------------------
t1, t2 = st.columns(2)

with t1:
    st.markdown('<div class="sf-card-title">Top Members</div>', unsafe_allow_html=True)
    st.markdown('<div class="sf-card-sub">Most active members in the selected range</div>', unsafe_allow_html=True)
    st.dataframe(top_members, use_container_width=True, hide_index=True)

with t2:
    st.markdown('<div class="sf-card-title">Inactive Members</div>', unsafe_allow_html=True)
    st.markdown('<div class="sf-card-sub">Longest gap since last recorded visit</div>', unsafe_allow_html=True)
    st.dataframe(inactive_members, use_container_width=True, hide_index=True)

st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

# --------------------------------------------------
# HEATMAP
# --------------------------------------------------
st.markdown('<div class="sf-card-title">Busy Times Heatmap</div>', unsafe_allow_html=True)
st.markdown('<div class="sf-card-sub">Check-ins by day and hour</div>', unsafe_allow_html=True)

if not heatmap.empty:
    heatmap_pivot = heatmap.pivot(index="day_name", columns="hour_24", values="checkins").fillna(0)
    desired_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heatmap_pivot = heatmap_pivot.reindex(desired_order)

    fig_heat = px.imshow(
        heatmap_pivot,
        aspect="auto",
        labels=dict(x="Hour", y="Day", color="Check-Ins"),
        color_continuous_scale="Blues"
    )
    fig_heat.update_layout(height=430, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_heat, use_container_width=True)
