import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import timedelta, timezone
from zoneinfo import ZoneInfo
import os

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Signature Fitness Member Check-In Dashboard",
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
            <h1 style="margin:0 0 8px 0;">Signature Fitness Member Check-In Dashboard</h1>
            <p style="color:#667085;font-size:16px;margin:0 0 24px 0;">
                Private internal dashboard
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("<div style='max-width:700px;margin:0 auto;'>", unsafe_allow_html=True)
    entered_password = st.text_input("Enter password", type="password")

    if st.button("Log in", width="stretch"):
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

@st.cache_data(ttl=600)
def run_query_with_cutoff(query: str, cutoff_mmdd: str) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("cutoff", "STRING", cutoff_mmdd)]
    )
    return client.query(query, job_config=job_config).to_dataframe()

@st.cache_data(ttl=600)
def run_query_params(query: str, params: tuple) -> pd.DataFrame:
    """params: tuple of (name, type, value), e.g. ("end_date", "DATE", date)."""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(n, t, v) for n, t, v in params]
    )
    return client.query(query, job_config=job_config).to_dataframe()

CLUB_TZ = ZoneInfo("America/New_York")
MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

def pct_change(new, old):
    if not old:
        return None
    return f"{(new - old) / old * 100:+.1f}%"

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

if full_range_df.empty or pd.isna(full_range_df.iloc[0]["min_dt"]):
    st.warning("No check-in data in BigQuery yet. The ABC import may not have run.")
    st.stop()

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
        st.image("SFCheckIn/sfLogo.png", width="stretch")
        st.markdown("</div>", unsafe_allow_html=True)

with hero_col:
    st.markdown(
        """
        <div class="sf-hero">
            <div class="sf-hero-title">Signature Fitness Member Check-In Dashboard</div>
            <div class="sf-hero-sub">Member check-in intelligence</div>
        </div>
        """,
        unsafe_allow_html=True
    )

# --------------------------------------------------
# LAST UPDATED + REFRESH
# --------------------------------------------------
last_loaded = run_query("SELECT MAX(loaded_at) AS last_loaded FROM `sigfit.checkInData.checkins_raw`")
last_loaded_ts = pd.to_datetime(last_loaded.iloc[0]["last_loaded"]) if not last_loaded.empty else None

u1, u2 = st.columns([5, 1], vertical_alignment="center")
with u1:
    if last_loaded_ts is not None and not pd.isna(last_loaded_ts):
        if last_loaded_ts.tzinfo is None:
            last_loaded_ts = last_loaded_ts.tz_localize("UTC")
        local = last_loaded_ts.tz_convert(CLUB_TZ)
        st.caption(
            f"**Last updated from ABC:** {format_ampm(local.tz_localize(None))} ET "
            f"· **Latest check-in:** {format_ampm(full_max_dt)} · New data arrives about every hour"
        )
with u2:
    if st.button("🔄 Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()

# --------------------------------------------------
# YEAR OVER YEAR (always full years; not affected by Report Filters)
# --------------------------------------------------
st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)
st.subheader("Year over Year")

# Compare through the last full day, so this year's partial day isn't compared to
# a full day last year.
cutoff_date = full_max_date - timedelta(days=1)
cutoff_mmdd = cutoff_date.strftime("%m%d")
st.caption(
    f"Each year from Jan 1 through **{cutoff_date.strftime('%b %-d')}**, so the comparison is like for like. "
    "This section always uses full years and ignores the Report Filters below."
)

ytd = run_query_with_cutoff("""
SELECT
  EXTRACT(YEAR FROM checkin_datetime) AS year,
  COUNT(*) AS checkins,
  COUNT(DISTINCT agreement_number) AS unique_members,
  COUNT(DISTINCT DATE(checkin_datetime)) AS days_with_checkins
FROM `sigfit.checkInData.checkin_clean`
WHERE FORMAT_DATETIME('%m%d', checkin_datetime) <= @cutoff
GROUP BY year
ORDER BY year
""", cutoff_mmdd)

monthly_by_year = run_query("""
SELECT
  EXTRACT(YEAR FROM checkin_datetime) AS year,
  EXTRACT(MONTH FROM checkin_datetime) AS month_num,
  COUNT(*) AS checkins,
  COUNT(DISTINCT agreement_number) AS unique_members,
  COUNT(DISTINCT DATE(checkin_datetime)) AS days_with_checkins
FROM `sigfit.checkInData.checkin_clean`
GROUP BY year, month_num
ORDER BY year, month_num
""")

if len(ytd) >= 2:
    cur, prev = ytd.iloc[-1], ytd.iloc[-2]
    cy, py = int(cur["year"]), int(prev["year"])
    cur_avg = cur["checkins"] / max(cur["days_with_checkins"], 1)
    prev_avg = prev["checkins"] / max(prev["days_with_checkins"], 1)
    k1, k2, k3 = st.columns(3)
    k1.metric(f"Check-ins, {cy} to date", f"{int(cur['checkins']):,}",
              pct_change(cur["checkins"], prev["checkins"]),
              help=f"{py} same period: {int(prev['checkins']):,}")
    k2.metric(f"Unique members, {cy} to date", f"{int(cur['unique_members']):,}",
              pct_change(cur["unique_members"], prev["unique_members"]),
              help=f"{py} same period: {int(prev['unique_members']):,}")
    k3.metric(f"Avg check-ins per day, {cy}", f"{cur_avg:,.0f}",
              pct_change(cur_avg, prev_avg),
              help=f"{py} same period: {prev_avg:,.0f}")
    st.caption(f"Arrows compare with {py} over the same dates.")
elif len(ytd) == 1:
    st.info("Only one year of data so far. Comparisons appear once there are two years.")

if not monthly_by_year.empty:
    metric_label = st.radio(
        "Compare by",
        ["Total check-ins", "Avg check-ins per day", "Unique members"],
        index=1,
        horizontal=True,
        help="Avg per day is the default because the current month isn't finished yet, so its total looks low.",
    )
    mby = monthly_by_year.copy()
    mby["Avg check-ins per day"] = (mby["checkins"] / mby["days_with_checkins"].clip(lower=1)).round(0)
    mby = mby.rename(columns={"checkins": "Total check-ins", "unique_members": "Unique members"})
    mby["Month"] = mby["month_num"].astype(int).apply(lambda m: MONTH_NAMES[m - 1])
    mby["Year"] = mby["year"].astype(int).astype(str)

    fig_yoy = px.line(
        mby, x="Month", y=metric_label, color="Year", markers=True,
        category_orders={"Month": MONTH_NAMES},
        color_discrete_sequence=["#94a3b8", "#2563eb", "#111111", "#60a5fa", "#1e3a8a"],
    )
    fig_yoy.update_traces(line=dict(width=4), marker=dict(size=8))
    fig_yoy.update_layout(height=400, margin=dict(l=10, r=10, t=10, b=10), legend_title_text="")
    st.plotly_chart(fig_yoy, width="stretch")

    table = mby.pivot(index="Month", columns="Year", values=metric_label).reindex(MONTH_NAMES)
    table = table.dropna(how="all")
    years = list(table.columns)
    if len(years) >= 2:
        a, b = years[-2], years[-1]
        table[f"Change {a}→{b}"] = ((table[b] - table[a]) / table[a] * 100).round(1).map(
            lambda v: "" if pd.isna(v) else f"{v:+.1f}%"
        )
    for y in years:
        table[y] = table[y].map(lambda v: "" if pd.isna(v) else f"{int(v):,}")
    current_month = MONTH_NAMES[full_max_date.month - 1]
    st.caption(f"{current_month} {full_max_date.year} is still in progress.")
    st.dataframe(table, width="stretch")

st.markdown("<div style='height:18px;'></div>", unsafe_allow_html=True)

# --------------------------------------------------
# FILTERS
# --------------------------------------------------
st.markdown('<div class="sf-filter-wrap">', unsafe_allow_html=True)
st.subheader("Report Filters")

f1, f2 = st.columns([2, 3])

with f1:
    preset = st.radio(
        "Date View",
        ["Full Data", "This Year", "Last Year", "Last 30 Days", "Last 90 Days", "Last 6 Months", "Custom Range"],
        horizontal=False
    )

if preset == "Full Data":
    selected_start = full_min_date
    selected_end = full_max_date
elif preset == "This Year":
    selected_end = full_max_date
    selected_start = max(full_min_date, full_max_date.replace(month=1, day=1))
elif preset == "Last Year":
    last_year = full_max_date.year - 1
    selected_start = max(full_min_date, full_max_date.replace(year=last_year, month=1, day=1))
    selected_end = min(full_max_date, full_max_date.replace(year=last_year, month=12, day=31))
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
  MAX(checkin_datetime) AS last_visit
FROM `sigfit.checkInData.checkin_clean`
WHERE DATE(checkin_datetime) BETWEEN @start_date AND @end_date
GROUP BY agreement_number
ORDER BY visits DESC
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
st.plotly_chart(fig_monthly, width="stretch")

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
    st.plotly_chart(fig_day, width="stretch")

with c2:
    st.markdown('<div class="sf-card-title">Hour of Day</div>', unsafe_allow_html=True)
    st.markdown('<div class="sf-card-sub">Daily traffic pressure points</div>', unsafe_allow_html=True)

    if not hour_of_day.empty:
        hour_of_day["hour_label"] = pd.to_datetime(hour_of_day["hour_24"], unit="h").dt.strftime("%I %p").str.lstrip("0")

    fig_hour = px.bar(hour_of_day, x="hour_label", y="checkins", color_discrete_sequence=["#1d4ed8"])
    fig_hour.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_hour, width="stretch")

with c3:
    st.markdown('<div class="sf-card-title">Engagement Buckets</div>', unsafe_allow_html=True)
    st.markdown('<div class="sf-card-sub">How frequently members are checking in</div>', unsafe_allow_html=True)

    bucket_order = ["1 Visit", "2-4 Visits", "5-9 Visits", "10-19 Visits", "20+ Visits"]
    if not engagement.empty:
        engagement["sort_order"] = engagement["engagement_bucket"].apply(lambda x: bucket_order.index(x) if x in bucket_order else 99)
        engagement = engagement.sort_values("sort_order")

    fig_eng = px.bar(engagement, x="engagement_bucket", y="member_count", color_discrete_sequence=["#2563eb"])
    fig_eng.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_eng, width="stretch")

st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

# --------------------------------------------------
# TABLES
# --------------------------------------------------
st.markdown('<div class="sf-card-title">Top Members</div>', unsafe_allow_html=True)
st.markdown('<div class="sf-card-sub">Most active members in the selected range</div>', unsafe_allow_html=True)
st.dataframe(top_members, width="stretch", hide_index=True)

st.markdown("<div style='height:18px;'></div>", unsafe_allow_html=True)

# --------------------------------------------------
# GHOST MEMBERS
# --------------------------------------------------
st.markdown('<div class="sf-card-title">Ghost Members</div>', unsafe_allow_html=True)
st.markdown(
    f'<div class="sf-card-sub">Active members with no check-in in the days leading up to '
    f'{selected_end.strftime("%m/%d/%Y")} (the end of the selected range)</div>',
    unsafe_allow_html=True,
)

ghost_days = st.selectbox("No check-in for at least", [14, 30, 60, 90], index=1,
                          format_func=lambda d: f"{d} days")

ghosts = run_query_params("""
WITH visits AS (
  SELECT
    member_id,
    MAX(checkin_datetime) AS last_visit,
    COUNTIF(DATE(checkin_datetime) > DATE_SUB(@end_date, INTERVAL @days + 90 DAY)
            AND DATE(checkin_datetime) <= DATE_SUB(@end_date, INTERVAL @days DAY)) AS visits_prior_90
  FROM `sigfit.checkInData.checkins_raw`
  WHERE DATE(checkin_datetime) <= @end_date
  GROUP BY member_id
)
SELECT
  m.agreement_number,
  TRIM(CONCAT(IFNULL(m.first_name, ''), ' ', IFNULL(m.last_name, ''))) AS member_name,
  v.last_visit,
  DATE_DIFF(@end_date, DATE(v.last_visit), DAY) AS days_since_last_visit,
  IFNULL(v.visits_prior_90, 0) AS visits_in_90_days_before
FROM `sigfit.checkInData.members` m
LEFT JOIN visits v USING (member_id)
WHERE LOWER(IFNULL(m.member_status, '')) = 'active'
  AND LOWER(IFNULL(m.join_status, '')) != 'prospect'
  AND (v.last_visit IS NULL OR DATE(v.last_visit) <= DATE_SUB(@end_date, INTERVAL @days DAY))
ORDER BY visits_in_90_days_before DESC, v.last_visit DESC
""", (("end_date", "DATE", selected_end), ("days", "INT64", ghost_days)))

if ghosts.empty:
    st.info("No ghost members found for this date range.")
else:
    never = ghosts["last_visit"].isna()
    g1, g2, g3 = st.columns(3)
    g1.metric("Ghost members", f"{len(ghosts):,}")
    g2.metric("Were regulars before going quiet", f"{int((ghosts['visits_in_90_days_before'] >= 4).sum()):,}",
              help="4+ check-ins in the 90 days before they stopped coming. Best people to reach out to.")
    g3.metric("No check-ins at all since Jan 2025", f"{int(never.sum()):,}")
    show = ghosts.copy()
    show["last_visit"] = pd.to_datetime(show["last_visit"]).dt.strftime("%m/%d/%Y %I:%M %p").fillna("None since Jan 2025")
    show["days_since_last_visit"] = show["days_since_last_visit"].astype("Int64")
    show = show.rename(columns={
        "agreement_number": "Agreement #", "member_name": "Member", "last_visit": "Last visit",
        "days_since_last_visit": "Days since last visit", "visits_in_90_days_before": "Visits in prior 90 days",
    })
    st.caption("Sorted with recent regulars first. Use the download icon on the table to export the list.")
    st.dataframe(show, width="stretch", hide_index=True, height=400)

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
    st.plotly_chart(fig_heat, width="stretch")

st.markdown("<div style='height:18px;'></div>", unsafe_allow_html=True)

# --------------------------------------------------
# MEMBER LOOKUP
# --------------------------------------------------
st.markdown('<div class="sf-card-title">Member Lookup</div>', unsafe_allow_html=True)
st.markdown('<div class="sf-card-sub">Search by name, agreement number or barcode. Shows all visits since Jan 2025.</div>',
            unsafe_allow_html=True)

search = st.text_input("Search member", placeholder="e.g. Smith, or an agreement number").strip()

if len(search) >= 2:
    matches = run_query_params("""
    SELECT
      member_id,
      agreement_number,
      TRIM(CONCAT(IFNULL(first_name, ''), ' ', IFNULL(last_name, ''))) AS member_name,
      member_status
    FROM `sigfit.checkInData.members`
    WHERE LOWER(CONCAT(IFNULL(first_name, ''), ' ', IFNULL(last_name, ''))) LIKE CONCAT('%', LOWER(@q), '%')
       OR agreement_number = @q
       OR barcode = @q
    ORDER BY LOWER(IFNULL(member_status, '')) = 'active' DESC, member_name
    LIMIT 50
    """, (("q", "STRING", search),))

    if matches.empty:
        st.info("No members found.")
    else:
        labels = [
            f"{r.member_name} · #{r.agreement_number or '—'} · {r.member_status or 'unknown'}"
            for r in matches.itertuples()
        ]
        pick = st.selectbox(f"{len(matches)} match(es){' (showing first 50)' if len(matches) == 50 else ''}",
                            range(len(labels)), format_func=lambda i: labels[i])
        member_id = matches.iloc[pick]["member_id"]

        visits = run_query_params("""
        SELECT checkin_datetime
        FROM `sigfit.checkInData.checkins_raw`
        WHERE member_id = @member_id
        ORDER BY checkin_datetime DESC
        """, (("member_id", "STRING", member_id),))

        if visits.empty:
            st.info("No check-ins for this member since Jan 2025.")
        else:
            vt = pd.to_datetime(visits["checkin_datetime"])
            in_range = vt[(vt.dt.date >= selected_start) & (vt.dt.date <= selected_end)]
            last = vt.max()
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Visits since Jan 2025", f"{len(vt):,}")
            m2.metric("Visits in selected range", f"{len(in_range):,}")
            m3.metric("Last visit", last.strftime("%m/%d/%Y"),
                      help=f"{(full_max_date - last.date()).days} days before the latest data")
            m4.metric("Usual time", vt.dt.hour.mode().map(
                lambda h: pd.Timestamp(2000, 1, 1, int(h)).strftime("%I %p").lstrip("0")).iloc[0])

            by_month = vt.dt.to_period("M").value_counts().sort_index()
            by_month = by_month.reindex(pd.period_range(by_month.index.min(), pd.Period(full_max_date, "M")),
                                        fill_value=0)
            fig_member = px.bar(x=by_month.index.strftime("%b %Y"), y=by_month.values,
                                labels={"x": "", "y": "Check-ins"}, color_discrete_sequence=["#2563eb"])
            fig_member.update_layout(height=300, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig_member, width="stretch")

            recent = pd.DataFrame({
                "Date": vt.dt.strftime("%m/%d/%Y"),
                "Day": vt.dt.strftime("%A"),
                "Time": vt.dt.strftime("%I:%M %p").str.lstrip("0"),
            })
            st.dataframe(recent.head(50), width="stretch", hide_index=True, height=300)
            st.caption("Most recent 50 visits.")
