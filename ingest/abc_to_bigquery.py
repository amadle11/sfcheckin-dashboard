"""
Pull individual check-ins from the ABC Financial REST API and load them into BigQuery.

Usage (normally run by GitHub Actions, see .github/workflows/ingest.yml):

    python ingest/abc_to_bigquery.py                     # yesterday + today (club time)
    python ingest/abc_to_bigquery.py --start 2026-06-01 --end 2026-11-24   # backfill
    python ingest/abc_to_bigquery.py --probe             # print API field names only, load nothing
    python ingest/abc_to_bigquery.py --setup             # create the tables + view (safe to re-run)
    python ingest/abc_to_bigquery.py --members           # refresh member names (daily)
    python ingest/abc_to_bigquery.py --discover          # try every known ABC endpoint, print field names only

Environment variables:
    ABC_APP_ID, ABC_APP_KEY      ABC API credentials
    ABC_CLUB                     club number (default 8027)
    GCP_SERVICE_ACCOUNT_JSON     full service-account JSON key (text)

How loading works: the table is partitioned by check-in date, and each run REPLACES the
whole partition for each day it pulls. Re-running a day never creates duplicates.

Privacy: the repo is public, so GitHub Actions logs are public. This script only ever
logs counts and field NAMES, never member names or values.
"""

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from google.cloud import bigquery
from google.oauth2 import service_account

CLUB = os.environ.get("ABC_CLUB", "8027")
CLUB_TZ = ZoneInfo("America/New_York")
BASE_URL = f"https://api.abcfinancial.com/rest/{CLUB}"
PAGE_SIZE = 500

PROJECT = "sigfit"
DATASET = "checkInData"
RAW_TABLE = f"{PROJECT}.{DATASET}.checkins_raw"
MEMBERS_TABLE = f"{PROJECT}.{DATASET}.members"
VIEW = f"{PROJECT}.{DATASET}.checkin_clean"  # the name the dashboard already reads

SCHEMA = [
    bigquery.SchemaField("checkin_id", "STRING"),
    bigquery.SchemaField("checkin_date", "DATE"),
    bigquery.SchemaField("checkin_datetime", "DATETIME"),  # club local time
    bigquery.SchemaField("member_id", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("barcode", "STRING"),
    bigquery.SchemaField("checkin_status", "STRING"),
    bigquery.SchemaField("station", "STRING"),
    bigquery.SchemaField("checkin_club", "STRING"),
    bigquery.SchemaField("home_club", "STRING"),
    bigquery.SchemaField("raw_json", "STRING"),  # full API record, in case we need more fields later
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

# Only the member fields the dashboard needs. ABC also returns address, phone, birth
# date and gender; we deliberately don't store those.
MEMBERS_SCHEMA = [
    bigquery.SchemaField("member_id", "STRING"),
    bigquery.SchemaField("agreement_number", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("barcode", "STRING"),
    bigquery.SchemaField("member_status", "STRING"),
    bigquery.SchemaField("join_status", "STRING"),
    bigquery.SchemaField("home_club", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

# The dashboard keeps reading `checkin_clean`; this view maps the new tables onto the
# column names app.py already uses. No gender column (business rule).
VIEW_SQL = f"""
SELECT
  c.checkin_id,
  c.checkin_datetime,
  COALESCE(m.agreement_number, c.member_id) AS agreement_number,
  c.member_id,
  TRIM(CONCAT(IFNULL(m.first_name, ''), ' ', IFNULL(m.last_name, ''))) AS member_name,
  m.barcode,
  m.member_status,
  c.checkin_status,
  c.station,
  c.checkin_club,
  c.home_club
FROM `{RAW_TABLE}` c
LEFT JOIN `{MEMBERS_TABLE}` m USING (member_id)
"""


# --------------------------------------------------
# ABC API
# --------------------------------------------------
def abc_headers():
    return {
        "Accept": "application/json;charset=UTF-8",
        "app_id": os.environ["ABC_APP_ID"],
        "app_key": os.environ["ABC_APP_KEY"],
        "clubNumber": CLUB,
    }


def abc_get(path, params):
    resp = requests.get(f"{BASE_URL}{path}", headers=abc_headers(), params=params, timeout=60)
    if resp.status_code != 200:
        # Status + short body only; error bodies don't contain member data.
        raise RuntimeError(f"ABC {path} returned {resp.status_code}: {resp.text[:300]}")
    return resp.json()


DETAILS = "/clubs/checkins/details"  # one record per check-in, with exact time


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def fetch_pages(path, list_key, params):
    records, page = [], 1
    while True:
        data = abc_get(path, {**params, "page": page, "size": PAGE_SIZE})
        batch = data.get(list_key) or []
        records.extend(batch)
        if page % 10 == 0:
            print(f"  {path}: {len(records)} records so far (page {page})", flush=True)
        if not batch or not (data.get("status") or {}).get("nextPage"):
            return records
        page += 1


def fetch_day(day):
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1) - timedelta(microseconds=1)
    checkins = fetch_pages(DETAILS, "checkins", {"checkInTimestampRange": f"{fmt(start)},{fmt(end)}"})
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for c in checkins:
        ts = str(c.get("checkInTimestamp") or "")[:19]
        if not ts.startswith(day.isoformat()):
            continue
        member = c.get("member") or {}
        rows.append({
            "checkin_id": c.get("checkInId"),
            "checkin_date": day.isoformat(),
            "checkin_datetime": ts.replace("T", " "),
            "member_id": member.get("memberId"),
            "first_name": None,
            "last_name": None,
            "barcode": None,
            "checkin_status": c.get("checkInStatus"),
            "station": c.get("stationName"),
            "checkin_club": CLUB,
            "home_club": member.get("homeClub"),
            "raw_json": json.dumps(c, default=str),
            "loaded_at": loaded_at,
        })
    return rows


def fetch_members():
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for m in fetch_pages("/members", "members", {}):
        personal = m.get("personal") or {}
        rows.append({
            "member_id": m.get("memberId"),
            "agreement_number": (m.get("agreement") or {}).get("agreementNumber"),
            "first_name": personal.get("firstName"),
            "last_name": personal.get("lastName"),
            "barcode": personal.get("barcode"),
            "member_status": personal.get("memberStatus"),
            "join_status": personal.get("joinStatus"),
            "home_club": personal.get("homeClub"),
            "loaded_at": loaded_at,
        })
    return rows


# --------------------------------------------------
# BigQuery
# --------------------------------------------------
def bq_client():
    info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info.get("project_id", PROJECT))


def setup(client):
    # Datasets created while the project was a sandbox keep a 60-day default expiry even
    # after billing is turned on. Clear it so new tables never expire.
    ds = client.create_dataset(f"{PROJECT}.{DATASET}", exists_ok=True)
    ds.default_table_expiration_ms = None
    ds.default_partition_expiration_ms = None
    try:
        client.update_dataset(ds, ["default_table_expiration_ms", "default_partition_expiration_ms"])
    except Exception as e:  # needs Data Owner/Admin; Data Editor can't change dataset settings
        print(f"Could not clear dataset expiry ({type(e).__name__}). Clear it in the console: "
              "checkInData > Edit details > untick 'Enable table expiration'.")

    table = bigquery.Table(RAW_TABLE, schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(field="checkin_date", expiration_ms=None)
    table = client.create_table(table, exists_ok=True)
    table.expires = None
    client.update_table(table, ["expires"])
    client.create_table(bigquery.Table(MEMBERS_TABLE, schema=MEMBERS_SCHEMA), exists_ok=True)
    view = bigquery.Table(VIEW)
    view.view_query = VIEW_SQL
    client.delete_table(VIEW, not_found_ok=True)  # an old table/view with this name may exist
    client.create_table(view)
    exp = client.get_table(RAW_TABLE).expires
    print(f"Table {RAW_TABLE} and view {VIEW} ready. Table expiry: {exp or 'never'}")
    if exp:
        print("WARNING: table has an expiry date. Turn on billing for the project (sandbox tables expire).")


def load_day(client, day, rows):
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # replace just this day
        time_partitioning=bigquery.TimePartitioning(field="checkin_date"),
    )
    partition = f"{RAW_TABLE}${day.strftime('%Y%m%d')}"
    client.load_table_from_json(rows, partition, job_config=job_config).result()


def load_members(client, rows):
    job_config = bigquery.LoadJobConfig(
        schema=MEMBERS_SCHEMA, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    client.load_table_from_json(rows, MEMBERS_TABLE, job_config=job_config).result()


# --------------------------------------------------
# Main
# --------------------------------------------------
SAFE_VALUE_HINTS = ("count", "timestamp", "club", "rel", "type", "status", "nextpage")


def describe(value, indent="  ", key=""):
    """Print structure: field names and types. Values only for non-personal fields
    (counts, timestamps, club numbers, link types). Link URLs have ids masked."""
    if isinstance(value, dict):
        for k, v in value.items():
            if isinstance(v, (dict, list)):
                print(f"{indent}{k}: {type(v).__name__}")
                describe(v, indent + "  ", k)
            else:
                show = any(h in k.lower() for h in SAFE_VALUE_HINTS) and "country" not in k.lower()
                if k.lower() == "href" and isinstance(v, str):
                    import re
                    v, show = re.sub(r"[0-9a-fA-F-]{16,}", "<id>", v), True
                print(f"{indent}{k}: {type(v).__name__}" + (f" = {v!r}" if show else ""))
    elif isinstance(value, list):
        print(f"{indent}[{len(value)} items]")
        if value:
            describe(value[0], indent + "  ", key)


def probe(day):
    """Print response structure for one check-in (no member data)."""
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1) - timedelta(microseconds=1)
    data = abc_get(DETAILS, {"checkInTimestampRange": f"{fmt(start)},{fmt(end)}", "page": 1, "size": 1})
    describe(data)


# Endpoints to try in --discover, by ABC API group. Paths are best guesses from ABC's
# API groups (Members and Prospects, Clubs, Calendars, Employees); a 404 just means
# that path doesn't exist for this account. Each call asks for one record only.
DISCOVER = {
    "Members and Prospects": [
        "/members",
        "/members/checkins/summaries",
        "/members/personals",
        "/members/agreements",
        "/members/groups",
        "/members/children",
        "/prospects",
    ],
    "Clubs": [
        "/clubs",
        "/clubs/plans",
        "/clubs/stations",
        "/clubs/checkins/summaries",
        "/clubs/checkins/details",
        "/clubs/transactions/pos",
        "/clubs/campaigns",
    ],
    "Calendars": [
        "/calendars/events",
        "/calendars/eventtypes",
    ],
    "Employees": [
        "/employees",
        "/employees/roles",
    ],
}


def discover():
    """Call each candidate endpoint once and print status + field names (no member data)."""
    today = datetime.now(CLUB_TZ).date()
    start = datetime.combine(today - timedelta(days=7), datetime.min.time())
    end = datetime.combine(today, datetime.max.time())
    ranges = {
        "/clubs/checkins/details": {"checkInTimestampRange": f"{fmt(start)},{fmt(end)}"},
        "/clubs/checkins/summaries": {"checkInTimestampRange": f"{fmt(start)},{fmt(end)}"},
        "/members/checkins/summaries": {"checkInTimestampRange": f"{fmt(start)},{fmt(end)}"},
        "/clubs/transactions/pos": {"transactionTimestampRange": f"{fmt(start)},{fmt(end)}"},
        "/calendars/events": {"eventDateRange": f"{today - timedelta(days=7)},{today}"},
    }
    found = []
    for group, paths in DISCOVER.items():
        print(f"\n==================== {group} ====================")
        for path in paths:
            params = {**ranges.get(path, {}), "page": 1, "size": 1}
            try:
                resp = requests.get(f"{BASE_URL}{path}", headers=abc_headers(), params=params, timeout=60)
            except requests.RequestException as err:
                print(f"\n{path}: request failed ({type(err).__name__})")
                continue
            print(f"\n{path}: HTTP {resp.status_code}")
            if resp.status_code == 200:
                found.append(path)
                try:
                    describe(resp.json())
                except ValueError:
                    print("  (not JSON)")
            else:
                # Error bodies don't contain member data; keep them short.
                print(f"  {resp.text[:200]}")
    print(f"\nAvailable: {', '.join(found) or 'none'}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=date.fromisoformat)
    p.add_argument("--end", type=date.fromisoformat)
    p.add_argument("--probe", action="store_true")
    p.add_argument("--setup", action="store_true")
    p.add_argument("--members", action="store_true", help="refresh the members table (names)")
    p.add_argument("--discover", action="store_true", help="try every known ABC endpoint, print field names only")
    args = p.parse_args()

    if args.discover:
        discover()
        return

    now = datetime.now(CLUB_TZ)
    today = now.date()
    # Scheduled runs redo yesterday and today. GitHub sometimes skips scheduled runs,
    # so this makes sure late-evening check-ins from yesterday are always picked up.
    start = args.start or today - timedelta(days=1)
    end = args.end or today

    if args.probe:
        probe(start if args.start else today)
        return

    client = bq_client()
    if args.setup:
        setup(client)

    if args.members:
        rows = fetch_members()
        load_members(client, rows)
        print(f"Members table refreshed: {len(rows)} members.")

    if (args.setup or args.members) and not (args.start or args.end):
        return

    total, day = 0, start
    while day <= end:
        rows = fetch_day(day)
        if rows:
            load_day(client, day, rows)
        print(f"{day}: loaded {len(rows)} check-ins")
        total += len(rows)
        day += timedelta(days=1)
    print(f"Done. {total} check-ins loaded for {start} to {end}.")


if __name__ == "__main__":
    sys.exit(main())
