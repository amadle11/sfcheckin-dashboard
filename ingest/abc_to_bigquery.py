"""
Pull individual check-ins from the ABC Financial REST API and load them into BigQuery.

Usage (normally run by GitHub Actions, see .github/workflows/ingest.yml):

    python ingest/abc_to_bigquery.py                     # today (club time)
    python ingest/abc_to_bigquery.py --start 2026-06-01 --end 2026-11-24   # backfill
    python ingest/abc_to_bigquery.py --probe             # print API field names only, load nothing
    python ingest/abc_to_bigquery.py --setup             # create the table + view (safe to re-run)

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
import hashlib
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

# The dashboard keeps reading `checkin_clean`; this view maps the new table onto the
# column names app.py already uses. No gender column (business rule).
VIEW_SQL = f"""
SELECT
  checkin_id,
  checkin_datetime,
  member_id AS agreement_number,
  TRIM(CONCAT(IFNULL(first_name, ''), ' ', IFNULL(last_name, ''))) AS member_name,
  barcode,
  checkin_status,
  station,
  checkin_club,
  home_club
FROM `{RAW_TABLE}`
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


SUMMARIES = "/members/checkins/summaries"
# ABC's summaries endpoint returns only a check-in COUNT per member for a time range,
# not the time of each visit. So we ask about one short slot at a time: a member who
# shows up in the 6:15-6:30 slot checked in during that slot. 15 minutes is plenty for
# hour-of-day charts, the heatmap and a rolling 90-minute crowd meter.
SLOT_MINUTES = int(os.environ.get("SLOT_MINUTES", "15"))


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def fetch_summaries(start, end):
    """All member summaries for check-ins between start and end (club local time)."""
    ts_range = f"{fmt(start)},{fmt(end)}"
    records, page = [], 1
    while True:
        data = abc_get(SUMMARIES, {"checkInTimestampRange": ts_range, "page": page, "size": PAGE_SIZE})
        batch = data.get("members") or []
        records.extend(batch)
        if not batch or not (data.get("status") or {}).get("nextPage"):
            return records
        page += 1


def visit_count(member):
    """checkInCounts looks like {"checkInCount": [{"count": "1", "club": "8027"}]}."""
    counts = (member.get("checkInCounts") or {}).get("checkInCount") or []
    if isinstance(counts, dict):
        counts = [counts]
    return sum(int(c.get("count") or 0) for c in counts if str(c.get("club", CLUB)) == CLUB)


def fetch_day(day):
    loaded_at = datetime.now(timezone.utc).isoformat()
    now = datetime.now(CLUB_TZ).replace(tzinfo=None)
    slot = datetime.combine(day, datetime.min.time())
    day_end = slot + timedelta(days=1)
    rows = []
    while slot < day_end and slot <= now:
        slot_end = slot + timedelta(minutes=SLOT_MINUTES) - timedelta(microseconds=1)
        for m in fetch_summaries(slot, slot_end):
            member_id = m.get("memberId")
            for n in range(visit_count(m)):
                rows.append({
                    "checkin_id": hashlib.sha1(f"{member_id}|{slot.isoformat()}|{n}".encode()).hexdigest(),
                    "checkin_date": day.isoformat(),
                    "checkin_datetime": slot.strftime("%Y-%m-%d %H:%M:%S"),  # start of the slot
                    "member_id": member_id,
                    "first_name": None,
                    "last_name": None,
                    "barcode": None,
                    "checkin_status": None,
                    "station": None,
                    "checkin_club": CLUB,
                    "home_club": None,
                    "raw_json": json.dumps(m, default=str),
                    "loaded_at": loaded_at,
                })
        slot += timedelta(minutes=SLOT_MINUTES)
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
    """Print response structures (no member data) and check which endpoints exist."""
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1) - timedelta(microseconds=1)
    ts_range = f"{fmt(start)},{fmt(end)}"
    for path in ["/members/checkins/details", "/checkins/details", "/checkins",
                 "/members/checkins", "/clubs/checkins/details", "/members/checkins/history"]:
        resp = requests.get(f"{BASE_URL}{path}", headers=abc_headers(), timeout=60,
                            params={"checkInTimestampRange": ts_range, "page": 1, "size": 1})
        print(f"{path}: HTTP {resp.status_code}")
        if resp.status_code == 200:
            describe(resp.json())

    data = abc_get(SUMMARIES, {"checkInTimestampRange": ts_range, "page": 1, "size": 1})
    member_id = (data.get("members") or [{}])[0].get("memberId")
    if member_id:
        for path in [f"/members/{member_id}", f"/members/{member_id}/checkins"]:
            resp = requests.get(f"{BASE_URL}{path}", headers=abc_headers(), timeout=60)
            print(f"{path.replace(member_id, '<id>')}: HTTP {resp.status_code}")
            if resp.status_code == 200:
                describe(resp.json())
    resp = requests.get(f"{BASE_URL}/members", headers=abc_headers(), timeout=60, params={"page": 1, "size": 1})
    print(f"/members (list): HTTP {resp.status_code}")
    if resp.status_code == 200:
        describe(resp.json())

    slot_end = start + timedelta(minutes=SLOT_MINUTES) - timedelta(microseconds=1)
    print(f"Slot check: {len(fetch_summaries(start.replace(hour=18), slot_end.replace(hour=18)))} members "
          f"in the 6:00-6:{SLOT_MINUTES:02d} PM slot on {day}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=date.fromisoformat)
    p.add_argument("--end", type=date.fromisoformat)
    p.add_argument("--probe", action="store_true")
    p.add_argument("--setup", action="store_true")
    args = p.parse_args()

    now = datetime.now(CLUB_TZ)
    today = now.date()
    # Hourly runs only redo today; the first runs after midnight also finish yesterday.
    start = args.start or (today - timedelta(days=1) if now.hour < 1 else today)
    end = args.end or today

    if args.probe:
        probe(start if args.start else today)
        return

    client = bq_client()
    if args.setup:
        setup(client)
        if not (args.start or args.end):
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
