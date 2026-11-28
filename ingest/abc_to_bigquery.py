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

# Member fields for the dashboard. ABC also returns street address, phone, exact birth
# date and gender; we deliberately don't store those. Age is computed at load time and
# only the 5-digit zip is kept, for the demographics page.
MEMBERS_SCHEMA = [
    bigquery.SchemaField("member_id", "STRING"),
    bigquery.SchemaField("agreement_number", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("barcode", "STRING"),
    bigquery.SchemaField("member_status", "STRING"),
    bigquery.SchemaField("join_status", "STRING"),
    bigquery.SchemaField("home_club", "STRING"),
    bigquery.SchemaField("is_active", "BOOL"),
    bigquery.SchemaField("is_converted_prospect", "BOOL"),
    bigquery.SchemaField("age", "INT64"),
    bigquery.SchemaField("zip5", "STRING"),
    bigquery.SchemaField("created_date", "DATE"),
    bigquery.SchemaField("first_checkin_date", "DATE"),
    bigquery.SchemaField("last_checkin_date", "DATE"),
    bigquery.SchemaField("total_checkins", "INT64"),
    bigquery.SchemaField("is_primary_member", "BOOL"),
    bigquery.SchemaField("is_non_member", "BOOL"),
    bigquery.SchemaField("membership_type", "STRING"),
    bigquery.SchemaField("term", "STRING"),
    bigquery.SchemaField("managed_type", "STRING"),
    bigquery.SchemaField("renewal_type", "STRING"),
    bigquery.SchemaField("entry_source", "STRING"),
    bigquery.SchemaField("sign_date", "DATE"),
    bigquery.SchemaField("begin_date", "DATE"),
    bigquery.SchemaField("since_date", "DATE"),
    bigquery.SchemaField("expiration_date", "DATE"),
    bigquery.SchemaField("is_past_due", "BOOL"),
    bigquery.SchemaField("past_due_balance", "NUMERIC"),
    bigquery.SchemaField("next_due_amount", "NUMERIC"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

# One row per day per status / membership type, so member counts build a history
# (ABC only tells us who is a member today).
SNAPSHOT_TABLE = f"{PROJECT}.{DATASET}.members_daily"
SNAPSHOT_SCHEMA = [
    bigquery.SchemaField("snapshot_date", "DATE"),
    bigquery.SchemaField("member_status", "STRING"),
    bigquery.SchemaField("membership_type", "STRING"),
    bigquery.SchemaField("is_active", "BOOL"),
    bigquery.SchemaField("members", "INT64"),
    bigquery.SchemaField("past_due_members", "INT64"),
    bigquery.SchemaField("past_due_balance", "NUMERIC"),
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


def _date(v):
    """ABC dates come as 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss...'; keep the date part."""
    if isinstance(v, str) and len(v) >= 10 and v[4] == "-" and v[7] == "-":
        try:
            return date.fromisoformat(v[:10]).isoformat()
        except ValueError:
            return None
    return None


def _bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str) and v.lower() in ("true", "false"):
        return v.lower() == "true"
    return None


def _num(v):
    try:
        return None if v in (None, "") else round(float(v), 2)
    except (TypeError, ValueError):
        return None


def _int(v):
    n = _num(v)
    return None if n is None else int(n)


def _age(birth, today):
    b = _date(birth)
    if not b:
        return None
    b = date.fromisoformat(b)
    age = today.year - b.year - ((today.month, today.day) < (b.month, b.day))
    return age if 0 < age < 110 else None


def fetch_members():
    loaded_at = datetime.now(timezone.utc).isoformat()
    today = datetime.now(CLUB_TZ).date()
    rows = []
    for m in fetch_pages("/members", "members", {}):
        personal = m.get("personal") or {}
        agreement = m.get("agreement") or {}
        zip_code = (personal.get("postalCode") or "").strip()[:5]
        rows.append({
            "member_id": m.get("memberId"),
            "agreement_number": agreement.get("agreementNumber"),
            "first_name": personal.get("firstName"),
            "last_name": personal.get("lastName"),
            "barcode": personal.get("barcode"),
            "member_status": personal.get("memberStatus"),
            "join_status": personal.get("joinStatus"),
            "home_club": personal.get("homeClub"),
            "is_active": _bool(personal.get("isActive")),
            "is_converted_prospect": _bool(personal.get("isConvertedProspect")),
            "age": _age(personal.get("birthDate"), today),
            "zip5": zip_code if zip_code.isdigit() and len(zip_code) == 5 else None,
            "created_date": _date(personal.get("createTimestamp")),
            "first_checkin_date": _date(personal.get("firstCheckInTimestamp")),
            "last_checkin_date": _date(personal.get("lastCheckInTimestamp")),
            "total_checkins": _int(personal.get("totalCheckInCount")),
            "is_primary_member": _bool(agreement.get("isPrimaryMember")),
            "is_non_member": _bool(agreement.get("isNonMember")),
            "membership_type": agreement.get("membershipType"),
            "term": agreement.get("term"),
            "managed_type": agreement.get("managedType"),
            "renewal_type": agreement.get("renewalType"),
            "entry_source": agreement.get("agreementEntrySourceReportName") or agreement.get("agreementEntrySource"),
            "sign_date": _date(agreement.get("signDate")),
            "begin_date": _date(agreement.get("beginDate")),
            "since_date": _date(agreement.get("sinceDate")),
            "expiration_date": _date(agreement.get("expirationDate")),
            "is_past_due": _bool(agreement.get("isPastDue")),
            "past_due_balance": _num(agreement.get("totalPastDueBalance")),
            "next_due_amount": _num(agreement.get("nextDueAmount")),
            "loaded_at": loaded_at,
        })
    return rows


def snapshot_rows(rows, day):
    groups = {}
    for r in rows:
        key = (r["member_status"], r["membership_type"], r["is_active"])
        g = groups.setdefault(key, {"members": 0, "past_due_members": 0, "past_due_balance": 0.0})
        g["members"] += 1
        if r["is_past_due"]:
            g["past_due_members"] += 1
        g["past_due_balance"] += r["past_due_balance"] or 0
    return [
        {"snapshot_date": day.isoformat(), "member_status": k[0], "membership_type": k[1], "is_active": k[2],
         "members": v["members"], "past_due_members": v["past_due_members"],
         "past_due_balance": round(v["past_due_balance"], 2)}
        for k, v in groups.items()
    ]


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
    # WRITE_TRUNCATE also updates the table's columns to MEMBERS_SCHEMA.
    job_config = bigquery.LoadJobConfig(
        schema=MEMBERS_SCHEMA, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    client.load_table_from_json(rows, MEMBERS_TABLE, job_config=job_config).result()

    # Today's counts replace any earlier snapshot for today.
    day = datetime.now(CLUB_TZ).date()
    snap_config = bigquery.LoadJobConfig(
        schema=SNAPSHOT_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(field="snapshot_date"),
    )
    partition = f"{SNAPSHOT_TABLE}${day.strftime('%Y%m%d')}"
    client.load_table_from_json(snapshot_rows(rows, day), partition, job_config=snap_config).result()


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
        active = sum(1 for r in rows if r["is_active"])
        print(f"Members table refreshed: {len(rows)} members ({active} active). Snapshot saved.")

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
