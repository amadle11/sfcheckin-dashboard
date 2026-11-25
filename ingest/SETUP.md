# ABC → BigQuery check-in feed: one-time setup

Data path: **ABC Financial API → BigQuery (`sigfit.checkInData`) → Streamlit dashboard.**
A GitHub Actions job (`.github/workflows/ingest.yml`) runs `ingest/abc_to_bigquery.py` every hour.

## 1. Turn on billing for the `sigfit` project
BigQuery's free "sandbox" deletes tables after 60 days. That's what broke the dashboard.
Google Cloud console → **Billing** → link a billing account to `sigfit`.
At this data size, usage normally stays inside BigQuery's free monthly tier.

## 2. Make a new service account key (the old one is public)
The old key was committed to this public repo in April, so it must be replaced.
1. Console → **IAM & Admin → Service Accounts** → `streamlit-dashboard@sigfit...`
2. **Permissions:** make sure it has **BigQuery Data Editor** and **BigQuery Job User** (IAM page).
3. **Keys** tab → delete the old key (id starts `04b71d`) → **Add key → JSON**. A file downloads.

## 3. Add three GitHub secrets
Repo → **Settings → Secrets and variables → Actions → New repository secret**:

| Name | Value |
|---|---|
| `ABC_APP_ID` | your ABC app id |
| `ABC_APP_KEY` | your ABC app key |
| `GCP_SERVICE_ACCOUNT_JSON` | paste the whole JSON key file |

## 4. Update the Streamlit secret
share.streamlit.io → your app → ⋮ → **Settings → Secrets**. Replace the `[gcp_service_account]`
block with the new key's values (same format as before). Keep `app_password` as it is.

## 5. Run it (Repo → Actions → "ABC check-ins to BigQuery" → Run workflow)
1. **mode = probe**: prints only the field *names* ABC returns. Send these to Claude so we can confirm the mapping.
2. **mode = setup**: creates the `checkins_raw` table and the `checkin_clean` view the dashboard reads.
3. **Backfill**: mode = normal with a start and end date. Do **one month per run** (e.g. `2026-10-01` to `2026-10-31`).
   ABC only returns check-in *counts*, so each day is read in 96 fifteen-minute slots; a month takes a while.
   Each day is pulled separately, and re-running a day replaces it, so there are no duplicates.

After that, the hourly schedule keeps today and yesterday up to date on its own.

Note: Actions logs in a public repo are public. The script only logs counts and field names, never member data.
