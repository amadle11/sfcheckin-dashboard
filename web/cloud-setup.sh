#!/usr/bin/env bash
# One-time setup + deploy of Sig Fit Reporting to Cloud Run in the sigfit project.
# Run in Google Cloud Shell from the repo root:   bash web/cloud-setup.sh
# Safe to re-run: every step skips what already exists. Re-running also redeploys.
set -euo pipefail

PROJECT=sigfit
REGION=us-east1
SERVICE=sigfit-reporting
SA_NAME=sigfit-reporting
SA="$SA_NAME@$PROJECT.iam.gserviceaccount.com"

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }
cd "$(dirname "$0")/.."

gcloud config set project "$PROJECT" >/dev/null

ADMIN_EMAIL=$(gcloud config get-value account 2>/dev/null)
read -r -p "Admin Google account for the app [$ADMIN_EMAIL]: " INPUT
ADMIN_EMAIL=${INPUT:-$ADMIN_EMAIL}

say "1/5  Turning on Cloud Run, Firestore, Secret Manager and build services (1-2 min)"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  firestore.googleapis.com secretmanager.googleapis.com

say "2/5  Firestore database for people and projects"
if ! gcloud firestore databases describe --database="(default)" >/dev/null 2>&1; then
  gcloud firestore databases create --location=nam5 --type=firestore-native
else
  echo "Already exists."
fi

say "3/5  Service account for the app"
if ! gcloud iam service-accounts describe "$SA" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_NAME" --display-name="Sig Fit Reporting app"
  sleep 10
fi
for ROLE in roles/bigquery.jobUser roles/bigquery.dataViewer roles/datastore.user; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA" --role="$ROLE" \
    --condition=None >/dev/null
done
echo "Can read BigQuery and use Firestore."

secret() { # name value
  if gcloud secrets describe "$1" >/dev/null 2>&1; then
    printf '%s' "$2" | gcloud secrets versions add "$1" --data-file=- >/dev/null
  else
    printf '%s' "$2" | gcloud secrets create "$1" --data-file=- >/dev/null
  fi
  gcloud secrets add-iam-policy-binding "$1" --member="serviceAccount:$SA" \
    --role=roles/secretmanager.secretAccessor >/dev/null
}

if ! gcloud secrets describe auth-secret >/dev/null 2>&1; then
  secret auth-secret "$(openssl rand -base64 32)"
fi
gcloud secrets describe google-client-secret >/dev/null 2>&1 || secret google-client-secret "not-set-yet"

say "4/5  Building and deploying the app (3-5 min)"
EXISTING_ID=$(gcloud run services describe "$SERVICE" --region "$REGION" --format=json 2>/dev/null | python3 -c '
import json, sys
try:
    env = json.load(sys.stdin)["spec"]["template"]["spec"]["containers"][0].get("env", [])
    print(next((e.get("value", "") for e in env if e["name"] == "AUTH_GOOGLE_ID"), ""))
except Exception:
    print("")' || true)
gcloud run deploy "$SERVICE" --source web --region "$REGION" --quiet \
  --service-account "$SA" --allow-unauthenticated \
  --update-env-vars "STORE=firestore,BQ_PROJECT=$PROJECT,AUTH_TRUST_HOST=true,ADMIN_EMAILS=$ADMIN_EMAIL,AUTH_GOOGLE_ID=${EXISTING_ID:-not-set-yet}" \
  --update-secrets "AUTH_SECRET=auth-secret:latest,AUTH_GOOGLE_SECRET=google-client-secret:latest"
URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')
gcloud run services update "$SERVICE" --region "$REGION" --update-env-vars "APP_URL=$URL" --quiet >/dev/null

if [ -n "$EXISTING_ID" ] && [ "$EXISTING_ID" != "not-set-yet" ]; then
  say "Done. The app is updated: $URL"
  exit 0
fi

say "5/5  Google sign-in (one-time, about 2 minutes in the browser)"
cat <<EOF
Open: https://console.cloud.google.com/auth/clients/create?project=$PROJECT

  If it asks you to configure the consent screen first: App name "Sig Fit Reporting",
  your email for support and contact, Audience = External, then come back to the link.

  Application type:            Web application
  Name:                        Sig Fit Reporting
  Authorized JavaScript origin: $URL
  Authorized redirect URI:      $URL/api/auth/callback/google

Click Create, then paste the two values it shows below.
EOF
read -r -p "Client ID: " CLIENT_ID
read -r -s -p "Client secret (hidden): " CLIENT_SECRET; echo
secret google-client-secret "$CLIENT_SECRET"
gcloud run services update "$SERVICE" --region "$REGION" --quiet \
  --update-env-vars "AUTH_GOOGLE_ID=$CLIENT_ID" \
  --update-secrets "AUTH_GOOGLE_SECRET=google-client-secret:latest" >/dev/null

say "All set. Open $URL and sign in as $ADMIN_EMAIL."
echo "If Google says the app isn't verified, that's expected for an internal app: click Continue."
echo "Under Google Auth Platform > Audience, click 'Publish app' so staff outside your account can sign in."
