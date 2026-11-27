# Deploying Sig Fit Reporting

The app runs on **Cloud Run** in the existing `sigfit` Google Cloud project, next to the
BigQuery check-in data. People and projects are stored in **Firestore** in the same
project. Sign-in is **Google**.

Run these once, in [Cloud Shell](https://shell.cloud.google.com) (already signed in as
the project owner). Replace `you@gmail.com` with the admin's Google account.

## 1. Turn on the services

```bash
gcloud config set project sigfit
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com firestore.googleapis.com
gcloud firestore databases create --location=nam5 --type=firestore-native
```

## 2. A service account for the app

```bash
gcloud iam service-accounts create sigfit-reporting --display-name="Sig Fit Reporting app"
SA=sigfit-reporting@sigfit.iam.gserviceaccount.com
gcloud projects add-iam-policy-binding sigfit --member=serviceAccount:$SA --role=roles/bigquery.jobUser
gcloud projects add-iam-policy-binding sigfit --member=serviceAccount:$SA --role=roles/bigquery.dataViewer
gcloud projects add-iam-policy-binding sigfit --member=serviceAccount:$SA --role=roles/datastore.user
```

## 3. Google sign-in

1. In the console, open **Google Auth Platform → Branding** and fill in the app name
   ("Sig Fit Reporting") and support email. Under **Audience**, pick **External** and
   publish the app (only people you add on the Admin page can get in anyway).
2. Open **Clients → Create client → Web application**. Leave the redirect URI empty for
   now and copy the **Client ID** and **Client secret**.

## 4. Deploy

From the repo root in Cloud Shell (`git clone https://github.com/amadle11/sfcheckin-dashboard && cd sfcheckin-dashboard`):

```bash
printf '%s' "$(openssl rand -base64 32)" | gcloud secrets create auth-secret --data-file=-
printf '%s' 'PASTE_CLIENT_SECRET' | gcloud secrets create google-client-secret --data-file=-
for s in auth-secret google-client-secret; do
  gcloud secrets add-iam-policy-binding $s --member=serviceAccount:$SA --role=roles/secretmanager.secretAccessor
done

gcloud run deploy sigfit-reporting --source web --region us-east1 \
  --service-account $SA --allow-unauthenticated \
  --set-env-vars "STORE=firestore,BQ_PROJECT=sigfit,AUTH_TRUST_HOST=true,ADMIN_EMAILS=you@gmail.com,AUTH_GOOGLE_ID=PASTE_CLIENT_ID" \
  --set-secrets "AUTH_SECRET=auth-secret:latest,AUTH_GOOGLE_SECRET=google-client-secret:latest"
```

`--allow-unauthenticated` only lets the sign-in page load; every page checks the
person's Google account against the Admin list.

The deploy prints the app's URL, e.g. `https://sigfit-reporting-xxxxx.us-east1.run.app`.

## 5. Finish sign-in

Back in **Clients**, edit the web client and add:

- Authorized JavaScript origin: the app URL
- Authorized redirect URI: `<app URL>/api/auth/callback/google`

Then set `APP_URL` so watcher emails link to the right place:

```bash
gcloud run services update sigfit-reporting --region us-east1 --update-env-vars APP_URL=<app URL>
```

Open the URL, sign in, and add people on the **Admin** page.

## Optional: watcher emails

Watchers get an email when a project gets an update or changes status. To turn it on,
create a Gmail [app password](https://myaccount.google.com/apppasswords) for the sending
account and set:

```bash
printf '%s' 'smtps://sender@gmail.com:APP_PASSWORD@smtp.gmail.com:465' | gcloud secrets create smtp-url --data-file=-
gcloud secrets add-iam-policy-binding smtp-url --member=serviceAccount:$SA --role=roles/secretmanager.secretAccessor
gcloud run services update sigfit-reporting --region us-east1 \
  --update-secrets SMTP_URL=smtp-url:latest --update-env-vars "MAIL_FROM=Sig Fit Reporting <sender@gmail.com>"
```

Without it, everything else works and emails are skipped.

## Updating

Re-run the `gcloud run deploy` command from step 4 after merging changes.
