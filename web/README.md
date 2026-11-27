# Sig Fit Reporting (web app)

One app for Signature Fitness reporting and projects: a Projects & Tasks board everyone
sees, an Executive Summary, a page per area (Check-In now; Members, Group Fitness, PT,
Sales, Billing, Juice Bar and Staff in later phases), and per-person menu access.

- **Stack:** Next.js (App Router) on Cloud Run, Google sign-in (Auth.js), Firestore for
  people and projects, BigQuery (`sigfit.checkInData`) for reporting.
- **Deploying:** see [DEPLOY.md](DEPLOY.md).

## Run it locally

```bash
cd web
npm install
cp .env.example .env.local   # then set STORE=file and DEV_LOGIN_EMAIL=you@example.com
npm run dev
```

With `DEV_LOGIN_EMAIL` set (development only), the app skips Google sign-in and treats
you as that person; list the same email in `ADMIN_EMAILS` to be an admin. Projects and
people are saved to `.data/store.json`. The Check-In page needs BigQuery credentials
(`GCP_SERVICE_ACCOUNT_JSON` or `GOOGLE_APPLICATION_CREDENTIALS`); without them it shows a
"can't load" card and the rest of the app still works.

## Access

| Role | Sees |
|---|---|
| Admin | Every menu, plus Admin |
| Executive | Every reporting menu |
| Manager | Only the menus ticked for them |
| Staff | Projects & Tasks, plus any menus ticked |

Everyone sees Projects & Tasks and Completed. Only people added on the Admin page (or in
`ADMIN_EMAILS`) can sign in.

## Projects & Tasks

- **Project** = major work; when it's marked Done it moves to the Completed menu.
  **Task** = smaller work, optionally part of a project.
- Priority P1 (now), P2 (next), P3 (later); owners order their own list with the arrows.
- **My work** shows your items, **Team** shows everyone's grouped by person.
- **Private** items are visible only to their owners and admins.
- **Watch** a project to get an email on each update or status change (needs `SMTP_URL`).
- Owners are matched by the person's name on the Admin page, so use the same first name.
