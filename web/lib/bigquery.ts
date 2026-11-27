import "server-only";

// Read-only queries against the check-in data the hourly ABC job loads.
// On Cloud Run the service's own account is used; locally set
// GOOGLE_APPLICATION_CREDENTIALS or GCP_SERVICE_ACCOUNT_JSON.

const PROJECT = process.env.BQ_PROJECT || "sigfit";
const CLEAN = `\`${PROJECT}.checkInData.checkin_clean\``;
const RAW = `\`${PROJECT}.checkInData.checkins_raw\``;
const CACHE_MS = 10 * 60 * 1000;

type Params = Record<string, string | number>;
type Row = Record<string, unknown>;

const g = globalThis as unknown as {
  __bq?: Promise<import("@google-cloud/bigquery").BigQuery>;
  __bqCache?: Map<string, { at: number; rows: Row[] }>;
};

function client() {
  g.__bq ??= import("@google-cloud/bigquery").then(({ BigQuery }) => {
    const json = process.env.GCP_SERVICE_ACCOUNT_JSON;
    return new BigQuery({ projectId: PROJECT, ...(json ? { credentials: JSON.parse(json) } : {}) });
  });
  return g.__bq;
}

async function query<T extends Row>(sql: string, params: Params = {}): Promise<T[]> {
  const cache = (g.__bqCache ??= new Map());
  const key = sql + JSON.stringify(params);
  const hit = cache.get(key);
  if (hit && Date.now() - hit.at < CACHE_MS) return hit.rows as T[];
  const [rows] = await (await client()).query({ query: sql, params });
  // BigQuery returns DATE/DATETIME/TIMESTAMP as objects with a .value string.
  const plain = (rows as Row[]).map((r) =>
    Object.fromEntries(
      Object.entries(r).map(([k, v]) => [k, v && typeof v === "object" && "value" in v ? (v as { value: unknown }).value : v]),
    ),
  );
  cache.set(key, { at: Date.now(), rows: plain });
  return plain as T[];
}

const num = (v: unknown) => Number(v ?? 0);

export interface YearToDate {
  year: number;
  checkins: number;
  uniqueMembers: number;
  days: number;
}

/** Each year from Jan 1 through today's month/day, so years compare like for like. */
export async function yearToDate(): Promise<{ cutoff: string; years: YearToDate[] }> {
  const rows = await query(
    `SELECT EXTRACT(YEAR FROM checkin_datetime) AS year, COUNT(*) AS checkins,
            COUNT(DISTINCT agreement_number) AS members, COUNT(DISTINCT DATE(checkin_datetime)) AS days
     FROM ${CLEAN}
     WHERE FORMAT_DATETIME('%m%d', checkin_datetime) <= FORMAT_DATE('%m%d', CURRENT_DATE('America/New_York'))
     GROUP BY year ORDER BY year`,
  );
  return {
    cutoff: new Date().toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "America/New_York" }),
    years: rows.map((r) => ({ year: num(r.year), checkins: num(r.checkins), uniqueMembers: num(r.members), days: num(r.days) })),
  };
}

export interface RecentCounts {
  today: number;
  last7: number;
  prior7: number;
  lastLoaded: string | null;
}

export async function recentCounts(): Promise<RecentCounts> {
  const [r] = await query(
    `SELECT
       COUNTIF(DATE(checkin_datetime) = CURRENT_DATE('America/New_York')) AS today,
       COUNTIF(DATE(checkin_datetime) BETWEEN DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 6 DAY) AND CURRENT_DATE('America/New_York')) AS last7,
       COUNTIF(DATE(checkin_datetime) BETWEEN DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 13 DAY) AND DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 7 DAY)) AS prior7
     FROM ${CLEAN}
     WHERE DATE(checkin_datetime) >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 13 DAY)`,
  );
  const [l] = await query(`SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(loaded_at)) AS last_loaded FROM ${RAW}`);
  return { today: num(r?.today), last7: num(r?.last7), prior7: num(r?.prior7), lastLoaded: (l?.last_loaded as string) ?? null };
}

export interface MonthRow {
  year: number;
  month: number;
  checkins: number;
  uniqueMembers: number;
  days: number;
}

export async function monthlyByYear(): Promise<MonthRow[]> {
  const rows = await query(
    `SELECT EXTRACT(YEAR FROM checkin_datetime) AS year, EXTRACT(MONTH FROM checkin_datetime) AS month,
            COUNT(*) AS checkins, COUNT(DISTINCT agreement_number) AS members, COUNT(DISTINCT DATE(checkin_datetime)) AS days
     FROM ${CLEAN} GROUP BY year, month ORDER BY year, month`,
  );
  return rows.map((r) => ({
    year: num(r.year), month: num(r.month), checkins: num(r.checkins), uniqueMembers: num(r.members), days: num(r.days),
  }));
}

export interface RangeReport {
  checkins: number;
  uniqueMembers: number;
  days: number;
  byWeekday: { day: string; checkins: number }[];
  byHour: { hour: number; checkins: number }[];
  topMembers: { name: string; visits: number }[];
}

export async function rangeReport(start: string, end: string): Promise<RangeReport> {
  const p = { start, end };
  const where = `WHERE DATE(checkin_datetime) BETWEEN DATE(@start) AND DATE(@end)`;
  const [k] = await query(
    `SELECT COUNT(*) AS checkins, COUNT(DISTINCT agreement_number) AS members, COUNT(DISTINCT DATE(checkin_datetime)) AS days
     FROM ${CLEAN} ${where}`, p);
  const wd = await query(
    `SELECT EXTRACT(DAYOFWEEK FROM checkin_datetime) AS d, COUNT(*) AS checkins FROM ${CLEAN} ${where} GROUP BY d ORDER BY d`, p);
  const hr = await query(
    `SELECT EXTRACT(HOUR FROM checkin_datetime) AS h, COUNT(*) AS checkins FROM ${CLEAN} ${where} GROUP BY h ORDER BY h`, p);
  const top = await query(
    `SELECT ANY_VALUE(member_name) AS name, COUNT(*) AS visits FROM ${CLEAN} ${where}
     GROUP BY agreement_number ORDER BY visits DESC LIMIT 10`, p);
  const names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  // Monday first, like the Streamlit dashboard.
  const byWeekday = [2, 3, 4, 5, 6, 7, 1].map((d) => ({
    day: names[d - 1],
    checkins: num(wd.find((r) => num(r.d) === d)?.checkins),
  }));
  return {
    checkins: num(k?.checkins),
    uniqueMembers: num(k?.members),
    days: num(k?.days),
    byWeekday,
    byHour: hr.map((r) => ({ hour: num(r.h), checkins: num(r.checkins) })),
    topMembers: top.map((r) => ({ name: String(r.name || "Unknown"), visits: num(r.visits) })),
  };
}
