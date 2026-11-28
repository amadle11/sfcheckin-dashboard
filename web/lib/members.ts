import "server-only";
import { query, num, PROJECT } from "./bigquery";

// Member reporting from the members table the daily ABC job loads (one row per member,
// as ABC has them today) and the members_daily snapshots (counts per day).

const M = `\`${PROJECT}.checkInData.members\``;
const SNAP = `\`${PROJECT}.checkInData.members_daily\``;
const ACTIVE = `COALESCE(is_active, LOWER(member_status) = 'active')`;
const TODAY = `CURRENT_DATE('America/New_York')`;

export interface MemberSummary {
  total: number;
  active: number;
  joins30: number;
  ended30: number;
  joinsMonth: number;
  endedMonth: number;
  pastDueMembers: number;
  pastDueBalance: number;
  ghosts: number;
  medianAge: number | null;
  loadedAt: string | null;
}

export async function memberSummary(): Promise<MemberSummary> {
  const [r] = await query(`
    SELECT
      COUNT(*) AS total,
      COUNTIF(${ACTIVE}) AS active,
      COUNTIF(sign_date > DATE_SUB(${TODAY}, INTERVAL 30 DAY)) AS joins30,
      COUNTIF(NOT ${ACTIVE} AND expiration_date > DATE_SUB(${TODAY}, INTERVAL 30 DAY) AND expiration_date <= ${TODAY}) AS ended30,
      COUNTIF(sign_date >= DATE_TRUNC(${TODAY}, MONTH)) AS joins_month,
      COUNTIF(NOT ${ACTIVE} AND expiration_date >= DATE_TRUNC(${TODAY}, MONTH) AND expiration_date <= ${TODAY}) AS ended_month,
      COUNTIF(${ACTIVE} AND is_past_due) AS past_due_members,
      SUM(IF(${ACTIVE}, past_due_balance, 0)) AS past_due_balance,
      COUNTIF(${ACTIVE} AND (last_checkin_date IS NULL OR last_checkin_date < DATE_SUB(${TODAY}, INTERVAL 30 DAY))) AS ghosts,
      APPROX_QUANTILES(IF(${ACTIVE}, age, NULL), 2)[OFFSET(1)] AS median_age,
      FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(loaded_at)) AS loaded_at
    FROM ${M}`);
  return {
    total: num(r?.total), active: num(r?.active), joins30: num(r?.joins30), ended30: num(r?.ended30),
    joinsMonth: num(r?.joins_month), endedMonth: num(r?.ended_month),
    pastDueMembers: num(r?.past_due_members), pastDueBalance: num(r?.past_due_balance),
    ghosts: num(r?.ghosts), medianAge: r?.median_age == null ? null : num(r.median_age),
    loadedAt: (r?.loaded_at as string) ?? null,
  };
}

export interface FlowMonth { month: string; joins: number; ended: number; net: number }

/** Joins (agreement sign date) vs ended (expiration date of members no longer active), last 24 months. */
export async function joinsVsEnded(): Promise<FlowMonth[]> {
  const rows = await query(`
    WITH months AS (
      SELECT m FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(DATE_TRUNC(${TODAY}, MONTH), INTERVAL 23 MONTH), DATE_TRUNC(${TODAY}, MONTH), INTERVAL 1 MONTH)) AS m
    ),
    j AS (SELECT DATE_TRUNC(sign_date, MONTH) AS m, COUNT(*) AS n FROM ${M} WHERE sign_date IS NOT NULL GROUP BY m),
    e AS (SELECT DATE_TRUNC(expiration_date, MONTH) AS m, COUNT(*) AS n FROM ${M}
          WHERE NOT ${ACTIVE} AND expiration_date <= ${TODAY} GROUP BY m)
    SELECT FORMAT_DATE('%Y-%m', months.m) AS month, IFNULL(j.n, 0) AS joins, IFNULL(e.n, 0) AS ended
    FROM months LEFT JOIN j USING (m) LEFT JOIN e USING (m) ORDER BY month`);
  return rows.map((r) => ({ month: String(r.month), joins: num(r.joins), ended: num(r.ended), net: num(r.joins) - num(r.ended) }));
}

export interface Count { label: string; count: number }

async function counts(sql: string): Promise<Count[]> {
  return (await query(sql)).map((r) => ({ label: String(r.label ?? "Not set"), count: num(r.n) }));
}

export const byStatus = () =>
  counts(`SELECT IFNULL(member_status, 'Not set') AS label, COUNT(*) AS n FROM ${M} GROUP BY label ORDER BY n DESC`);

export const byMembershipType = () =>
  counts(`SELECT IFNULL(membership_type, 'Not set') AS label, COUNT(*) AS n FROM ${M} WHERE ${ACTIVE} GROUP BY label ORDER BY n DESC LIMIT 15`);

export const byAgeBand = () =>
  counts(`
    SELECT CASE
      WHEN age IS NULL THEN 'Unknown' WHEN age < 18 THEN 'Under 18' WHEN age < 25 THEN '18-24'
      WHEN age < 35 THEN '25-34' WHEN age < 45 THEN '35-44' WHEN age < 55 THEN '45-54'
      WHEN age < 65 THEN '55-64' ELSE '65+' END AS label, COUNT(*) AS n
    FROM ${M} WHERE ${ACTIVE} GROUP BY label
    ORDER BY CASE label WHEN 'Under 18' THEN 0 WHEN '18-24' THEN 1 WHEN '25-34' THEN 2 WHEN '35-44' THEN 3
      WHEN '45-54' THEN 4 WHEN '55-64' THEN 5 WHEN '65+' THEN 6 ELSE 7 END`);

export const byZip = () =>
  counts(`SELECT zip5 AS label, COUNT(*) AS n FROM ${M} WHERE ${ACTIVE} AND zip5 IS NOT NULL GROUP BY label ORDER BY n DESC LIMIT 12`);

export const bySource = () =>
  counts(`SELECT IFNULL(entry_source, 'Not set') AS label, COUNT(*) AS n FROM ${M}
          WHERE sign_date > DATE_SUB(${TODAY}, INTERVAL 12 MONTH) GROUP BY label ORDER BY n DESC LIMIT 10`);

export async function activeTrend(): Promise<{ date: string; active: number }[]> {
  const rows = await query(`
    SELECT FORMAT_DATE('%Y-%m-%d', snapshot_date) AS d, SUM(IF(is_active, members, 0)) AS active
    FROM ${SNAP} GROUP BY d ORDER BY d`);
  return rows.map((r) => ({ date: String(r.d), active: num(r.active) }));
}

export interface MemberRow {
  name: string;
  status: string;
  type: string;
  lastCheckin: string | null;
  daysSince: number | null;
  totalCheckins: number;
  signDate: string | null;
}

const memberCols = `
  TRIM(CONCAT(IFNULL(first_name, ''), ' ', IFNULL(last_name, ''))) AS name,
  IFNULL(member_status, '') AS status, IFNULL(membership_type, '') AS type,
  FORMAT_DATE('%Y-%m-%d', last_checkin_date) AS last_checkin,
  DATE_DIFF(${TODAY}, last_checkin_date, DAY) AS days_since,
  IFNULL(total_checkins, 0) AS total_checkins,
  FORMAT_DATE('%Y-%m-%d', sign_date) AS sign_date`;

const toMember = (r: Record<string, unknown>): MemberRow => ({
  name: String(r.name || "Unknown"), status: String(r.status), type: String(r.type),
  lastCheckin: (r.last_checkin as string) ?? null, daysSince: r.days_since == null ? null : num(r.days_since),
  totalCheckins: num(r.total_checkins), signDate: (r.sign_date as string) ?? null,
});

/** Active members who haven't checked in for 30+ days (or ever), most recently lapsed first. */
export async function ghostMembers(): Promise<MemberRow[]> {
  const rows = await query(`
    SELECT ${memberCols} FROM ${M}
    WHERE ${ACTIVE} AND (last_checkin_date IS NULL OR last_checkin_date < DATE_SUB(${TODAY}, INTERVAL 30 DAY))
    ORDER BY last_checkin_date DESC NULLS LAST LIMIT 200`);
  return rows.map(toMember);
}

export async function lookupMembers(q: string): Promise<MemberRow[]> {
  const rows = await query(`
    SELECT ${memberCols} FROM ${M}
    WHERE LOWER(CONCAT(IFNULL(first_name, ''), ' ', IFNULL(last_name, ''))) LIKE CONCAT('%', LOWER(@q), '%')
       OR barcode = @q OR agreement_number = @q
    ORDER BY ${ACTIVE} DESC, last_checkin_date DESC NULLS LAST LIMIT 25`, { q });
  return rows.map(toMember);
}

export interface PastDueType { type: string; members: number; pastDue: number; balance: number }

/** Past-due balances on active members, by membership type. */
export async function pastDueByType(): Promise<PastDueType[]> {
  const rows = await query(`
    SELECT IFNULL(membership_type, 'Not set') AS type, COUNT(*) AS members,
           COUNTIF(is_past_due) AS past_due, IFNULL(SUM(past_due_balance), 0) AS balance
    FROM ${M} WHERE ${ACTIVE} GROUP BY type ORDER BY balance DESC`);
  return rows.map((r) => ({ type: String(r.type), members: num(r.members), pastDue: num(r.past_due), balance: num(r.balance) }));
}

export async function pastDueTrend(): Promise<{ date: string; balance: number; members: number }[]> {
  const rows = await query(`
    SELECT FORMAT_DATE('%Y-%m-%d', snapshot_date) AS d, SUM(IF(is_active, past_due_balance, 0)) AS balance,
           SUM(IF(is_active, past_due_members, 0)) AS members
    FROM ${SNAP} GROUP BY d ORDER BY d`);
  return rows.map((r) => ({ date: String(r.d), balance: num(r.balance), members: num(r.members) }));
}
