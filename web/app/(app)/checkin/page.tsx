import Link from "next/link";
import { requireMenu } from "@/lib/access";
import { monthlyByYear, rangeReport, recentCounts, yearToDate } from "@/lib/bigquery";
import { MONTHS, RANGES, rangeDates, type RangeKey } from "@/lib/dates";
import { Kpi, DataError, pct } from "@/components/Kpi";
import { Bars, YearLines } from "@/components/Charts";

export const metadata = { title: "Check-In · Sig Fit Reporting" };

const n = (v: number) => Math.round(v).toLocaleString("en-US");

export default async function CheckInPage({ searchParams }: { searchParams: Promise<{ range?: string; metric?: string }> }) {
  await requireMenu("checkin");
  const sp = await searchParams;
  const range: RangeKey = (sp.range && sp.range in RANGES ? sp.range : "90d") as RangeKey;
  const metric = sp.metric === "total" || sp.metric === "members" ? sp.metric : "avg";
  const { start, end } = rangeDates(range);

  let data;
  try {
    data = await Promise.all([yearToDate(), monthlyByYear(), rangeReport(start, end), recentCounts()]);
  } catch (err) {
    console.error("[checkin] BigQuery error", err);
    return (<><h1>Check-In</h1><div className="section"><DataError what="check-in data" /></div></>);
  }
  const [ytd, monthly, report, recent] = data;
  const [prev, cur] = ytd.years.slice(-2);
  const avg = (y?: { checkins: number; days: number }) => (y ? y.checkins / Math.max(y.days, 1) : 0);

  const years = [...new Set(monthly.map((m) => m.year))].slice(-4).map(String);
  const value = (m: (typeof monthly)[number]) =>
    metric === "total" ? m.checkins : metric === "members" ? m.uniqueMembers : Math.round(m.checkins / Math.max(m.days, 1));
  const lineRows = MONTHS.map((month, i) => {
    const row: Record<string, string | number> = { month };
    for (const y of years) {
      const m = monthly.find((r) => r.year === Number(y) && r.month === i + 1);
      if (m) row[y] = value(m);
    }
    return row;
  });
  const hourRows = report.byHour.map((h) => ({
    hour: h.hour === 0 ? "12a" : h.hour < 12 ? `${h.hour}a` : h.hour === 12 ? "12p" : `${h.hour - 12}p`,
    checkins: h.checkins,
  }));
  const updated = recent.lastLoaded
    ? new Date(recent.lastLoaded).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/New_York" })
    : null;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Check-In</h1>
          <div className="muted">Member check-ins from ABC, refreshed hourly{updated && <> · last loaded {updated}</>}</div>
        </div>
      </div>

      <h2>Year over year</h2>
      <p className="small muted" style={{ marginTop: -6 }}>Each year from Jan 1 through {ytd.cutoff}, so the comparison is like for like.</p>
      {cur ? (
        <div className="grid grid-3">
          <Kpi label={`Check-ins, ${cur.year} to date`} value={n(cur.checkins)} change={prev ? pct(cur.checkins, prev.checkins) : null} note={prev ? `vs ${prev.year}` : undefined} />
          <Kpi label={`Unique members, ${cur.year} to date`} value={n(cur.uniqueMembers)} change={prev ? pct(cur.uniqueMembers, prev.uniqueMembers) : null} note={prev ? `vs ${prev.year}` : undefined} />
          <Kpi label={`Avg check-ins per day, ${cur.year}`} value={n(avg(cur))} change={prev ? pct(avg(cur), avg(prev)) : null} note={prev ? `vs ${prev.year}` : undefined} />
        </div>
      ) : <div className="empty">No check-in data yet.</div>}

      <div className="card section">
        <div className="row" style={{ justifyContent: "space-between", marginBottom: 8 }}>
          <h3 style={{ margin: 0 }}>By month</h3>
          <div className="tabs" style={{ margin: 0, border: "none" }}>
            <Link href={`?range=${range}&metric=avg`} className={metric === "avg" ? "active" : ""}>Avg per day</Link>
            <Link href={`?range=${range}&metric=total`} className={metric === "total" ? "active" : ""}>Total</Link>
            <Link href={`?range=${range}&metric=members`} className={metric === "members" ? "active" : ""}>Unique members</Link>
          </div>
        </div>
        <YearLines rows={lineRows} years={years} />
        {metric === "avg" && <p className="small muted" style={{ margin: 0 }}>Avg per day is the default because the current month isn&apos;t finished, so its total looks low.</p>}
      </div>

      <div className="section">
        <div className="row" style={{ justifyContent: "space-between" }}>
          <h2 style={{ margin: 0 }}>Report</h2>
          <div className="tabs" style={{ margin: 0 }}>
            {(Object.keys(RANGES) as RangeKey[]).map((k) => (
              <Link key={k} href={`?range=${k}&metric=${metric}`} className={k === range ? "active" : ""}>{RANGES[k]}</Link>
            ))}
          </div>
        </div>
        <p className="small muted">{start} to {end}</p>
        <div className="grid grid-3">
          <Kpi label="Check-ins" value={n(report.checkins)} />
          <Kpi label="Unique members" value={n(report.uniqueMembers)} />
          <Kpi label="Avg per day" value={n(report.checkins / Math.max(report.days, 1))} />
        </div>
        <div className="grid grid-2 section" style={{ marginTop: 14 }}>
          <div className="card"><h3>By day of week</h3><Bars rows={report.byWeekday} x="day" y="checkins" /></div>
          <div className="card"><h3>By hour</h3><Bars rows={hourRows} x="hour" y="checkins" /></div>
        </div>
        <div className="card section table-wrap" style={{ marginTop: 14 }}>
          <h3>Most frequent members</h3>
          <table>
            <thead><tr><th>#</th><th>Member</th><th style={{ textAlign: "right" }}>Visits</th></tr></thead>
            <tbody>
              {report.topMembers.map((m, i) => (
                <tr key={i}><td className="muted">{i + 1}</td><td>{m.name}</td><td style={{ textAlign: "right" }}>{n(m.visits)}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      <p className="small muted section">
        Ghost Members and Member Lookup are still on the{" "}
        <a href="https://sfcheckin-dashboard-g52ujbcpflw2mhvve7rxzq.streamlit.app/">Streamlit dashboard</a> and move here in Phase 2 with the full member data.
      </p>
    </>
  );
}
