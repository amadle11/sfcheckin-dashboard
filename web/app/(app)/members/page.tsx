import { requireMenu } from "@/lib/access";
import {
  activeTrend, byAgeBand, byMembershipType, bySource, byStatus, byZip, ghostMembers, joinsVsEnded, lookupMembers,
  memberSummary, type MemberRow,
} from "@/lib/members";
import { MONTHS } from "@/lib/dates";
import { Kpi } from "@/components/Kpi";
import { Bars, GroupedBars, TrendLine } from "@/components/Charts";
import { CountTable } from "@/components/CountTable";

export const metadata = { title: "Members · Sig Fit Reporting" };

const n = (v: number) => Math.round(v).toLocaleString("en-US");
const monthLabel = (ym: string) => `${MONTHS[Number(ym.slice(5, 7)) - 1]} ${ym.slice(2, 4)}`;
const fmtDay = (d: string | null) =>
  d ? new Date(d + "T12:00:00").toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" }) : "Never";

function MemberTable({ rows, empty }: { rows: MemberRow[]; empty: string }) {
  if (rows.length === 0) return <div className="empty">{empty}</div>;
  return (
    <table>
      <thead><tr><th>Member</th><th>Status</th><th>Membership</th><th>Joined</th><th>Last check-in</th><th style={{ textAlign: "right" }}>Total visits</th></tr></thead>
      <tbody>
        {rows.map((m, i) => (
          <tr key={i}>
            <td><b>{m.name}</b></td>
            <td>{m.status}</td>
            <td>{m.type}</td>
            <td style={{ whiteSpace: "nowrap" }}>{m.signDate ? fmtDay(m.signDate) : ""}</td>
            <td style={{ whiteSpace: "nowrap" }}>{fmtDay(m.lastCheckin)}{m.daysSince != null && <span className="muted"> · {m.daysSince}d ago</span>}</td>
            <td style={{ textAlign: "right" }}>{n(m.totalCheckins)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function NotReady({ err }: { err: unknown }) {
  const msg = String((err as Error)?.message ?? err);
  const missingColumns = /Unrecognized name|not found/i.test(msg);
  return (
    <div className="card">
      <h3>Member data isn&apos;t ready yet</h3>
      <p className="small muted" style={{ margin: 0 }}>
        {missingColumns
          ? "The members table still has the old columns. It refreshes daily at 4 AM, or run the ABC workflow with mode = members to load it now."
          : "The app couldn't reach BigQuery. Check that its service account can read the sigfit.checkInData dataset."}
      </p>
    </div>
  );
}

export default async function MembersPage({ searchParams }: { searchParams: Promise<{ q?: string }> }) {
  await requireMenu("members");
  const q = ((await searchParams).q ?? "").trim().slice(0, 60);

  let data;
  try {
    data = await Promise.all([
      memberSummary(), joinsVsEnded(), byMembershipType(), byAgeBand(), byZip(), bySource(), byStatus(), activeTrend(),
      ghostMembers(), q ? lookupMembers(q) : Promise.resolve(null),
    ]);
  } catch (err) {
    console.error("[members] BigQuery error", err);
    return (<><h1>Members</h1><div className="section"><NotReady err={err} /></div></>);
  }
  const [s, flow, types, ages, zips, sources, statuses, trend, ghosts, found] = data;
  const flowRows = flow.map((f) => ({ month: monthLabel(f.month), Joins: f.joins, Ended: f.ended }));
  const last12 = flow.slice(-12);
  const net12 = last12.reduce((t, f) => t + f.net, 0);
  const updated = s.loadedAt
    ? new Date(s.loadedAt).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/New_York" })
    : null;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Members</h1>
          <div className="muted">From ABC, refreshed daily{updated && <> · last loaded {updated}</>}</div>
        </div>
      </div>

      <div className="grid grid-4">
        <Kpi label="Active members" value={n(s.active)} note={`of ${n(s.total)} member records`} />
        <Kpi label="Joined this month" value={n(s.joinsMonth)} note={`${n(s.joins30)} in the last 30 days`} />
        <Kpi label="Ended this month" value={n(s.endedMonth)} note={`${n(s.ended30)} in the last 30 days`} />
        <Kpi label="Net, last 12 months" value={(net12 >= 0 ? "+" : "") + n(net12)} note="joins minus ended" />
      </div>

      <div className="card section">
        <h3>Net impact: joins vs ended, by month</h3>
        <GroupedBars rows={flowRows} x="month" series={[{ key: "Joins", label: "Joined", color: "#2563eb" }, { key: "Ended", label: "Ended", color: "#94a3b8" }]} />
        <p className="small muted" style={{ margin: 0 }}>
          Joined = agreement sign date. Ended = expiration date of members ABC no longer lists as active. Ended only counts
          former members ABC still returns, so check it against the Net Impact sheet before retiring that sheet.
        </p>
      </div>

      <div className="card section">
        <h3>Active members over time</h3>
        {trend.length > 1 ? (
          <TrendLine rows={trend} x="date" y="active" label="Active members" />
        ) : (
          <p className="small muted" style={{ margin: 0 }}>
            The app saves a count every day starting today, so this chart fills in over the coming weeks.
          </p>
        )}
      </div>

      <div className="grid grid-2 section">
        <CountTable title="Active members by membership type" rows={types} />
        <div className="card">
          <h3>Active members by age</h3>
          {s.medianAge != null && <p className="small muted" style={{ marginTop: -4 }}>Median age {s.medianAge}</p>}
          <Bars rows={ages.map((a) => ({ band: a.label, members: a.count }))} x="band" y="members" />
        </div>
        <CountTable title="Where active members live" rows={zips} note="Top zip codes" />
        <CountTable title="How new members joined" rows={sources} unit="Joins" note="Agreement entry source, last 12 months" />
        <CountTable title="All member records by ABC status" rows={statuses} unit="Records" />
      </div>

      <div className="card section table-wrap">
        <h3>Member lookup</h3>
        <form className="row" style={{ marginBottom: 10 }}>
          <input type="text" name="q" defaultValue={q} placeholder="Name, barcode or agreement number" style={{ maxWidth: 360 }} aria-label="Search members" />
          <button className="btn primary" type="submit">Search</button>
        </form>
        {found && <MemberTable rows={found} empty={`No members match "${q}".`} />}
      </div>

      <div className="card section table-wrap">
        <h3>Ghost members: {n(s.ghosts)}</h3>
        <p className="small muted" style={{ marginTop: -4 }}>
          Active members who haven&apos;t checked in for 30+ days, most recently lapsed first, so the front desk can reach
          out before they cancel.{ghosts.length < s.ghosts && <> Showing the first {ghosts.length}.</>}
        </p>
        <MemberTable rows={ghosts} empty="Everyone has been in during the last 30 days." />
      </div>
    </>
  );
}
