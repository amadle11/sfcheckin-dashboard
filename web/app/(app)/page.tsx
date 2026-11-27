import Link from "next/link";
import { canSee, requireUser } from "@/lib/access";
import { menuByKey, MENUS } from "@/lib/menus";
import { store } from "@/lib/store";
import { byPriority, canView, isOpen, isOwner } from "@/lib/projects";
import { recentCounts, yearToDate } from "@/lib/bigquery";
import { ProjectCard, ItemRow } from "@/components/ItemBits";
import { Kpi, DataError, pct } from "@/components/Kpi";

const n = (v: number) => Math.round(v).toLocaleString("en-US");

async function ExecutiveSummary() {
  let data;
  try {
    data = await Promise.all([recentCounts(), yearToDate()]);
  } catch (err) {
    console.error("[home] BigQuery error", err);
    return <DataError what="the Executive Summary" />;
  }
  const [recent, ytd] = data;
  const [prev, cur] = ytd.years.slice(-2);
  return (
    <>
      <div className="grid grid-4">
        <Kpi label="Check-ins today" value={n(recent.today)} />
        <Kpi label="Check-ins, last 7 days" value={n(recent.last7)} change={pct(recent.last7, recent.prior7)} note="vs prior 7 days" />
        {cur && <Kpi label={`Check-ins, ${cur.year} to date`} value={n(cur.checkins)} change={prev ? pct(cur.checkins, prev.checkins) : null} note={prev ? `vs ${prev.year}` : undefined} />}
        {cur && <Kpi label={`Unique members, ${cur.year} to date`} value={n(cur.uniqueMembers)} change={prev ? pct(cur.uniqueMembers, prev.uniqueMembers) : null} note={prev ? `vs ${prev.year}` : undefined} />}
      </div>
      <p className="small muted">
        Members, class attendance, PT, revenue and juice bar margin join this summary as each area is built:{" "}
        {MENUS.filter((m) => m.phase).map((m) => `${m.label} (Phase ${m.phase})`).join(", ")}.{" "}
        <Link href="/checkin">Open Check-In →</Link>
      </p>
    </>
  );
}

export default async function Home() {
  const user = await requireUser();
  const items = (await (await store()).listItems()).filter((i) => canView(user, i) && isOpen(i)).sort(byPriority);
  const priority = items.filter((i) => i.type === "project" && i.priority === "P1" && i.visibility === "team");
  const mine = items.filter((i) => isOwner(user, i)).slice(0, 5);
  const showExec = canSee(user, menuByKey("executive")!);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Hi {user.name}</h1>
          <div className="muted">
            {new Date().toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", timeZone: "America/New_York" })}
          </div>
        </div>
        <Link className="btn" href="/projects">All projects &amp; tasks</Link>
      </div>

      <h2>Priority projects</h2>
      {priority.length ? (
        <div className="strip">{priority.map((i) => <ProjectCard key={i.id} item={i} />)}</div>
      ) : (
        <div className="empty">No P1 projects right now.</div>
      )}

      {mine.length > 0 && (
        <div className="card section">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <h2 style={{ margin: 0 }}>Your next up</h2>
            <Link className="small" href="/projects">My work →</Link>
          </div>
          {mine.map((i) => <ItemRow key={i.id} item={i} />)}
        </div>
      )}

      {showExec && (
        <div className="section">
          <h2>Executive summary</h2>
          <ExecutiveSummary />
        </div>
      )}
    </>
  );
}
