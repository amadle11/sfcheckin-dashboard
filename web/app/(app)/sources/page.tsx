import { requireMenu } from "@/lib/access";
import { SOURCES } from "@/lib/sources";
import { recentCounts } from "@/lib/bigquery";

export const metadata = { title: "Sources · Sig Fit Reporting" };

export default async function SourcesPage() {
  await requireMenu("sources");
  let lastLoaded: string | null = null;
  try {
    lastLoaded = (await recentCounts()).lastLoaded;
  } catch {
    /* shown as unknown */
  }
  const fmt = (iso: string) =>
    new Date(iso).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/New_York" });
  return (
    <>
      <div className="page-head">
        <div>
          <h1>Sources</h1>
          <div className="muted">Where each report&apos;s data comes from today, and the phase that brings it into the app.</div>
        </div>
      </div>
      <div className="card table-wrap">
        <table>
          <thead><tr><th>Area</th><th>Source</th><th>Today</th><th>Could ABC replace it?</th><th>Phase</th><th>Last refresh</th></tr></thead>
          <tbody>
            {SOURCES.map((s) => (
              <tr key={s.name}>
                <td>{s.area}</td>
                <td><b>{s.name}</b></td>
                <td>{s.today}</td>
                <td className="muted">{s.abc}</td>
                <td>{s.phase ?? ""}</td>
                <td className="small muted">{s.name === "Check-In Dashboard" ? (lastLoaded ? fmt(lastLoaded) : "Unknown") : "Not synced yet"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
