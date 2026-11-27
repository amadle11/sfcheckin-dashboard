import Link from "next/link";
import { requireUser } from "@/lib/access";
import { store } from "@/lib/store";
import { canView, fmtDate } from "@/lib/projects";

export const metadata = { title: "Completed · Sig Fit Reporting" };

export default async function CompletedPage() {
  const user = await requireUser();
  const done = (await (await store()).listItems())
    .filter((i) => canView(user, i) && i.type === "project" && i.status === "done")
    .sort((a, b) => (b.completedAt ?? "").localeCompare(a.completedAt ?? ""));
  return (
    <>
      <div className="page-head">
        <div>
          <h1>Completed</h1>
          <div className="muted">Major projects that are finished. Tasks don&apos;t show here.</div>
        </div>
      </div>
      <div className="card table-wrap">
        {done.length === 0 ? (
          <div className="empty">No completed projects yet. When a project&apos;s status is set to Done, it moves here.</div>
        ) : (
          <table>
            <thead><tr><th>Project</th><th>Owners</th><th>Finished</th><th>Final note</th></tr></thead>
            <tbody>
              {done.map((i) => {
                const note = [...i.updates].reverse().find((u) => u.text)?.text;
                return (
                  <tr key={i.id}>
                    <td><Link href={`/projects/${i.id}`}><b>{i.title}</b></Link></td>
                    <td>{i.owners.join(", ")}</td>
                    <td style={{ whiteSpace: "nowrap" }}>{fmtDate(i.completedAt)}</td>
                    <td className="muted">{note ?? ""}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </>
  );
}
