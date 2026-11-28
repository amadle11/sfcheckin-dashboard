import Link from "next/link";
import { notFound } from "next/navigation";
import { requireUser } from "@/lib/access";
import { store } from "@/lib/store";
import { byPriority, canEdit, canView, fmtDate, fmtDateTime } from "@/lib/projects";
import { Pills, ItemRow } from "@/components/ItemBits";
import { ItemFields } from "@/components/ItemForm";
import { STATUS_LABEL } from "@/lib/types";
import { deleteItem, editItem, postUpdate, toggleWatch } from "../actions";

export default async function ItemPage({ params }: { params: Promise<{ id: string }> }) {
  const user = await requireUser();
  const { id } = await params;
  const s = await store();
  const item = await s.getItem(id);
  if (!item || !canView(user, item)) notFound();
  const all = (await s.listItems()).filter((i) => canView(user, i));
  const parent = item.parentId ? all.find((i) => i.id === item.parentId) : undefined;
  const children = all.filter((i) => i.parentId === item.id).sort(byPriority);
  const projects = all.filter((i) => i.type === "project" && i.status !== "done");
  const editable = canEdit(user, item);
  const watching = item.watchers.includes(user.email);
  const users = await s.listUsers();
  const watcherNames = item.watchers.map((w) => users.find((u) => u.email === w)?.name ?? w.split("@")[0]);

  return (
    <>
      <div className="small" style={{ marginBottom: 10 }}>
        <Link href="/projects">← Projects &amp; Tasks</Link>
        {parent && <> · part of <Link href={`/projects/${parent.id}`}>{parent.title}</Link></>}
      </div>
      <div className="page-head">
        <div>
          <h1>{item.title}</h1>
          <div className="row" style={{ marginTop: 8 }}>
            <Pills item={item} />
            <span className="small muted">
              {item.type === "project" ? "Project" : "Task"} · {item.owners.join(", ")}
              {item.targetDate && <> · due {fmtDate(item.targetDate)}</>}
              {item.completedAt && <> · finished {fmtDate(item.completedAt)}</>}
            </span>
          </div>
        </div>
        <form action={toggleWatch} className="row">
          <input type="hidden" name="id" value={item.id} />
          <span className="small muted" title={watcherNames.join(", ")}>{item.watchers.length} watching</span>
          <button className={`btn ${watching ? "" : "primary"}`} type="submit">{watching ? "Stop watching" : "Watch"}</button>
        </form>
      </div>

      {item.status === "blocked" && item.blockedReason && <div className="notice">Blocked: {item.blockedReason}</div>}
      {item.description && <div className="card" style={{ whiteSpace: "pre-wrap", marginBottom: 14 }}>{item.description}</div>}

      <div className="grid grid-2">
        <div className="card">
          <h2>Updates</h2>
          {editable && (
            <form key={item.updatedAt} action={postUpdate} style={{ marginBottom: 18 }}>
              <input type="hidden" name="id" value={item.id} />
              <div className="field">
                <textarea name="text" placeholder="What changed? Watchers get an email." aria-label="Update" />
              </div>
              <div className="row">
                <select name="status" defaultValue={item.status} aria-label="Status" style={{ width: "auto" }}>
                  {Object.entries(STATUS_LABEL).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
                </select>
                <button className="btn primary" type="submit">Post update</button>
              </div>
            </form>
          )}
          {item.updates.length === 0 ? (
            <div className="empty">No updates yet.</div>
          ) : (
            <div className="log">
              {[...item.updates].reverse().map((u) => (
                <div className="entry" key={u.id}>
                  <div className="who">{u.author} · {fmtDateTime(u.at)}</div>
                  {u.statusTo && (
                    <div className="small"><b>{u.statusFrom ? STATUS_LABEL[u.statusFrom] : ""} → {STATUS_LABEL[u.statusTo]}</b></div>
                  )}
                  {u.text && <div style={{ whiteSpace: "pre-wrap" }}>{u.text}</div>}
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="grid" style={{ alignContent: "start" }}>
          {item.type === "project" && (
            <div className="card">
              <h2>Tasks in this project</h2>
              {children.length ? children.map((c) => <ItemRow key={c.id} item={c} />) : <div className="empty">No tasks linked yet.</div>}
            </div>
          )}
          {editable && (
            <div className="card">
              <details>
                <summary style={{ cursor: "pointer", fontWeight: 600 }}>Edit details</summary>
                <form key={item.updatedAt} action={editItem} style={{ marginTop: 14 }}>
                  <input type="hidden" name="id" value={item.id} />
                  <ItemFields item={item} projects={projects} />
                  <button className="btn primary" type="submit">Save</button>
                </form>
                {(user.role === "admin" || item.createdBy === user.email) && (
                  <form action={deleteItem} style={{ marginTop: 14 }}>
                    <input type="hidden" name="id" value={item.id} />
                    <button className="btn danger small" type="submit">Delete this item</button>
                  </form>
                )}
              </details>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
