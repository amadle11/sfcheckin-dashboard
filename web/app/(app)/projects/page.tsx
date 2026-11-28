import Link from "next/link";
import { requireUser } from "@/lib/access";
import { store } from "@/lib/store";
import { byPriority, canEdit, canView, isOpen, isOwner } from "@/lib/projects";
import { menuByKey } from "@/lib/menus";
import { ProjectCard, PriorityGroups, ItemRow } from "@/components/ItemBits";
import { ItemFields } from "@/components/ItemForm";
import { createItem } from "./actions";

export const metadata = { title: "Projects & Tasks · Sig Fit Reporting" };

type View = "mine" | "team" | "all";

export default async function ProjectsPage({ searchParams }: { searchParams: Promise<{ view?: string; denied?: string }> }) {
  const user = await requireUser();
  const { view: v, denied } = await searchParams;
  const view: View = v === "team" || v === "all" ? v : "mine";
  const all = (await (await store()).listItems()).filter((i) => canView(user, i)).sort(byPriority);
  const open = all.filter(isOpen);
  const projects = all.filter((i) => i.type === "project" && isOpen(i));
  const parents = new Map(all.map((i) => [i.id, i.title]));
  const priorityProjects = open.filter((i) => i.type === "project" && i.priority === "P1" && i.visibility === "team");
  const mine = open.filter((i) => isOwner(user, i));

  // Team view: each person's open items; an item with two owners shows under both.
  const people = [...new Set(open.flatMap((i) => i.owners))].sort((a, b) => a.localeCompare(b));

  return (
    <>
      {denied && <div className="notice">You don&apos;t have access to {menuByKey(denied)?.label ?? "that page"}. Ask an admin if you need it.</div>}
      <div className="page-head">
        <div>
          <h1>Projects &amp; Tasks</h1>
          <div className="muted">What everyone has going on, in priority order.</div>
        </div>
      </div>

      <h2>Priority projects</h2>
      {priorityProjects.length ? (
        <div className="strip">{priorityProjects.map((i) => <ProjectCard key={i.id} item={i} />)}</div>
      ) : (
        <div className="empty">No P1 projects right now.</div>
      )}

      <div className="section card">
        <details className="add">
          <summary className="btn primary">+ Add a project or task</summary>
          <form action={createItem} style={{ marginTop: 16 }}>
            <ItemFields projects={projects} defaultOwner={user.name} />
            <button className="btn primary" type="submit">Add</button>
          </form>
        </details>
      </div>

      <div className="section card">
        <div className="tabs">
          <Link href="/projects" className={view === "mine" ? "active" : ""}>My work ({mine.length})</Link>
          <Link href="/projects?view=team" className={view === "team" ? "active" : ""}>Team</Link>
          <Link href="/projects?view=all" className={view === "all" ? "active" : ""}>All open ({open.length})</Link>
        </div>

        {view === "mine" && (
          <>
            <p className="small muted" style={{ marginTop: 0 }}>
              Everything with <b>{user.name}</b> as an owner. Use the arrows to put your list in order.
            </p>
            <PriorityGroups items={mine} canMove={(i) => canEdit(user, i)} parents={parents} />
          </>
        )}

        {view === "team" && (
          people.length === 0 ? <div className="empty">No open items.</div> :
          people.map((person) => {
            const list = open.filter((i) => i.owners.includes(person));
            return (
              <div key={person} style={{ marginBottom: 18 }}>
                <div className="group-head"><h3 style={{ margin: 0 }}>{person}</h3><span className="small muted">{list.length} open</span></div>
                {list.map((item) => (
                  <ItemRow key={item.id} item={item} parentTitle={item.parentId ? parents.get(item.parentId) : undefined} />
                ))}
              </div>
            );
          })
        )}

        {view === "all" && <PriorityGroups items={open} canMove={(i) => canEdit(user, i)} parents={parents} />}
      </div>
    </>
  );
}
