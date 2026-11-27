import Link from "next/link";
import { PRIORITY_LABEL, STATUS_LABEL, type Item } from "@/lib/types";
import { fmtDate } from "@/lib/projects";
import { swapRank } from "@/app/(app)/projects/actions";

export function Pills({ item, showPriority = true }: { item: Item; showPriority?: boolean }) {
  return (
    <span className="row" style={{ gap: 6 }}>
      {showPriority && <span className={`pill ${item.priority}`}>{item.priority}</span>}
      <span className={`pill ${item.status}`}>{STATUS_LABEL[item.status]}</span>
      {item.type === "task" && <span className="pill">Task</span>}
      {item.visibility === "private" && <span className="pill private">Private</span>}
    </span>
  );
}

export function ProjectCard({ item }: { item: Item }) {
  return (
    <Link className="proj-card" href={`/projects/${item.id}`}>
      <Pills item={item} showPriority={false} />
      <div className="t">{item.title}</div>
      <div className="small muted">
        {item.owners.join(", ")}
        {item.targetDate && <> · due {fmtDate(item.targetDate)}</>}
      </div>
      {item.status === "blocked" && item.blockedReason && (
        <div className="small" style={{ color: "var(--bad)", marginTop: 6 }}>Blocked: {item.blockedReason}</div>
      )}
    </Link>
  );
}

export function ItemRow({
  item, prev, next, canMove, parentTitle,
}: { item: Item; prev?: Item; next?: Item; canMove?: boolean; parentTitle?: string }) {
  const last = item.updates.at(-1);
  return (
    <div className="item-row">
      {canMove ? (
        <div className="rank-btns">
          <form action={swapRank}>
            <input type="hidden" name="a" value={item.id} />
            <input type="hidden" name="b" value={prev?.id ?? ""} />
            <button disabled={!prev} title="Move up" aria-label="Move up">▲</button>
          </form>
          <form action={swapRank}>
            <input type="hidden" name="a" value={item.id} />
            <input type="hidden" name="b" value={next?.id ?? ""} />
            <button disabled={!next} title="Move down" aria-label="Move down">▼</button>
          </form>
        </div>
      ) : (
        <span />
      )}
      <div>
        <Link className="title" href={`/projects/${item.id}`}>{item.title}</Link>
        <div className="meta">
          {item.owners.join(", ")}
          {parentTitle && <> · part of {parentTitle}</>}
          {item.targetDate && <> · due {fmtDate(item.targetDate)}</>}
          {last?.text && <> · latest: {last.text.length > 80 ? last.text.slice(0, 80) + "…" : last.text}</>}
        </div>
      </div>
      <Pills item={item} showPriority={false} />
    </div>
  );
}

export function PriorityGroups({
  items, canMove, parents,
}: { items: Item[]; canMove: (i: Item) => boolean; parents: Map<string, string> }) {
  const groups = (["P1", "P2", "P3"] as const).map((p) => ({ p, list: items.filter((i) => i.priority === p) }));
  if (items.length === 0) return <div className="empty">Nothing here yet.</div>;
  return (
    <>
      {groups
        .filter((g) => g.list.length)
        .map(({ p, list }) => (
          <div key={p}>
            <div className="group-head">
              <span className={`pill ${p}`}>{PRIORITY_LABEL[p]}</span>
              <span className="small muted">{list.length}</span>
            </div>
            {list.map((item, i) => (
              <ItemRow
                key={item.id}
                item={item}
                prev={list[i - 1]}
                next={list[i + 1]}
                canMove={canMove(item)}
                parentTitle={item.parentId ? parents.get(item.parentId) : undefined}
              />
            ))}
          </div>
        ))}
    </>
  );
}
