import type { Item } from "@/lib/types";

/** Fields shared by the New item and Edit forms. */
export function ItemFields({ item, projects, defaultOwner }: { item?: Item; projects: Item[]; defaultOwner?: string }) {
  return (
    <>
      <div className="field">
        <label htmlFor="title">Title</label>
        <input id="title" name="title" type="text" required maxLength={200} defaultValue={item?.title} placeholder="e.g. Rebuild Group Fitness Sign-In Process" />
      </div>
      <div className="form-grid">
        <div className="field">
          <label htmlFor="type">Type</label>
          <select id="type" name="type" defaultValue={item?.type ?? "task"}>
            <option value="project">Project (major work)</option>
            <option value="task">Task</option>
          </select>
        </div>
        <div className="field">
          <label htmlFor="priority">Priority</label>
          <select id="priority" name="priority" defaultValue={item?.priority ?? "P2"}>
            <option value="P1">P1 · Now</option>
            <option value="P2">P2 · Next</option>
            <option value="P3">P3 · Later</option>
          </select>
        </div>
        <div className="field">
          <label htmlFor="status">Status</label>
          <select id="status" name="status" defaultValue={item?.status ?? "not_started"}>
            <option value="not_started">Not started</option>
            <option value="in_progress">In progress</option>
            <option value="blocked">Blocked</option>
            <option value="done">Done</option>
          </select>
        </div>
        <div className="field">
          <label htmlFor="targetDate">Target date</label>
          <input id="targetDate" name="targetDate" type="date" defaultValue={item?.targetDate} />
        </div>
      </div>
      <div className="form-grid">
        <div className="field">
          <label htmlFor="owners">Owners (comma-separated names)</label>
          <input id="owners" name="owners" type="text" defaultValue={item?.owners.join(", ") ?? defaultOwner} />
        </div>
        <div className="field">
          <label htmlFor="parentId">Part of project</label>
          <select id="parentId" name="parentId" defaultValue={item?.parentId ?? ""}>
            <option value="">None</option>
            {projects.filter((p) => p.id !== item?.id).map((p) => (
              <option key={p.id} value={p.id}>{p.title}</option>
            ))}
          </select>
        </div>
        <div className="field">
          <label htmlFor="visibility">Who can see it</label>
          <select id="visibility" name="visibility" defaultValue={item?.visibility ?? "team"}>
            <option value="team">Team</option>
            <option value="private">Private (owners and admins)</option>
          </select>
        </div>
      </div>
      {item?.status === "blocked" && (
        <div className="field">
          <label htmlFor="blockedReason">Blocked because</label>
          <input id="blockedReason" name="blockedReason" type="text" maxLength={300} defaultValue={item.blockedReason} />
        </div>
      )}
      <div className="field">
        <label htmlFor="description">Notes</label>
        <textarea id="description" name="description" defaultValue={item?.description} placeholder="What it is and what done looks like" />
      </div>
    </>
  );
}
