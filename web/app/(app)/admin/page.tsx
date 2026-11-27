import { requireMenu } from "@/lib/access";
import { store } from "@/lib/store";
import { GRANTABLE } from "@/lib/menus";
import { ROLE_LABEL, type AppUser, type Role } from "@/lib/types";
import { removeUser, saveUser } from "./actions";

export const metadata = { title: "Admin · Sig Fit Reporting" };

const ROLE_HELP: Record<Role, string> = {
  admin: "Every menu, plus this Admin page",
  executive: "Every reporting menu",
  manager: "Only the menus ticked below",
  staff: "Projects & Tasks, plus any menus ticked below",
};

function UserForm({ user, self }: { user?: AppUser; self?: boolean }) {
  return (
    <form action={saveUser}>
      <div className="form-grid">
        <div className="field">
          <label>Google email</label>
          {user ? (
            <><input type="hidden" name="email" value={user.email} /><div style={{ padding: "8px 0" }}>{user.email}</div></>
          ) : (
            <input name="email" type="email" required placeholder="name@gmail.com" />
          )}
        </div>
        <div className="field">
          <label>Name on the board</label>
          <input name="name" type="text" defaultValue={user?.name} placeholder="e.g. Natalia" required />
        </div>
        <div className="field">
          <label>Role</label>
          <select name="role" defaultValue={user?.role ?? "staff"} disabled={self}>
            {(Object.keys(ROLE_LABEL) as Role[]).map((r) => <option key={r} value={r}>{ROLE_LABEL[r]}: {ROLE_HELP[r]}</option>)}
          </select>
          {self && <input type="hidden" name="role" value="admin" />}
        </div>
        <div className="field">
          <label>Access</label>
          <select name="active" defaultValue={user?.active === false ? "off" : "on"} disabled={self}>
            <option value="on">Can sign in</option>
            <option value="off">Paused</option>
          </select>
        </div>
      </div>
      <div className="field">
        <label>Menus (for Manager and Staff)</label>
        <div className="checks">
          {GRANTABLE.map((m) => (
            <label key={m.key}>
              <input type="checkbox" name="menus" value={m.key} defaultChecked={user?.menus.includes(m.key)} /> {m.label}
            </label>
          ))}
        </div>
      </div>
      <button className="btn primary" type="submit">{user ? "Save" : "Add person"}</button>
    </form>
  );
}

export default async function AdminPage() {
  const me = await requireMenu("admin");
  const users = (await (await store()).listUsers()).sort((a, b) => a.name.localeCompare(b.name));
  const menuLabel = (u: AppUser) =>
    u.role === "admin" || u.role === "executive" ? "All" : GRANTABLE.filter((m) => u.menus.includes(m.key)).map((m) => m.label).join(", ") || "Projects only";
  return (
    <>
      <div className="page-head">
        <div>
          <h1>Admin</h1>
          <div className="muted">Who can sign in and which menus they see. Everyone sees Projects &amp; Tasks and Completed.</div>
        </div>
      </div>

      <div className="card">
        <h2>Add a person</h2>
        <UserForm />
      </div>

      <div className="card section table-wrap">
        <h2>People ({users.length})</h2>
        <table>
          <thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Menus</th><th></th></tr></thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.email}>
                <td><b>{u.name}</b>{!u.active && <> <span className="pill">Paused</span></>}</td>
                <td>{u.email}</td>
                <td>{ROLE_LABEL[u.role]}</td>
                <td className="small">{menuLabel(u)}</td>
                <td>
                  <details>
                    <summary className="btn small">Edit</summary>
                    <div style={{ marginTop: 10, minWidth: 320 }}>
                      <UserForm user={u} self={u.email === me.email} />
                      {u.email !== me.email && (
                        <form action={removeUser} style={{ marginTop: 10 }}>
                          <input type="hidden" name="email" value={u.email} />
                          <button className="btn danger small" type="submit">Remove</button>
                        </form>
                      )}
                    </div>
                  </details>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
