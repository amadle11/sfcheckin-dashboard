import Link from "next/link";
import Nav from "@/components/Nav";
import { requireUser, visibleMenus } from "@/lib/access";
import { ROLE_LABEL } from "@/lib/types";

export const dynamic = "force-dynamic";

export default async function AppLayout({ children }: { children: React.ReactNode }) {
  const user = await requireUser();
  const items = visibleMenus(user).map((m) => ({ href: m.href, label: m.label, section: m.section, phase: m.phase }));
  const devMode = process.env.NODE_ENV !== "production" && !!process.env.DEV_LOGIN_EMAIL;
  return (
    <div className="shell">
      <aside className="sidebar">
        <Link className="brand" href="/">
          <img src="/sfLogo.png" alt="" />
          <span>Sig Fit<br />Reporting</span>
        </Link>
        <Nav items={items} />
        <div className="me">
          <b>{user.name}</b>
          {ROLE_LABEL[user.role]}
          {!devMode && (
            <form
              action={async () => {
                "use server";
                const { signOut } = await import("@/auth");
                await signOut({ redirectTo: "/signin" });
              }}
            >
              <button type="submit">Sign out</button>
            </form>
          )}
        </div>
      </aside>
      <main className="main">{children}</main>
    </div>
  );
}
