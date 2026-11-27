import "server-only";
import { cache } from "react";
import { redirect } from "next/navigation";
import { store } from "./store";
import { MENUS, type Menu, type MenuKey } from "./menus";
import type { AppUser } from "./types";

const adminEmails = () =>
  (process.env.ADMIN_EMAILS || "")
    .split(",")
    .map((e) => e.trim().toLowerCase())
    .filter(Boolean);

export async function isAllowedEmail(email: string): Promise<boolean> {
  if (adminEmails().includes(email)) return true;
  const user = await (await store()).getUser(email);
  return !!user?.active;
}

function devEmail(): string | null {
  if (process.env.NODE_ENV === "production") return null;
  return process.env.DEV_LOGIN_EMAIL?.toLowerCase() || null;
}

/** The signed-in person with their role and menus, or null. */
export const getCurrentUser = cache(async (): Promise<AppUser | null> => {
  let email = devEmail();
  let name: string | undefined;
  if (!email) {
    const { auth } = await import("@/auth");
    const session = await auth();
    email = session?.user?.email?.toLowerCase() ?? null;
    name = session?.user?.name ?? undefined;
  }
  if (!email) return null;

  const s = await store();
  const existing = await s.getUser(email);
  if (adminEmails().includes(email)) {
    // Admins from ADMIN_EMAILS get a record on first sign-in so they show on the Admin page.
    if (!existing || existing.role !== "admin" || !existing.active) {
      const user: AppUser = {
        email,
        name: existing?.name || name?.split(" ")[0] || email.split("@")[0],
        role: "admin",
        menus: existing?.menus ?? [],
        active: true,
        createdAt: existing?.createdAt ?? new Date().toISOString(),
      };
      await s.saveUser(user);
      return user;
    }
    return existing;
  }
  return existing?.active ? existing : null;
});

export function canSee(user: AppUser, menu: Menu): boolean {
  if (menu.adminOnly) return user.role === "admin";
  if (menu.open) return true;
  if (user.role === "admin" || user.role === "executive") return true;
  return user.menus.includes(menu.key);
}

export function visibleMenus(user: AppUser): Menu[] {
  return MENUS.filter((m) => canSee(user, m));
}

/** Use at the top of a page: sends people without access away. */
export async function requireUser(): Promise<AppUser> {
  const user = await getCurrentUser();
  if (!user) redirect("/signin");
  return user;
}

export async function requireMenu(key: MenuKey): Promise<AppUser> {
  const user = await requireUser();
  const menu = MENUS.find((m) => m.key === key)!;
  if (!canSee(user, menu)) redirect("/projects?denied=" + key);
  return user;
}
