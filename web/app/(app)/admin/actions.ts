"use server";

import { revalidatePath } from "next/cache";
import { requireMenu } from "@/lib/access";
import { store } from "@/lib/store";
import { GRANTABLE } from "@/lib/menus";
import type { AppUser, Role } from "@/lib/types";

const ROLES: Role[] = ["admin", "executive", "manager", "staff"];

export async function saveUser(form: FormData) {
  const admin = await requireMenu("admin");
  const email = String(form.get("email") ?? "").trim().toLowerCase();
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) throw new Error("Enter a valid email.");
  const s = await store();
  const existing = await s.getUser(email);
  const role = ROLES.includes(form.get("role") as Role) ? (form.get("role") as Role) : "staff";
  if (email === admin.email && role !== "admin") throw new Error("You can't remove your own admin role.");
  const allowed = new Set(GRANTABLE.map((m) => m.key as string));
  const user: AppUser = {
    email,
    name: String(form.get("name") ?? "").trim().slice(0, 60) || existing?.name || email.split("@")[0],
    role,
    menus: form.getAll("menus").map(String).filter((k) => allowed.has(k)),
    active: form.get("active") !== "off",
    createdAt: existing?.createdAt ?? new Date().toISOString(),
  };
  await s.saveUser(user);
  revalidatePath("/admin");
}

export async function removeUser(form: FormData) {
  const admin = await requireMenu("admin");
  const email = String(form.get("email") ?? "").toLowerCase();
  if (email === admin.email) throw new Error("You can't remove yourself.");
  await (await store()).deleteUser(email);
  revalidatePath("/admin");
}
