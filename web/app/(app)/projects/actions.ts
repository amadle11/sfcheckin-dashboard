"use server";

import { revalidatePath } from "next/cache";
import { redirect } from "next/navigation";
import { requireUser } from "@/lib/access";
import { store, newId } from "@/lib/store";
import { canEdit, canView } from "@/lib/projects";
import { notifyWatchers } from "@/lib/notify";
import { STATUS_LABEL, type AppUser, type Item, type ItemType, type Priority, type Status, type Visibility } from "@/lib/types";

const PRIORITIES: Priority[] = ["P1", "P2", "P3"];
const STATUSES: Status[] = ["not_started", "in_progress", "blocked", "done"];
const pick = <T extends string>(v: FormDataEntryValue | null, allowed: T[], fallback: T): T =>
  allowed.includes(v as T) ? (v as T) : fallback;
const str = (v: FormDataEntryValue | null, max = 2000) => String(v ?? "").trim().slice(0, max);
const owners = (v: FormDataEntryValue | null) =>
  [...new Set(str(v, 500).split(",").map((s) => s.trim()).filter(Boolean))].slice(0, 10);
const date = (v: FormDataEntryValue | null) => (/^\d{4}-\d{2}-\d{2}$/.test(str(v)) ? str(v) : undefined);

function refresh(id?: string) {
  revalidatePath("/", "layout");
  if (id) revalidatePath(`/projects/${id}`);
}

async function loadEditable(id: string): Promise<{ user: AppUser; item: Item }> {
  const user = await requireUser();
  const item = await (await store()).getItem(id);
  if (!item || !canView(user, item)) redirect("/projects");
  if (!canEdit(user, item)) throw new Error("Only owners or an admin can change this item.");
  return { user, item };
}

async function watchersFor(ownerNames: string[], creator: string): Promise<string[]> {
  const users = await (await store()).listUsers();
  const names = ownerNames.map((n) => n.toLowerCase());
  return [...new Set([creator, ...users.filter((u) => names.includes(u.name.toLowerCase())).map((u) => u.email)])];
}

export async function createItem(form: FormData) {
  const user = await requireUser();
  const s = await store();
  const title = str(form.get("title"), 200);
  if (!title) return;
  const priority = pick(form.get("priority"), PRIORITIES, "P2");
  const ownerList = owners(form.get("owners"));
  const all = await s.listItems();
  const now = new Date().toISOString();
  const item: Item = {
    id: newId(),
    title,
    description: str(form.get("description")),
    type: pick<ItemType>(form.get("type"), ["project", "task"], "task"),
    parentId: str(form.get("parentId")) || undefined,
    owners: ownerList.length ? ownerList : [user.name],
    priority,
    rank: Math.max(-1, ...all.filter((i) => i.priority === priority).map((i) => i.rank)) + 1,
    status: pick(form.get("status"), STATUSES, "not_started"),
    targetDate: date(form.get("targetDate")),
    visibility: pick<Visibility>(form.get("visibility"), ["team", "private"], "team"),
    createdBy: user.email,
    createdAt: now,
    updatedAt: now,
    watchers: [],
    updates: [],
  };
  item.watchers = await watchersFor(item.owners, user.email);
  await s.saveItem(item);
  refresh();
  redirect(`/projects/${item.id}`);
}

export async function editItem(form: FormData) {
  const { user, item } = await loadEditable(str(form.get("id")));
  const before = item.status;
  const status = pick(form.get("status"), STATUSES, item.status);
  const next: Item = {
    ...item,
    title: str(form.get("title"), 200) || item.title,
    description: str(form.get("description")),
    type: pick<ItemType>(form.get("type"), ["project", "task"], item.type),
    parentId: str(form.get("parentId")) || undefined,
    owners: owners(form.get("owners")).length ? owners(form.get("owners")) : item.owners,
    priority: pick(form.get("priority"), PRIORITIES, item.priority),
    status,
    blockedReason: status === "blocked" ? str(form.get("blockedReason"), 300) || item.blockedReason : undefined,
    targetDate: date(form.get("targetDate")),
    visibility: pick<Visibility>(form.get("visibility"), ["team", "private"], item.visibility),
    updatedAt: new Date().toISOString(),
  };
  applyStatus(next, before, status, user, "");
  await (await store()).saveItem(next);
  if (before !== status) {
    await notifyWatchers(next, user.email, `${next.title}: ${STATUS_LABEL[status]}`,
      `${user.name} changed the status from ${STATUS_LABEL[before]} to ${STATUS_LABEL[status]}.` +
      (next.blockedReason ? `\nBlocked: ${next.blockedReason}` : ""));
  }
  refresh(item.id);
  redirect(`/projects/${item.id}`);
}

function applyStatus(item: Item, from: Status, to: Status, user: AppUser, text: string) {
  if (from !== to) {
    item.completedAt = to === "done" ? new Date().toISOString() : undefined;
    item.updates = [
      ...item.updates,
      { id: newId(), at: new Date().toISOString(), author: user.name, authorEmail: user.email, text, statusFrom: from, statusTo: to },
    ];
  } else if (text) {
    item.updates = [...item.updates, { id: newId(), at: new Date().toISOString(), author: user.name, authorEmail: user.email, text }];
  }
}

export async function postUpdate(form: FormData) {
  const { user, item } = await loadEditable(str(form.get("id")));
  const text = str(form.get("text"));
  const before = item.status;
  const status = pick(form.get("status"), STATUSES, item.status);
  if (!text && status === before) return;
  const next: Item = { ...item, status, updatedAt: new Date().toISOString() };
  if (status === "blocked") next.blockedReason = text.slice(0, 300) || item.blockedReason;
  else next.blockedReason = undefined;
  applyStatus(next, before, status, user, text);
  await (await store()).saveItem(next);
  const statusLine = before !== status ? `Status: ${STATUS_LABEL[before]} → ${STATUS_LABEL[status]}\n` : "";
  await notifyWatchers(next, user.email,
    before !== status ? `${next.title}: ${STATUS_LABEL[status]}` : `Update on ${next.title}`,
    `${statusLine}${user.name}: ${text}`);
  refresh(item.id);
}

export async function toggleWatch(form: FormData) {
  const user = await requireUser();
  const s = await store();
  const item = await s.getItem(str(form.get("id")));
  if (!item || !canView(user, item)) return;
  const watching = item.watchers.includes(user.email);
  await s.saveItem({ ...item, watchers: watching ? item.watchers.filter((w) => w !== user.email) : [...item.watchers, user.email] });
  refresh(item.id);
}

/** Swap the order of two items (the arrows on My work and Team). */
export async function swapRank(form: FormData) {
  const user = await requireUser();
  const s = await store();
  const a = await s.getItem(str(form.get("a")));
  const b = await s.getItem(str(form.get("b")));
  if (!a || !b || a.priority !== b.priority || !canEdit(user, a) || !canView(user, b)) return;
  let [ra, rb] = [b.rank, a.rank];
  if (ra === rb) [ra, rb] = [a.rank - 0.5, a.rank];
  await s.saveItem({ ...a, rank: ra });
  await s.saveItem({ ...b, rank: rb });
  refresh();
}

export async function deleteItem(form: FormData) {
  const { user, item } = await loadEditable(str(form.get("id")));
  if (user.role !== "admin" && item.createdBy !== user.email) throw new Error("Only the creator or an admin can delete.");
  // Soft delete: hidden everywhere, but kept in the database in case of a misclick.
  await (await store()).saveItem({ ...item, deletedAt: new Date().toISOString() });
  refresh();
  redirect("/projects");
}
