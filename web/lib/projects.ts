import "server-only";
import type { AppUser, Item, Priority } from "./types";

const PRIORITY_ORDER: Record<Priority, number> = { P1: 0, P2: 1, P3: 2 };

export function canView(user: AppUser, item: Item): boolean {
  if (item.deletedAt) return false;
  if (item.visibility === "team") return true;
  return user.role === "admin" || item.createdBy === user.email || isOwner(user, item);
}

export function isOwner(user: AppUser, item: Item): boolean {
  const me = user.name.trim().toLowerCase();
  return item.owners.some((o) => o.trim().toLowerCase() === me);
}

export function canEdit(user: AppUser, item: Item): boolean {
  return user.role === "admin" || item.createdBy === user.email || isOwner(user, item);
}

export function byPriority(a: Item, b: Item): number {
  return PRIORITY_ORDER[a.priority] - PRIORITY_ORDER[b.priority] || a.rank - b.rank || a.createdAt.localeCompare(b.createdAt);
}

export const isOpen = (i: Item) => i.status !== "done";

export function fmtDate(iso?: string): string {
  if (!iso) return "";
  const d = iso.length === 10 ? new Date(iso + "T12:00:00") : new Date(iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "America/New_York" });
}

export function fmtDateTime(iso: string): string {
  return new Date(iso).toLocaleString("en-US", {
    month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/New_York",
  });
}
