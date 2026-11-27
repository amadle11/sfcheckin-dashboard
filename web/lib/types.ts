export type Role = "admin" | "executive" | "manager" | "staff";

export interface AppUser {
  email: string; // lowercased, also the id
  name: string; // shown on the board and used to match owners
  role: Role;
  menus: string[]; // menu keys, used for manager and staff
  active: boolean;
  createdAt: string;
}

export type ItemType = "project" | "task";
export type Priority = "P1" | "P2" | "P3";
export type Status = "not_started" | "in_progress" | "blocked" | "done";
export type Visibility = "team" | "private";

export interface ItemUpdate {
  id: string;
  at: string;
  author: string; // display name
  authorEmail: string;
  text: string;
  statusFrom?: Status;
  statusTo?: Status;
}

export interface Item {
  id: string;
  title: string;
  description: string;
  type: ItemType;
  parentId?: string; // a task can belong to a project
  owners: string[]; // display names
  priority: Priority;
  rank: number; // order within a priority, lower comes first
  status: Status;
  blockedReason?: string;
  targetDate?: string; // YYYY-MM-DD
  visibility: Visibility;
  createdBy: string; // email
  createdAt: string;
  updatedAt: string;
  completedAt?: string;
  watchers: string[]; // emails
  deletedAt?: string;
  updates: ItemUpdate[];
}

export const STATUS_LABEL: Record<Status, string> = {
  not_started: "Not started",
  in_progress: "In progress",
  blocked: "Blocked",
  done: "Done",
};

export const PRIORITY_LABEL: Record<Priority, string> = {
  P1: "P1 · Now",
  P2: "P2 · Next",
  P3: "P3 · Later",
};

export const ROLE_LABEL: Record<Role, string> = {
  admin: "Admin",
  executive: "Executive",
  manager: "Manager",
  staff: "Staff",
};
