import type { Item } from "./types";

// The starting items on the Projects board, from the build plan.
export const SEED_ITEMS: Array<Pick<Item, "title" | "type" | "owners" | "priority" | "status"> & Partial<Item>> = [
  {
    title: "Merge all 4 websites into one and do a complete rebuild",
    type: "project",
    owners: ["Pablo", "Anthony"],
    priority: "P1",
    status: "not_started",
  },
  {
    title: "Rebuild Group Fitness Sign-In Process",
    type: "project",
    owners: ["Anthony"],
    priority: "P1",
    status: "not_started",
  },
  {
    title: "Build Sig Fit Reporting web app",
    type: "project",
    owners: ["Anthony"],
    priority: "P1",
    status: "in_progress",
    description: "One reporting app for Sig Fit: executive summary, a menu per area, per-person access and this projects board.",
  },
  {
    title: "Build WebApp for All Group Fitness",
    type: "project",
    owners: ["Anthony"],
    priority: "P2",
    status: "not_started",
  },
];
