// Every menu in the app. Access is granted per menu on the Admin page.
// "open" menus are visible to everyone who can sign in.

export type MenuKey =
  | "projects"
  | "completed"
  | "executive"
  | "checkin"
  | "members"
  | "group-fitness"
  | "personal-training"
  | "sales"
  | "billing"
  | "juice-bar"
  | "staff"
  | "sources"
  | "admin";

export interface Menu {
  key: MenuKey;
  label: string;
  href: string;
  section: "Work" | "Reporting" | "Settings";
  open?: boolean; // everyone sees it
  adminOnly?: boolean;
  phase?: number; // planned build phase when the page isn't built yet
  blurb?: string;
}

export const MENUS: Menu[] = [
  { key: "projects", label: "Projects & Tasks", href: "/projects", section: "Work", open: true },
  { key: "completed", label: "Completed", href: "/completed", section: "Work", open: true },
  { key: "executive", label: "Executive Summary", href: "/", section: "Reporting" },
  { key: "checkin", label: "Check-In", href: "/checkin", section: "Reporting" },
  {
    key: "members", label: "Members", href: "/members", section: "Reporting",
    blurb: "Active members, joins vs cancels, net impact and demographics from ABC. Replaces SF Member Trends and Net Impact Member Analysis.",
  },
  {
    key: "group-fitness", label: "Group Fitness", href: "/area/group-fitness", section: "Reporting", phase: 3,
    blurb: "Attendance by class, instructor and week from Group Fitness Numbers and the CrossFit/HYROX and Yoga sign-ins.",
  },
  {
    key: "personal-training", label: "Personal Training", href: "/area/personal-training", section: "Reporting", phase: 4,
    blurb: "PT sessions and Free PT leads matched to members who joined.",
  },
  {
    key: "sales", label: "Sales & Commissions", href: "/area/sales", section: "Reporting", phase: 4,
    blurb: "Sales and PT commissions, with inputs from ABC where possible.",
  },
  {
    key: "billing", label: "Billing", href: "/billing", section: "Reporting",
    blurb: "Collections, freezes and coupons vs sales.",
  },
  {
    key: "juice-bar", label: "Juice Bar", href: "/area/juice-bar", section: "Reporting", phase: 6,
    blurb: "Daily sales, Fuel Chef cost and meal inventory, for margin by week.",
  },
  {
    key: "staff", label: "Staff", href: "/area/staff", section: "Reporting", phase: 6,
    blurb: "Who is out this week, from the time-off calendar and form.",
  },
  { key: "sources", label: "Sources", href: "/sources", section: "Settings" },
  { key: "admin", label: "Admin", href: "/admin", section: "Settings", adminOnly: true },
];

/** Menus that can be ticked per person on the Admin page. */
export const GRANTABLE: Menu[] = MENUS.filter((m) => !m.open && !m.adminOnly);

export function menuByKey(key: string): Menu | undefined {
  return MENUS.find((m) => m.key === key);
}
