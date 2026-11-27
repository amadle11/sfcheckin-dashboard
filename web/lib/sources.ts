// The project inventory from the build plan: where each area's data comes from today.
// Links to the sheets are kept out of this public repo on purpose.

export interface Source {
  area: string;
  name: string;
  today: string;
  abc: string;
  phase: number | null;
}

export const SOURCES: Source[] = [
  { area: "Check-In", name: "Check-In Dashboard", today: "ABC API → BigQuery (hourly)", abc: "Already on it", phase: 1 },
  { area: "Check-In", name: "Member Check-In Data", today: "Sheet", abc: "Yes, same check-in feed", phase: 1 },
  { area: "Check-In", name: "Checkins by Month", today: "Sheet", abc: "Yes, monthly rollup of the feed", phase: 1 },
  { area: "Members", name: "Net Impact Member Analysis", today: "Sheet", abc: "Likely: joins vs cancels", phase: 2 },
  { area: "Members", name: "SF Member Trends", today: "Sheet", abc: "Likely: ABC members", phase: 2 },
  { area: "Members", name: "Collections Data", today: "Sheet", abc: "Possibly: ABC billing", phase: 5 },
  { area: "Members", name: "Coupons Vs. Sales", today: "Sheet", abc: "Possibly: ABC billing", phase: 5 },
  { area: "Group Fitness", name: "Group Fitness Numbers", today: "Sheet, coaches type counts", abc: "No, rebuild later", phase: 3 },
  { area: "Group Fitness", name: "Group Fitness Summary", today: "Sheet rollup", abc: "Replaced by the app's rollup", phase: 3 },
  { area: "Group Fitness", name: "CROSSFIT/HYROX Daily Sign-In", today: "Sheet, a tab per class", abc: "Maybe, if classes are in ABC Calendars", phase: 3 },
  { area: "Group Fitness", name: "YOGA Daily Sign-In", today: "Sheet, a tab per class", abc: "Maybe, if classes are in ABC Calendars", phase: 3 },
  { area: "Personal Training", name: "Personal Training Data", today: "Sheet", abc: "Possibly, if ABC has PT sessions", phase: 4 },
  { area: "Sales & Commissions", name: "Sig Fit PT Commissions", today: "Sheet", abc: "Partly: inputs from ABC", phase: 4 },
  { area: "Sales & Commissions", name: "Sig Fit Sales Commission", today: "Sheet", abc: "Partly: new sales from ABC", phase: 4 },
  { area: "Marketing", name: "Free Personal Training form", today: "Google Form", abc: "Match leads to ABC members", phase: 4 },
  { area: "Juice Bar", name: "Juice Bar Daily Sales", today: "Sheet", abc: "Only if the bar rings through ABC POS", phase: 6 },
  { area: "Juice Bar", name: "Fuel Chef Invoices", today: "Sheet", abc: "No", phase: 6 },
  { area: "Juice Bar", name: "Meal Inventory", today: "Sheet", abc: "No", phase: 6 },
  { area: "Staff", name: "SF Time-Off Calendar", today: "Sheet + request form", abc: "No", phase: 6 },
  { area: "Staff", name: "INTERNAL SF Floor Staff Time-Off", today: "Sheet", abc: "No", phase: 6 },
];
