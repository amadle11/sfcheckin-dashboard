// Date helpers in club time (America/New_York).

export const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

export function todayISO(): string {
  return new Date().toLocaleDateString("en-CA", { timeZone: "America/New_York" });
}

function shift(iso: string, days: number): string {
  const d = new Date(iso + "T12:00:00Z");
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

export const RANGES = {
  "30d": "Last 30 days",
  "90d": "Last 90 days",
  ytd: "This year",
  "last-year": "Last year",
  "12m": "Last 12 months",
} as const;
export type RangeKey = keyof typeof RANGES;

export function rangeDates(key: RangeKey): { start: string; end: string } {
  const today = todayISO();
  const y = Number(today.slice(0, 4));
  switch (key) {
    case "30d": return { start: shift(today, -29), end: today };
    case "90d": return { start: shift(today, -89), end: today };
    case "ytd": return { start: `${y}-01-01`, end: today };
    case "last-year": return { start: `${y - 1}-01-01`, end: `${y - 1}-12-31` };
    case "12m": return { start: shift(today, -364), end: today };
  }
}
