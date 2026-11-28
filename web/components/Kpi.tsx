export function pct(now: number, before: number): number | null {
  if (!before) return null;
  return ((now - before) / before) * 100;
}

export function Kpi({ label, value, change, note }: { label: string; value: string; change?: number | null; note?: string }) {
  return (
    <div className="card kpi">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      {change != null && (
        <div className={`delta ${change >= 0 ? "up" : "down"}`}>
          {change >= 0 ? "▲" : "▼"} {Math.abs(change).toFixed(1)}%{note && <span className="muted"> {note}</span>}
        </div>
      )}
      {change == null && note && <div className="delta muted">{note}</div>}
    </div>
  );
}

export function DataError({ what }: { what: string }) {
  return (
    <div className="card">
      <h3>Can&apos;t load {what} right now</h3>
      <p className="small muted" style={{ margin: 0 }}>
        The app couldn&apos;t reach BigQuery. If this keeps happening, check that the app&apos;s service account can read the
        <code> sigfit.checkInData</code> dataset (see web/DEPLOY.md).
      </p>
    </div>
  );
}
