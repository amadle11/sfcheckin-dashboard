import type { Count } from "@/lib/members";

export function CountTable({ title, rows, unit = "Members", note }: { title: string; rows: Count[]; unit?: string; note?: string }) {
  const total = rows.reduce((s, r) => s + r.count, 0) || 1;
  return (
    <div className="card table-wrap">
      <h3>{title}</h3>
      {note && <p className="small muted" style={{ marginTop: -4 }}>{note}</p>}
      {rows.length === 0 ? <div className="empty">No data yet.</div> : (
        <table>
          <thead><tr><th></th><th style={{ textAlign: "right" }}>{unit}</th><th style={{ textAlign: "right" }}>Share</th></tr></thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.label}>
                <td>{r.label}</td>
                <td style={{ textAlign: "right" }}>{r.count.toLocaleString("en-US")}</td>
                <td style={{ textAlign: "right" }} className="muted">{((r.count / total) * 100).toFixed(0)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
