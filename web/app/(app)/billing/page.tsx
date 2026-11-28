import { requireMenu } from "@/lib/access";
import { memberSummary, pastDueByType, pastDueTrend } from "@/lib/members";
import { Kpi } from "@/components/Kpi";
import { TrendLine } from "@/components/Charts";

export const metadata = { title: "Billing · Sig Fit Reporting" };

const n = (v: number) => Math.round(v).toLocaleString("en-US");
const usd = (v: number) => v.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });

export default async function BillingPage() {
  await requireMenu("billing");
  let data;
  try {
    data = await Promise.all([memberSummary(), pastDueByType(), pastDueTrend()]);
  } catch (err) {
    console.error("[billing] BigQuery error", err);
    return (
      <><h1>Billing</h1><div className="card section"><h3>Billing data isn&apos;t ready yet</h3>
        <p className="small muted" style={{ margin: 0 }}>It comes from the daily ABC member refresh. Run the ABC workflow with mode = members to load it now.</p></div></>
    );
  }
  const [s, types, trend] = data;
  return (
    <>
      <div className="page-head">
        <div>
          <h1>Billing</h1>
          <div className="muted">Past-due balances on active members, from ABC agreements, refreshed daily.</div>
        </div>
      </div>
      <div className="grid grid-3">
        <Kpi label="Active members past due" value={n(s.pastDueMembers)} note={s.active ? `${((s.pastDueMembers / s.active) * 100).toFixed(1)}% of active members` : undefined} />
        <Kpi label="Total past-due balance" value={usd(s.pastDueBalance)} />
        <Kpi label="Average per past-due member" value={usd(s.pastDueMembers ? s.pastDueBalance / s.pastDueMembers : 0)} />
      </div>
      <div className="card section">
        <h3>Past-due balance over time</h3>
        {trend.length > 1 ? <TrendLine rows={trend} x="date" y="balance" label="Past-due balance" /> :
          <p className="small muted" style={{ margin: 0 }}>Saved daily starting today, so this fills in over the coming weeks.</p>}
      </div>
      <div className="card section table-wrap">
        <h3>By membership type</h3>
        <table>
          <thead><tr><th>Membership</th><th style={{ textAlign: "right" }}>Active</th><th style={{ textAlign: "right" }}>Past due</th><th style={{ textAlign: "right" }}>Balance</th></tr></thead>
          <tbody>
            {types.map((t) => (
              <tr key={t.type}>
                <td>{t.type}</td><td style={{ textAlign: "right" }}>{n(t.members)}</td>
                <td style={{ textAlign: "right" }}>{n(t.pastDue)}</td><td style={{ textAlign: "right" }}>{usd(t.balance)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="small muted section">
        Freezes and coupons aren&apos;t in what our ABC keys can reach yet, so Collections Data and Coupons Vs. Sales stay
        in Natalia&apos;s sheets for now.
      </p>
    </>
  );
}
