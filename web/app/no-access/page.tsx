import Link from "next/link";

export default async function NoAccess({ searchParams }: { searchParams: Promise<{ email?: string }> }) {
  const { email } = await searchParams;
  return (
    <div className="center-page">
      <div className="signin-card">
        <img src="/sfLogo.png" alt="" />
        <h1>No access yet</h1>
        <p className="muted">
          {email ? <><b>{email}</b> isn&apos;t</> : "This account isn't"} set up for Sig Fit Reporting. Ask an admin to add
          you on the Admin page, then sign in again.
        </p>
        <Link className="btn" href="/signin">Back to sign-in</Link>
      </div>
    </div>
  );
}
