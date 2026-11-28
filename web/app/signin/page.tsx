import { redirect } from "next/navigation";
import { signIn } from "@/auth";
import { getCurrentUser } from "@/lib/access";

export default async function SignInPage({ searchParams }: { searchParams: Promise<{ error?: string }> }) {
  if (await getCurrentUser()) redirect("/");
  const { error } = await searchParams;
  return (
    <div className="center-page">
      <div className="signin-card">
        <img src="/sfLogo.png" alt="" />
        <h1>Sig Fit Reporting</h1>
        <p className="muted">Sign in with the Google account your access was set up for.</p>
        {error && <p className="notice">Sign-in didn&apos;t work. Try again, or ask an admin to check your access.</p>}
        <form
          action={async () => {
            "use server";
            await signIn("google", { redirectTo: "/" });
          }}
        >
          <button className="btn primary" type="submit" style={{ width: "100%", justifyContent: "center", padding: "10px" }}>
            Sign in with Google
          </button>
        </form>
      </div>
    </div>
  );
}
