import NextAuth from "next-auth";
import Google from "next-auth/providers/google";
import { isAllowedEmail } from "@/lib/access";

export const { handlers, auth, signIn, signOut } = NextAuth({
  providers: [Google],
  pages: { signIn: "/signin", error: "/signin" },
  session: { strategy: "jwt" },
  callbacks: {
    // Only people added on the Admin page (or listed in ADMIN_EMAILS) can sign in.
    async signIn({ profile }) {
      const email = profile?.email?.toLowerCase();
      if (!email || profile?.email_verified === false) return "/no-access";
      return (await isAllowedEmail(email)) ? true : `/no-access?email=${encodeURIComponent(email)}`;
    },
  },
});
