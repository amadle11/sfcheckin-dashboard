import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Sig Fit Reporting",
  description: "Signature Fitness reporting and projects",
  icons: { icon: "/sfLogo.png" },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
