import "server-only";
import type { Item } from "./types";

/**
 * Emails everyone watching a project, except the person who made the change.
 * Needs SMTP_URL (and ideally MAIL_FROM, APP_URL). Without SMTP_URL it only logs.
 */
export async function notifyWatchers(item: Item, actorEmail: string, subject: string, body: string) {
  const to = item.watchers.filter((w) => w !== actorEmail);
  if (to.length === 0) return;
  const smtp = process.env.SMTP_URL;
  const link = process.env.APP_URL ? `${process.env.APP_URL.replace(/\/$/, "")}/projects/${item.id}` : "";
  const text = `${body}\n\n${link ? `Open it: ${link}\n\n` : ""}You get this because you watch "${item.title}" on Sig Fit Reporting.`;
  if (!smtp) {
    console.log(`[notify] SMTP_URL not set; would email ${to.length} watcher(s): ${subject}`);
    return;
  }
  try {
    const nodemailer = await import("nodemailer");
    const transport = nodemailer.createTransport(smtp);
    await transport.sendMail({
      from: process.env.MAIL_FROM || "Sig Fit Reporting <no-reply@localhost>",
      bcc: to,
      subject: `[Sig Fit] ${subject}`,
      text,
    });
  } catch (err) {
    // A failed email never blocks the update itself.
    console.error("[notify] email failed", err);
  }
}
