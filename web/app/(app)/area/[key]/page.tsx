import { notFound } from "next/navigation";
import Link from "next/link";
import { requireMenu } from "@/lib/access";
import { MENUS, type MenuKey } from "@/lib/menus";

// Placeholder for the areas built in later phases.
export default async function AreaPage({ params }: { params: Promise<{ key: string }> }) {
  const { key } = await params;
  const menu = MENUS.find((m) => m.href === `/area/${key}`);
  if (!menu) notFound();
  await requireMenu(menu.key as MenuKey);
  return (
    <>
      <h1>{menu.label}</h1>
      <div className="card section coming">
        <span className="pill phase">Coming in Phase {menu.phase}</span>
        <p style={{ maxWidth: 560, margin: "8px auto" }}>{menu.blurb}</p>
        <p className="small muted">Until then the existing sheets stay the source. See <Link href="/sources">Sources</Link>.</p>
      </div>
    </>
  );
}
