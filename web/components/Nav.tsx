"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

export interface NavItem {
  href: string;
  label: string;
  section: string;
  phase?: number;
}

export default function Nav({ items }: { items: NavItem[] }) {
  const path = usePathname();
  const sections = [...new Set(items.map((i) => i.section))];
  const isActive = (href: string) => (href === "/" ? path === "/" : path === href || path.startsWith(href + "/"));
  return (
    <nav className="nav">
      {sections.map((s) => (
        <div key={s} style={{ display: "contents" }}>
          <div className="nav-section">{s}</div>
          {items
            .filter((i) => i.section === s)
            .map((i) => (
              <Link key={i.href} href={i.href} className={isActive(i.href) ? "active" : ""}>
                <span>{i.label}</span>
                {i.phase && <span className="soon">Phase {i.phase}</span>}
              </Link>
            ))}
        </div>
      ))}
    </nav>
  );
}
