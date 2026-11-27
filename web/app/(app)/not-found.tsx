import Link from "next/link";
export default function NotFound() {
  return (<div className="card coming"><h2>Not found</h2><p className="muted">It may have been deleted or made private.</p><Link className="btn" href="/projects">Projects &amp; Tasks</Link></div>);
}
