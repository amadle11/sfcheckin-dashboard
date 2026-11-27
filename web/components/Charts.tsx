"use client";
import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

const YEAR_COLORS = ["#94a3b8", "#60a5fa", "#2563eb", "#111111"];
const fmt = (v: number) => v.toLocaleString("en-US");

/** rows: [{ month: "Jan", "2025": 410, "2026": 455 }, ...] */
export function YearLines({ rows, years }: { rows: Record<string, string | number>[]; years: string[] }) {
  const colors = YEAR_COLORS.slice(-years.length);
  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={rows} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid stroke="#eef0f3" vertical={false} />
        <XAxis dataKey="month" tickLine={false} axisLine={false} fontSize={12} />
        <YAxis tickLine={false} axisLine={false} fontSize={12} tickFormatter={fmt} width={48} />
        <Tooltip formatter={(v) => fmt(Number(v))} />
        <Legend />
        {years.map((y, i) => (
          <Line key={y} dataKey={y} stroke={colors[i]} strokeWidth={i === years.length - 1 ? 3 : 2} dot={{ r: 3 }} connectNulls={false} />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

export function Bars({ rows, x, y, height = 240 }: { rows: Record<string, string | number>[]; x: string; y: string; height?: number }) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={rows} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
        <CartesianGrid stroke="#eef0f3" vertical={false} />
        <XAxis dataKey={x} tickLine={false} axisLine={false} fontSize={12} />
        <YAxis tickLine={false} axisLine={false} fontSize={12} tickFormatter={fmt} width={48} />
        <Tooltip formatter={(v) => fmt(Number(v))} cursor={{ fill: "#f3f4f6" }} />
        <Bar dataKey={y} fill="#2563eb" radius={[4, 4, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}
