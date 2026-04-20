"use client";

import { sectorLabel } from "@/lib/constants";
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, ReferenceLine 
} from "recharts";

interface SectorBreakdownChartProps {
  data: Record<string, number>;
}

export function SectorBreakdownChart({ data }: SectorBreakdownChartProps) {
  const chartData = Object.entries(data)
    .map(([slug, count]) => ({
      name: sectorLabel(slug),
      value: count,
    }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 7);

  // Vibrant, rich UI colors
  const COLORS = ['#6366f1', '#10B981', '#06b6d4', '#f43f5e', '#8b5cf6', '#f59e0b', '#3b82f6'];

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-slate-900 text-white p-4 rounded-xl shadow-2xl border border-slate-700">
          <p className="font-bold text-sm mb-1">{label}</p>
          <p className="text-emerald-400 font-mono text-2xl font-black">
            {payload[0].value.toLocaleString()} <span className="text-xs text-slate-400 uppercase tracking-widest font-bold">Tenders</span>
          </p>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="w-full h-[350px]">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          data={chartData}
          layout="vertical"
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
          barSize={40}
        >
          <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" />
          <XAxis type="number" hide />
          <YAxis 
            type="category" 
            dataKey="name" 
            axisLine={false} 
            tickLine={false} 
            tick={{ fill: '#475569', fontSize: 13, fontWeight: 700 }}
            width={160}
          />
          <Tooltip cursor={{ fill: 'rgba(241, 245, 249, 0.5)' }} content={<CustomTooltip />} />
          <Bar dataKey="value" radius={[0, 8, 8, 0]}>
            {chartData.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
