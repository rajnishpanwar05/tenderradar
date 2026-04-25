import { cn } from "@/lib/utils";
import { formatNumber } from "@/lib/format";

interface KpiCardProps {
  title:     string;
  value:     number;
  subtitle?: string;
  icon:      React.ComponentType<{ className?: string }>;
  trend?:    "up" | "down" | "neutral";
  accent?:   "blue" | "cyan" | "violet" | "amber" | "emerald" | "rose";
  className?: string;
}

const ACCENT_CONFIG = {
  blue:    { dot: "bg-slate-900" },
  cyan:    { dot: "bg-slate-700" },
  violet:  { dot: "bg-slate-800" },
  amber:   { dot: "bg-slate-700" },
  emerald: { dot: "bg-slate-900" },
  rose:    { dot: "bg-slate-800" },
};

export function KpiCard({ title, value, subtitle, icon: Icon, accent = "blue", className }: KpiCardProps) {
  const cfg = ACCENT_CONFIG[accent];

  return (
    <div className={cn(
      "relative shell-panel glass-hover rounded-2xl p-5 overflow-hidden",
      className
    )}>
      {/* Ambient glow */}
      <div className={cn(
        "absolute -top-6 -right-6 w-24 h-24 rounded-full blur-2xl opacity-10",
        cfg.dot
      )} />

      {/* Top row */}
      <div className="relative flex items-start justify-between mb-4">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider">{title}</p>
        <div className={cn("w-8 h-8 rounded-lg flex items-center justify-center shadow-sm flex-shrink-0 border border-slate-200 bg-slate-50", cfg.dot)}>
          <Icon className="w-4 h-4 text-white" />
        </div>
      </div>

      {/* Value */}
      <p className="relative text-3xl font-semibold tracking-tight tabular-nums font-mono text-slate-900">
        {formatNumber(value)}
      </p>

      {/* Subtitle */}
      {subtitle && (
        <p className="relative mt-1.5 text-[11px] text-slate-500">{subtitle}</p>
      )}
    </div>
  );
}
