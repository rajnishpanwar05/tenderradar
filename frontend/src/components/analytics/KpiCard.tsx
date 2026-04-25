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

const ACCENT_MAP = {
  blue:    { border: "border-l-blue-500",    bg: "bg-blue-50",    icon: "text-blue-600"    },
  cyan:    { border: "border-l-cyan-500",    bg: "bg-cyan-50",    icon: "text-cyan-600"    },
  violet:  { border: "border-l-violet-500",  bg: "bg-violet-50",  icon: "text-violet-600"  },
  amber:   { border: "border-l-amber-500",   bg: "bg-amber-50",   icon: "text-amber-600"   },
  emerald: { border: "border-l-emerald-500", bg: "bg-emerald-50", icon: "text-emerald-600" },
  rose:    { border: "border-l-rose-500",    bg: "bg-rose-50",    icon: "text-rose-600"    },
};

export function KpiCard({ title, value, subtitle, icon: Icon, accent, className }: KpiCardProps) {
  const s = accent ? ACCENT_MAP[accent] : { border: "border-l-slate-400", bg: "bg-slate-100", icon: "text-slate-500" };
  return (
    <div className={cn(
      "stat-card border-l-4",
      s.border,
      className
    )}>
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-medium text-slate-500 uppercase tracking-wide">{title}</p>
        <div className={cn("w-8 h-8 rounded-md flex items-center justify-center flex-shrink-0", s.bg)}>
          <Icon className={cn("w-4 h-4", s.icon)} />
        </div>
      </div>
      <p className="text-3xl font-bold tracking-tight tabular-nums text-slate-900 leading-none">
        {formatNumber(value)}
      </p>
      {subtitle && (
        <p className="mt-1.5 text-xs text-slate-500">{subtitle}</p>
      )}
    </div>
  );
}
