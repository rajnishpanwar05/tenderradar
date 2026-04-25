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

export function KpiCard({ title, value, subtitle, icon: Icon, className }: KpiCardProps) {
  return (
    <div className={cn(
      "bg-white border border-slate-200 rounded-lg shadow-sm p-5",
      className
    )}>
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-medium text-slate-500 uppercase tracking-wide">{title}</p>
        <div className="w-8 h-8 rounded-md bg-slate-100 flex items-center justify-center flex-shrink-0">
          <Icon className="w-4 h-4 text-slate-500" />
        </div>
      </div>
      <p className="text-2xl font-semibold tracking-tight tabular-nums text-slate-900">
        {formatNumber(value)}
      </p>
      {subtitle && (
        <p className="mt-1 text-xs text-slate-500">{subtitle}</p>
      )}
    </div>
  );
}
