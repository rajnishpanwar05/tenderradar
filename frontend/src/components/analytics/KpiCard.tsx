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
  blue:    { top: "card-accent-blue",    icon: "gradient-blue",    text: "text-blue-400",    num: "text-blue-300" },
  cyan:    { top: "card-accent-cyan",    icon: "gradient-blue",    text: "text-cyan-400",    num: "text-cyan-300" },
  violet:  { top: "card-accent-violet",  icon: "gradient-violet",  text: "text-violet-400",  num: "text-violet-300" },
  amber:   { top: "card-accent-amber",   icon: "gradient-amber",   text: "text-amber-400",   num: "text-amber-300" },
  emerald: { top: "card-accent-emerald", icon: "gradient-emerald", text: "text-emerald-400", num: "text-emerald-300" },
  rose:    { top: "card-accent-rose",    icon: "gradient-rose",    text: "text-rose-400",    num: "text-rose-300" },
};

export function KpiCard({ title, value, subtitle, icon: Icon, accent = "blue", className }: KpiCardProps) {
  const cfg = ACCENT_CONFIG[accent];

  return (
    <div className={cn(
      "relative glass glass-hover rounded-xl p-5 overflow-hidden",
      cfg.top,
      className
    )}>
      {/* Ambient glow */}
      <div className={cn(
        "absolute -top-6 -right-6 w-24 h-24 rounded-full blur-2xl opacity-20",
        cfg.icon
      )} />

      {/* Top row */}
      <div className="relative flex items-start justify-between mb-4">
        <p className="text-xs font-medium text-white/40 uppercase tracking-wider">{title}</p>
        <div className={cn("w-8 h-8 rounded-lg flex items-center justify-center shadow-lg flex-shrink-0", cfg.icon)}>
          <Icon className="w-4 h-4 text-white" />
        </div>
      </div>

      {/* Value */}
      <p className={cn(
        "relative text-3xl font-bold tracking-tight tabular-nums font-mono",
        cfg.num
      )}>
        {formatNumber(value)}
      </p>

      {/* Subtitle */}
      {subtitle && (
        <p className="relative mt-1.5 text-[11px] text-white/30">{subtitle}</p>
      )}
    </div>
  );
}
