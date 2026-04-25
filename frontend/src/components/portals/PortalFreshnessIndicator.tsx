import { cn } from "@/lib/utils";
import { freshnessHours } from "@/lib/format";

interface PortalFreshnessIndicatorProps {
  lastScrapedAt: string | null;
  showLabel?: boolean;
}

type Freshness = "fresh" | "recent" | "stale" | "unknown";

function getFreshness(hours: number | null): Freshness {
  if (hours === null) return "unknown";
  if (hours < 6) return "fresh";
  if (hours <= 24) return "recent";
  return "stale";
}

const FRESHNESS_CONFIG: Record<
  Freshness,
  { dotCls: string; label: string }
> = {
  fresh:   { dotCls: "bg-emerald-500", label: "Fresh" },
  recent:  { dotCls: "bg-slate-500",   label: "Recent" },
  stale:   { dotCls: "bg-rose-500",    label: "Stale" },
  unknown: { dotCls: "bg-slate-400",   label: "Unknown" },
};

export function PortalFreshnessIndicator({
  lastScrapedAt,
  showLabel = false,
}: PortalFreshnessIndicatorProps) {
  const hours = freshnessHours(lastScrapedAt);
  const freshness = getFreshness(hours);
  const config = FRESHNESS_CONFIG[freshness];

  return (
    <span className="inline-flex items-center gap-1.5">
      <span
        className={cn(
          "h-2 w-2 rounded-full",
          config.dotCls,
          freshness === "fresh" && "animate-pulse"
        )}
        aria-hidden="true"
      />
      {showLabel && (
        <span className="text-xs text-slate-500">{config.label}</span>
      )}
    </span>
  );
}
