import { cn } from "@/lib/utils";
import { getDeadlineInfo } from "@/lib/format";

/**
 * Config for the stored deadline_category field from the intel API.
 * Keys must match the backend bucket values returned by _classify_deadline().
 *
 * Buckets are mutually exclusive:
 *   closing_soon  0–7 days   · needs_action  8–21 days   · plan_ahead  22+
 */
export const DEADLINE_CATEGORY_CONFIG = {
  closing_soon: {
    label: "Closing Soon",
    cls:   "bg-slate-100 text-slate-700 border-slate-200",
  },
  needs_action: {
    label: "Needs Action",
    cls:   "bg-slate-100 text-slate-700 border-slate-200",
  },
  plan_ahead: {
    label: "Plan Ahead",
    cls:   "bg-slate-50 text-slate-600 border-slate-200",
  },
  unknown: {
    label: "—",
    cls:   "bg-muted text-muted-foreground border-transparent",
  },
  // Legacy values (stored before the rename) — map to nearest new bucket
  urgent: {
    label: "Closing Soon",
    cls:   "bg-slate-100 text-slate-700 border-slate-200",
  },
  soon: {
    label: "Needs Action",
    cls:   "bg-slate-100 text-slate-700 border-slate-200",
  },
  normal: {
    label: "Plan Ahead",
    cls:   "bg-slate-50 text-slate-600 border-slate-200",
  },
} as const;

interface DeadlineChipProps {
  deadline: string | null;
  isExpired: boolean;
  className?: string;
}

export function DeadlineChip({ deadline, isExpired, className }: DeadlineChipProps) {
  const info = getDeadlineInfo(deadline, isExpired);

  if (info.urgency === "none") {
      return (
        <span
          className={cn(
          "inline-flex items-center rounded-full border bg-slate-50 px-2.5 py-0.5 text-xs text-slate-500",
          className
        )}
      >
        —
      </span>
    );
  }

  if (info.urgency === "expired") {
      return (
        <span
          className={cn(
          "inline-flex items-center rounded-full border border-slate-200 bg-slate-100 px-2.5 py-0.5 text-xs font-medium text-slate-700",
          className
        )}
      >
        Expired
      </span>
    );
  }

  if (info.urgency === "closing_soon") {
      return (
        <span
          className={cn(
          "inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-slate-100 px-2.5 py-0.5 text-xs font-medium text-slate-700",
          className
        )}
      >
        <span
          className="h-1.5 w-1.5 animate-pulse rounded-full bg-slate-700"
          aria-hidden="true"
        />
        {info.label}
      </span>
    );
  }

  if (info.urgency === "needs_action") {
      return (
        <span
          className={cn(
          "inline-flex items-center rounded-full border border-slate-200 bg-slate-100 px-2.5 py-0.5 text-xs font-medium text-slate-700",
          className
        )}
      >
        {info.label}
      </span>
    );
  }

  // plan_ahead
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border bg-white px-2.5 py-0.5 text-xs text-slate-500",
        className
      )}
    >
      {info.label}
    </span>
  );
}
