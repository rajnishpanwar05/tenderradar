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
    cls:   "bg-orange-50 text-orange-700 border-orange-200 dark:bg-orange-900/30 dark:text-orange-400",
  },
  needs_action: {
    label: "Needs Action",
    cls:   "bg-amber-50 text-amber-700 border-amber-200 dark:bg-amber-900/30 dark:text-amber-400",
  },
  plan_ahead: {
    label: "Plan Ahead",
    cls:   "bg-slate-50 text-slate-600 border-slate-200 dark:bg-slate-800 dark:text-slate-400",
  },
  unknown: {
    label: "—",
    cls:   "bg-muted text-muted-foreground border-transparent",
  },
  // Legacy values (stored before the rename) — map to nearest new bucket
  urgent: {
    label: "Closing Soon",
    cls:   "bg-orange-50 text-orange-700 border-orange-200 dark:bg-orange-900/30 dark:text-orange-400",
  },
  soon: {
    label: "Needs Action",
    cls:   "bg-amber-50 text-amber-700 border-amber-200 dark:bg-amber-900/30 dark:text-amber-400",
  },
  normal: {
    label: "Plan Ahead",
    cls:   "bg-slate-50 text-slate-600 border-slate-200 dark:bg-slate-800 dark:text-slate-400",
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
          "inline-flex items-center rounded-full border bg-muted px-2.5 py-0.5 text-xs text-muted-foreground",
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
          "inline-flex items-center rounded-full border border-red-200 bg-red-50 px-2.5 py-0.5 text-xs font-medium text-red-700 dark:border-red-800 dark:bg-red-900/30 dark:text-red-400",
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
          "inline-flex items-center gap-1.5 rounded-full border border-orange-200 bg-orange-50 px-2.5 py-0.5 text-xs font-medium text-orange-700 dark:border-orange-800 dark:bg-orange-900/30 dark:text-orange-400",
          className
        )}
      >
        <span
          className="h-1.5 w-1.5 animate-pulse rounded-full bg-orange-500"
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
          "inline-flex items-center rounded-full border border-amber-200 bg-amber-50 px-2.5 py-0.5 text-xs font-medium text-amber-700 dark:border-amber-800 dark:bg-amber-900/30 dark:text-amber-400",
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
        "inline-flex items-center rounded-full border bg-background px-2.5 py-0.5 text-xs text-muted-foreground",
        className
      )}
    >
      {info.label}
    </span>
  );
}
