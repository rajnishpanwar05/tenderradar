// =============================================================================
// lib/format.ts — Display formatting utilities
// =============================================================================

import { differenceInDays, differenceInHours, format, formatDistanceToNow, parseISO } from "date-fns";

// ---------------------------------------------------------------------------
// Dates
// ---------------------------------------------------------------------------

/**
 * Urgency buckets — mutually exclusive, matching backend deadline_category values.
 *   closing_soon  0–7 days remaining   (act immediately)
 *   needs_action  8–21 days remaining  (prepare now)
 *   plan_ahead    22+ days remaining   (schedule work)
 *   expired       deadline passed
 *   none          no deadline recorded
 */
export type DeadlineUrgency =
  | "expired"
  | "closing_soon"
  | "needs_action"
  | "plan_ahead"
  | "none";

export interface DeadlineInfo {
  label:   string;
  urgency: DeadlineUrgency;
  daysLeft: number | null;
}

export function getDeadlineInfo(
  deadline: string | null,
  isExpired: boolean
): DeadlineInfo {
  if (!deadline) {
    return { label: "No deadline", urgency: "none", daysLeft: null };
  }

  const d = parseISO(deadline);
  const now = new Date();

  if (isExpired || d < now) {
    return { label: "Expired", urgency: "expired", daysLeft: null };
  }

  const days = differenceInDays(d, now);

  if (days === 0) {
    const hours = differenceInHours(d, now);
    return { label: `${hours}h left`, urgency: "closing_soon", daysLeft: 0 };
  }
  if (days <= 7)  return { label: `${days}d left`, urgency: "closing_soon", daysLeft: days };
  if (days <= 21) return { label: `${days}d left`, urgency: "needs_action",  daysLeft: days };

  return { label: format(d, "d MMM yyyy"), urgency: "plan_ahead", daysLeft: days };
}

export function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return format(parseISO(iso), "d MMM yyyy");
  } catch {
    return iso;
  }
}

export function timeAgo(iso: string | null): string {
  if (!iso) return "—";
  try {
    return formatDistanceToNow(parseISO(iso), { addSuffix: true });
  } catch {
    return iso;
  }
}

export function freshnessHours(iso: string | null): number | null {
  if (!iso) return null;
  try {
    return differenceInHours(new Date(), parseISO(iso));
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Numbers & budget
// ---------------------------------------------------------------------------

export function formatScore(score: number): string {
  return Math.round(score).toString();
}

export function formatBudget(usd: number | null): string {
  if (!usd) return "—";
  if (usd >= 1_000_000) return `$${(usd / 1_000_000).toFixed(1)}M`;
  if (usd >= 1_000)     return `$${Math.round(usd / 1_000)}K`;
  return `$${usd.toLocaleString()}`;
}

export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

export function truncate(str: string, maxLen: number): string {
  if (str.length <= maxLen) return str;
  return str.slice(0, maxLen - 1) + "…";
}

export function capitalize(str: string): string {
  if (!str) return str;
  return str.charAt(0).toUpperCase() + str.slice(1);
}
