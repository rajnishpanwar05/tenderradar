"use client";
import { priorityConfig } from "@/lib/constants";
import { cn } from "@/lib/utils";

interface Props {
  score: number;
  size?: "sm" | "md";
  showBar?: boolean;
}

export function PriorityBadge({ score, size = "md", showBar = false }: Props) {
  const cfg = priorityConfig(score);
  return (
    <div className={cn("inline-flex items-center gap-1.5", size === "sm" && "text-xs")}>
      <span
        className={cn(
          "inline-flex items-center rounded-full border px-2 py-0.5 font-semibold tabular-nums",
          size === "sm" ? "text-xs" : "text-xs",
          cfg.cls,
        )}
      >
        {score}
      </span>
      {showBar && (
        <div className="h-1.5 w-16 rounded-full bg-muted overflow-hidden">
          <div
            className={cn("h-full rounded-full transition-all", cfg.bar)}
            style={{ width: `${Math.min(score, 100)}%` }}
          />
        </div>
      )}
    </div>
  );
}
