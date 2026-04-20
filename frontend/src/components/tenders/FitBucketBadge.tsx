import { cn } from "@/lib/utils";
import { FIT_BUCKET_CONFIG } from "@/lib/constants";
import type { FitBucket } from "@/lib/api-types";

interface FitBucketBadgeProps {
  bucket: FitBucket;
  score?: number;
  showScore?: boolean;
  className?: string;
}

export function FitBucketBadge({
  bucket,
  score,
  showScore = false,
  className,
}: FitBucketBadgeProps) {
  const config = FIT_BUCKET_CONFIG[bucket];

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-xs font-semibold",
        config.badgeCls,
        className
      )}
    >
      <span
        className={cn("h-1.5 w-1.5 rounded-full", config.dotColor)}
        aria-hidden="true"
      />
      {showScore && score !== undefined
        ? `${config.label} · ${Math.round(score)}`
        : config.label}
    </span>
  );
}
