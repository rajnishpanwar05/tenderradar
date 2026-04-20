import { cn } from "@/lib/utils";
import { sectorLabel, SECTOR_CHART_COLORS } from "@/lib/constants";

interface SectorBadgeProps {
  sector: string;
  className?: string;
}

export function SectorBadge({ sector, className }: SectorBadgeProps) {
  const label = sectorLabel(sector);
  const color = SECTOR_CHART_COLORS[sector] ?? "#6b7280";

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border bg-background px-2.5 py-0.5 text-xs font-medium max-w-[130px]",
        className
      )}
    >
      <span
        className="h-2 w-2 flex-shrink-0 rounded-full"
        style={{ backgroundColor: color }}
        aria-hidden="true"
      />
      <span className="truncate">{label}</span>
    </span>
  );
}
