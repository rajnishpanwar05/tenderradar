import { cn } from "@/lib/utils";
import { portalLabel } from "@/lib/constants";

interface PortalIconProps {
  portal: string;
  showLabel?: boolean;
  className?: string;
}

/** Deterministic color derived from portal slug string */
function hashColor(str: string): string {
  const PALETTE = [
    "#1d4ed8", // blue-700
    "#0f766e", // teal-700
    "#7c3aed", // violet-600
    "#b45309", // amber-700
    "#be185d", // pink-700
    "#0369a1", // sky-700
    "#15803d", // green-700
    "#c2410c", // orange-700
    "#1e40af", // blue-800
    "#6d28d9", // violet-700
    "#0e7490", // cyan-700
    "#92400e", // amber-800
  ];
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (h * 31 + str.charCodeAt(i)) >>> 0;
  }
  return PALETTE[h % PALETTE.length];
}

export function PortalIcon({ portal, showLabel = false, className }: PortalIconProps) {
  const label = portalLabel(portal);
  const initials = label.slice(0, 2).toUpperCase();
  const bg = hashColor(portal);

  return (
    <span className={cn("inline-flex items-center gap-1.5", className)}>
      <span
        className="inline-flex h-6 w-6 flex-shrink-0 items-center justify-center rounded text-[10px] font-bold text-white"
        style={{ backgroundColor: bg }}
        title={label}
        aria-label={label}
      >
        {initials}
      </span>
      {showLabel && (
        <span className="text-xs font-medium text-muted-foreground">{label}</span>
      )}
    </span>
  );
}
