"use client";
import { X } from "lucide-react";
import { cn } from "@/lib/utils";
import { sectorLabel, portalLabel } from "@/lib/constants";
import type { TenderFilters } from "@/lib/api-types";

interface ActiveFilterTagsProps {
  filters: TenderFilters;
  onRemove: (key: keyof TenderFilters, value?: string) => void;
  onReset: () => void;
}

interface ChipProps {
  label: string;
  onRemove: () => void;
  className?: string;
}

function Chip({ label, onRemove, className }: ChipProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border bg-secondary px-2.5 py-1 text-xs font-medium",
        className
      )}
    >
      {label}
      <button
        type="button"
        onClick={onRemove}
        className="ml-0.5 rounded-full p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
        aria-label={`Remove filter: ${label}`}
      >
        <X className="h-3 w-3" />
      </button>
    </span>
  );
}

export function ActiveFilterTags({ filters, onRemove, onReset }: ActiveFilterTagsProps) {
  const chips: { key: keyof TenderFilters; value?: string; label: string }[] = [];

  // Sectors
  for (const sector of filters.sectors) {
    chips.push({ key: "sectors", value: sector, label: sectorLabel(sector) });
  }

  // Source portals
  for (const portal of filters.source_portals) {
    chips.push({ key: "source_portals", value: portal, label: portalLabel(portal) });
  }

  // Countries
  for (const country of filters.countries) {
    chips.push({ key: "countries", value: country, label: country });
  }

  // Service types
  for (const st of filters.service_types) {
    chips.push({ key: "service_types", value: st, label: st.replace(/_/g, " ") });
  }

  // Min fit score
  if (filters.min_fit_score > 0) {
    chips.push({
      key: "min_fit_score",
      label: `Fit \u2265 ${filters.min_fit_score}%`,
    });
  }

  // Exclude expired (when false = "include expired" is notable)
  if (!filters.exclude_expired) {
    chips.push({ key: "exclude_expired", label: "Include expired" });
  }

  // Exclude duplicates (when false = "include duplicates" is notable)
  if (!filters.exclude_duplicates) {
    chips.push({ key: "exclude_duplicates", label: "Include duplicates" });
  }

  if (chips.length === 0) {
    return null;
  }

  return (
    <div className="flex flex-wrap items-center gap-2">
      {chips.map((chip, i) => (
        <Chip
          key={`${chip.key}-${chip.value ?? i}`}
          label={chip.label}
          onRemove={() => onRemove(chip.key, chip.value)}
        />
      ))}
      <button
        type="button"
        onClick={onReset}
        className="text-xs text-muted-foreground underline-offset-2 hover:text-foreground hover:underline"
      >
        Clear all
      </button>
    </div>
  );
}
