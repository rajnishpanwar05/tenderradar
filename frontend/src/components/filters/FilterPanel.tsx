"use client";
import { useState } from "react";
import { ChevronDown, ChevronUp, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { SECTOR_LABELS, PORTAL_LABELS } from "@/lib/constants";
import { hasActiveFilters } from "@/lib/url-state";
import { useDebounce } from "@/hooks/useDebounce";
import type { TenderFilters } from "@/lib/api-types";

interface FilterPanelProps {
  filters: TenderFilters;
  onSetFilter: <K extends keyof TenderFilters>(key: K, value: TenderFilters[K]) => void;
  onToggleArray: (key: "sectors" | "service_types" | "countries" | "source_portals", value: string) => void;
  onReset: () => void;
}

interface CollapsibleSectionProps {
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
}

function CollapsibleSection({ title, children, defaultOpen = true }: CollapsibleSectionProps) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div className="border-b pb-4">
      <button
        type="button"
        className="flex w-full items-center justify-between py-2 text-sm font-semibold"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        {title}
        {open ? (
          <ChevronUp className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        )}
      </button>
      {open && <div className="mt-2">{children}</div>}
    </div>
  );
}

interface CheckboxListProps {
  items: Record<string, string>;
  selected: string[];
  onToggle: (value: string) => void;
}

function CheckboxList({ items, selected, onToggle }: CheckboxListProps) {
  return (
    <div className="space-y-1.5">
      {Object.entries(items).map(([slug, label]) => (
        <label
          key={slug}
          className="flex cursor-pointer items-center gap-2 text-sm hover:text-foreground"
        >
          <input
            type="checkbox"
            className="h-3.5 w-3.5 rounded border-border accent-primary"
            checked={selected.includes(slug)}
            onChange={() => onToggle(slug)}
          />
          <span className={cn("leading-tight", selected.includes(slug) ? "font-medium" : "text-muted-foreground")}>
            {label}
          </span>
        </label>
      ))}
    </div>
  );
}

export function FilterPanel({ filters, onSetFilter, onToggleArray, onReset }: FilterPanelProps) {
  const [localSearch, setLocalSearch] = useState(filters.q);
  const debouncedSearch = useDebounce(localSearch, 300);

  // Sync debounced search to URL
  useState(() => {
    if (debouncedSearch !== filters.q) {
      onSetFilter("q", debouncedSearch);
    }
  });

  // When filters.q changes externally (e.g. reset), sync back
  const [prevQ, setPrevQ] = useState(filters.q);
  if (filters.q !== prevQ) {
    setPrevQ(filters.q);
    if (filters.q !== localSearch) {
      setLocalSearch(filters.q);
    }
  }

  function handleSearchChange(value: string) {
    setLocalSearch(value);
    // Immediately push if debounce not desired — we use the debounce hook above
    // but since useState side-effect won't work, we call onSetFilter after debounce
    // The effect is triggered below via debouncedSearch changes.
  }

  // Trigger filter update when debounced value changes
  const [prevDebounced, setPrevDebounced] = useState(debouncedSearch);
  if (debouncedSearch !== prevDebounced) {
    setPrevDebounced(debouncedSearch);
    onSetFilter("q", debouncedSearch);
  }

  const isActive = hasActiveFilters(filters);

  return (
    <aside className="hidden w-64 flex-shrink-0 space-y-4 lg:block">
      {/* Heading */}
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
          Filters
        </h2>
        {isActive && (
          <Button
            variant="ghost"
            size="sm"
            onClick={onReset}
            className="h-6 px-2 text-xs text-muted-foreground hover:text-foreground"
          >
            <X className="mr-1 h-3 w-3" />
            Reset
          </Button>
        )}
      </div>

      {/* Search */}
      <div className="border-b pb-4">
        <label className="mb-1.5 block text-xs font-semibold">Search</label>
        <div className="relative">
          <input
            type="text"
            placeholder="Keywords, org, country…"
            value={localSearch}
            onChange={(e) => handleSearchChange(e.target.value)}
            className="w-full rounded-md border bg-background px-3 py-1.5 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
          />
          {localSearch && (
            <button
              type="button"
              onClick={() => {
                setLocalSearch("");
                onSetFilter("q", "");
              }}
              className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
              aria-label="Clear search"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </div>

      {/* Sectors */}
      <CollapsibleSection title="Sectors">
        <CheckboxList
          items={SECTOR_LABELS}
          selected={filters.sectors}
          onToggle={(slug) => onToggleArray("sectors", slug)}
        />
      </CollapsibleSection>

      {/* Portals */}
      <CollapsibleSection title="Portals">
        <CheckboxList
          items={PORTAL_LABELS}
          selected={filters.source_portals}
          onToggle={(slug) => onToggleArray("source_portals", slug)}
        />
      </CollapsibleSection>

      {/* Fit Score */}
      <CollapsibleSection title="Fit Score">
        <div className="space-y-2">
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>Min: {filters.min_fit_score}%</span>
            {filters.min_fit_score > 0 && (
              <button
                type="button"
                onClick={() => onSetFilter("min_fit_score", 0)}
                className="hover:text-foreground"
              >
                Clear
              </button>
            )}
          </div>
          <input
            type="range"
            min={0}
            max={100}
            step={5}
            value={filters.min_fit_score}
            onChange={(e) => onSetFilter("min_fit_score", Number(e.target.value))}
            className="w-full accent-primary"
          />
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>0</span>
            <span>100</span>
          </div>
        </div>
      </CollapsibleSection>

      {/* Options */}
      <CollapsibleSection title="Options">
        <div className="space-y-3">
          <ToggleSwitch
            label="Exclude expired"
            checked={filters.exclude_expired}
            onChange={(v) => onSetFilter("exclude_expired", v)}
          />
          <ToggleSwitch
            label="Exclude duplicates"
            checked={filters.exclude_duplicates}
            onChange={(v) => onSetFilter("exclude_duplicates", v)}
          />
        </div>
      </CollapsibleSection>
    </aside>
  );
}

interface ToggleSwitchProps {
  label: string;
  checked: boolean;
  onChange: (v: boolean) => void;
}

function ToggleSwitch({ label, checked, onChange }: ToggleSwitchProps) {
  return (
    <label className="flex cursor-pointer items-center justify-between gap-2 text-sm">
      <span className={cn(checked ? "font-medium" : "text-muted-foreground")}>
        {label}
      </span>
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        onClick={() => onChange(!checked)}
        className={cn(
          "relative inline-flex h-5 w-9 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-1",
          checked ? "bg-primary" : "bg-muted"
        )}
      >
        <span
          className={cn(
            "inline-block h-3.5 w-3.5 rounded-full bg-white shadow transition-transform",
            checked ? "translate-x-4" : "translate-x-1"
          )}
        />
      </button>
    </label>
  );
}
