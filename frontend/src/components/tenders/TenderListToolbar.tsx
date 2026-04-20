"use client";
import { Loader2, Table2, LayoutGrid } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface TenderListToolbarProps {
  total: number;
  isLoading: boolean;
  view: "table" | "cards";
  onViewChange: (v: "table" | "cards") => void;
  sortBy: string;
  sortOrder: string;
  onSort: (field: string, order: string) => void;
}

const SORT_OPTIONS: { label: string; field: string; order: string }[] = [
  { label: "Fit Score",   field: "fit_score",  order: "desc" },
  { label: "Date Added",  field: "scraped_at", order: "desc" },
  { label: "Deadline",    field: "deadline",   order: "asc"  },
];

export function TenderListToolbar({
  total,
  isLoading,
  view,
  onViewChange,
  sortBy,
  sortOrder,
  onSort,
}: TenderListToolbarProps) {
  const activeSort = SORT_OPTIONS.find(
    (o) => o.field === sortBy && o.order === sortOrder
  );

  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      {/* Left — result count + loading indicator */}
      <div className="flex items-center gap-2">
        {isLoading ? (
          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" aria-label="Loading" />
        ) : null}
        <span className="text-sm text-muted-foreground">
          <span className="font-semibold text-foreground">{total.toLocaleString()}</span>{" "}
          {total === 1 ? "tender" : "tenders"} found
        </span>
      </div>

      {/* Right — view toggle + sort */}
      <div className="flex items-center gap-2">
        {/* Sort dropdown */}
        <div className="flex items-center gap-1 rounded-md border bg-background px-2 py-1 text-sm">
          <span className="text-xs text-muted-foreground">Sort:</span>
          <select
            className="cursor-pointer bg-transparent text-sm font-medium focus:outline-none"
            value={`${sortBy}::${sortOrder}`}
            onChange={(e) => {
              const [field, order] = e.target.value.split("::");
              onSort(field, order);
            }}
            aria-label="Sort tenders by"
          >
            {SORT_OPTIONS.map((o) => (
              <option key={`${o.field}::${o.order}`} value={`${o.field}::${o.order}`}>
                {o.label}
              </option>
            ))}
            {/* Fallback option for non-standard combos */}
            {!activeSort && (
              <option value={`${sortBy}::${sortOrder}`}>Custom</option>
            )}
          </select>
        </div>

        {/* View toggle */}
        <div className="flex items-center rounded-md border">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onViewChange("table")}
            className={cn(
              "rounded-r-none border-r px-2.5",
              view === "table" && "bg-muted"
            )}
            aria-label="Table view"
            aria-pressed={view === "table"}
          >
            <Table2 className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onViewChange("cards")}
            className={cn(
              "rounded-l-none px-2.5",
              view === "cards" && "bg-muted"
            )}
            aria-label="Card view"
            aria-pressed={view === "cards"}
          >
            <LayoutGrid className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  );
}
