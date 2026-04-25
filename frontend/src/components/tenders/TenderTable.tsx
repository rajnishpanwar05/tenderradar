"use client";
import { ChevronUp, ChevronDown, ExternalLink, ArrowRight } from "lucide-react";
import {
  Table, TableHeader, TableBody,
  TableRow, TableHead, TableCell,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import type { TenderIntelItem } from "@/lib/api-types";
import { SectorBadge } from "./SectorBadge";
import { FitBucketBadge } from "./FitBucketBadge";
import { DEADLINE_CATEGORY_CONFIG } from "./DeadlineChip";
import { portalLabel } from "@/lib/constants";
import { handleTenderClick } from "@/lib/tender-links";

interface TenderTableProps {
  tenders:       TenderIntelItem[];
  sortBy:        string;
  sortOrder:     string;
  onSort:        (field: string) => void;
  onTenderClick: (id: string) => void;
}

function SortableHead({
  field, sortBy, sortOrder, onSort, children, className,
}: {
  field: string; sortBy: string; sortOrder: string;
  onSort: (f: string) => void; children: React.ReactNode; className?: string;
}) {
  const active = sortBy === field;
  return (
    <TableHead
      className={cn("cursor-pointer select-none whitespace-nowrap", className)}
      onClick={() => onSort(field)}
    >
      <span className="inline-flex items-center gap-1.5 hover:text-slate-900 transition-colors">
        {children}
        {active
          ? sortOrder === "asc"
            ? <ChevronUp   className="h-4 w-4 text-slate-700" />
            : <ChevronDown className="h-4 w-4 text-slate-700" />
          : <ChevronDown className="h-4 w-4 opacity-0 group-hover:opacity-100 text-gray-400" />
        }
      </span>
    </TableHead>
  );
}

// Deadline category → small colored chip for light theme
function DeadlineCategoryChip({ cat }: { cat: string }) {
  const config = DEADLINE_CATEGORY_CONFIG[cat as keyof typeof DEADLINE_CATEGORY_CONFIG]
    ?? DEADLINE_CATEGORY_CONFIG.unknown;
  
    // Bright styling mapping just in case config uses dark tailwind text
    return (
    <span className="inline-flex items-center rounded-full bg-slate-100 border border-slate-200 px-3 py-1 text-[11px] font-semibold tracking-wide text-slate-700 shadow-sm uppercase">
      {config.label}
    </span>
  );
}

export function TenderTable({
  tenders, sortBy, sortOrder, onSort, onTenderClick,
}: TenderTableProps) {
  return (
    <div className="w-full">
      <Table>
        <TableHeader>
          <TableRow className="border-b border-slate-200 bg-slate-50/60 hover:bg-slate-50/60 group">
            <TableHead className="w-[100px] text-xs font-semibold tracking-widest uppercase text-slate-400 py-6 pl-8">Portal</TableHead>
            <SortableHead
              field="title"
              sortBy={sortBy}
              sortOrder={sortOrder}
              onSort={onSort}
              className="text-xs font-semibold tracking-widest uppercase text-slate-400 w-[45%] py-6"
            >
              Opportunity Details
            </SortableHead>
            <SortableHead
              field="priority_score"
              sortBy={sortBy}
              sortOrder={sortOrder}
              onSort={onSort}
              className="text-xs font-semibold tracking-widest uppercase text-slate-400 w-[100px] py-6"
            >
              AI Score
            </SortableHead>
            <TableHead className="w-[150px] text-xs font-semibold tracking-widest uppercase text-slate-400 py-6">Sector</TableHead>
            <TableHead className="w-[140px] text-xs font-semibold tracking-widest uppercase text-slate-400 py-6">Timeline</TableHead>
            <TableHead className="w-[70px] text-center text-xs font-semibold tracking-widest uppercase text-slate-400 py-6 pr-8">Action</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {tenders.length === 0 && (
            <TableRow className="border-0 hover:bg-transparent">
              <TableCell colSpan={6} className="py-32 text-center">
                <div className="flex flex-col items-center gap-4">
                  <div className="h-16 w-16 bg-slate-100 rounded-full flex items-center justify-center animate-pulse border border-slate-200">
                    <span className="text-3xl opacity-20 text-slate-900">◉</span>
                  </div>
                  <span className="text-xl font-semibold text-slate-800 tracking-tight">No active tenders found.</span>
                  <span className="text-sm font-medium text-slate-400">Try loosening your filters or changing the sector and region.</span>
                </div>
              </TableCell>
            </TableRow>
          )}
          {tenders.map((t) => {
            const score = t.priority_score ?? 0;
            const isHighMatch = score >= 80;

            return (
              <TableRow
                key={t.tender_id}
                className={cn(
                  "cursor-pointer group border-b border-gray-100 transition-all duration-300",
                  "hover:bg-slate-50",
                )}
                onClick={() => onTenderClick(t.tender_id)}
              >
                {/* Portal */}
                <TableCell className="py-6 pl-8">
                  <span className="inline-block bg-slate-100/80 border border-slate-200 text-slate-500 font-bold uppercase tracking-widest text-[10px] px-3 py-1.5 rounded-lg shadow-sm">
                    {portalLabel(t.source_site)}
                  </span>
                </TableCell>

                {/* Title + Org subtitle */}
                <TableCell className="py-6 pr-6">
                  <div className="flex flex-col gap-2">
                    <span className="line-clamp-2 text-base md:text-[17px] font-semibold leading-snug text-slate-900 group-hover:text-slate-700 transition-colors">
                      {t.title}
                    </span>
                    <div className="flex items-center gap-3">
                      <span className="text-xs font-medium text-slate-500 uppercase tracking-widest bg-slate-50 px-2 py-1 rounded inline-block border border-slate-100">
                        {t.organization && t.organization.toLowerCase() !== "unknown" ? t.organization : "General Org"}
                      </span>
                      {isHighMatch && (
                        <span className="text-[10px] font-semibold text-emerald-700 bg-emerald-50 border border-emerald-200 px-2 py-1 rounded uppercase tracking-wider">
                          Highly Recommended
                        </span>
                      )}
                    </div>
                  </div>
                </TableCell>

                {/* Priority score */}
                <TableCell className="py-6">
                  <span className={cn(
                    "inline-flex items-center justify-center px-4 py-2 font-semibold text-sm rounded-xl shadow-sm transition-transform group-hover:scale-105",
                    score >= 80
                      ? "bg-slate-900 text-white border-none shadow-sm"
                      : score >= 60
                      ? "bg-slate-700 text-white border-none"
                      : "bg-slate-100 text-slate-500 border border-slate-200",
                  )}>
                    {score}%
                  </span>
                </TableCell>

                {/* Sector */}
                <TableCell className="py-6">
                  {t.sector && t.sector !== "unknown"
                    ? <span className="text-xs font-medium text-slate-700 bg-slate-100 border border-slate-200 px-3 py-1.5 rounded-full uppercase tracking-widest inline-block shadow-sm">{t.sector.replace("_", " ")}</span>
                    : (
                      <span className="inline-flex items-center rounded-full px-3 py-1.5 text-[10px] font-bold uppercase tracking-widest border text-slate-400 border-slate-200 bg-slate-50">
                        Not Classified
                      </span>
                    )
                  }
                </TableCell>

                {/* Deadline */}
                <TableCell className="py-6">
                  <DeadlineCategoryChip cat={t.deadline_category} />
                </TableCell>

                {/* External link / Action */}
                <TableCell className="py-6 text-center pr-8">
                  <div className="w-10 h-10 rounded-full border border-slate-200 flex items-center justify-center mx-auto text-slate-400 group-hover:border-slate-900 group-hover:text-white group-hover:bg-slate-900 group-hover:shadow-md transition-all">
                    <ArrowRight className="h-5 w-5" />
                  </div>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}
