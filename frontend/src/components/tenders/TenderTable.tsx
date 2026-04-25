"use client";
import { ChevronUp, ChevronDown, ArrowRight } from "lucide-react";
import { cn } from "@/lib/utils";
import type { TenderIntelItem } from "@/lib/api-types";
import { portalLabel, sectorLabel } from "@/lib/constants";

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
    <th
      className={cn("cursor-pointer select-none text-left px-4 py-3 text-xs font-medium text-slate-500 uppercase tracking-wide", className)}
      onClick={() => onSort(field)}
    >
      <span className="inline-flex items-center gap-1 hover:text-slate-800 transition-colors">
        {children}
        {active
          ? sortOrder === "asc"
            ? <ChevronUp className="h-3.5 w-3.5 text-slate-600" />
            : <ChevronDown className="h-3.5 w-3.5 text-slate-600" />
          : <ChevronDown className="h-3.5 w-3.5 text-slate-300" />
        }
      </span>
    </th>
  );
}

export function TenderTable({
  tenders, sortBy, sortOrder, onSort, onTenderClick,
}: TenderTableProps) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-slate-50 border-b border-slate-200">
            <th className="text-left px-4 py-3 text-xs font-medium text-slate-500 uppercase tracking-wide w-32">Portal</th>
            <SortableHead field="title" sortBy={sortBy} sortOrder={sortOrder} onSort={onSort} className="w-auto">
              Title
            </SortableHead>
            <SortableHead field="priority_score" sortBy={sortBy} sortOrder={sortOrder} onSort={onSort} className="w-24">
              Score
            </SortableHead>
            <th className="text-left px-4 py-3 text-xs font-medium text-slate-500 uppercase tracking-wide w-36">Sector</th>
            <th className="text-left px-4 py-3 text-xs font-medium text-slate-500 uppercase tracking-wide w-28">Status</th>
            <th className="w-10" />
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {tenders.length === 0 && (
            <tr>
              <td colSpan={6} className="py-20 text-center text-sm text-slate-500">
                No tenders found. Try adjusting your filters.
              </td>
            </tr>
          )}
          {tenders.map((t) => {
            const score = t.priority_score ?? 0;
            return (
              <tr
                key={t.tender_id}
                className="bg-white hover:bg-slate-50 cursor-pointer transition-colors"
                onClick={() => onTenderClick(t.tender_id)}
              >
                {/* Portal */}
                <td className="px-4 py-3.5">
                  <PortalBadge name={t.source_site} />
                </td>

                {/* Title */}
                <td className="px-4 py-3.5">
                  <div className="space-y-0.5">
                    <p className="font-medium text-slate-900 line-clamp-2 leading-snug">
                      {t.title}
                    </p>
                    {t.organization && t.organization.toLowerCase() !== "unknown" && (
                      <p className="text-xs text-slate-500 truncate">{t.organization}</p>
                    )}
                  </div>
                </td>

                {/* Score */}
                <td className="px-4 py-3.5">
                  <ScoreBadge score={score} />
                </td>

                {/* Sector */}
                <td className="px-4 py-3.5">
                  {t.sector && t.sector !== "unknown" ? (
                    <span className="text-xs text-slate-600">
                      {sectorLabel(t.sector)}
                    </span>
                  ) : (
                    <span className="text-xs text-slate-400">—</span>
                  )}
                </td>

                {/* Status / deadline */}
                <td className="px-4 py-3.5">
                  <DeadlineChip cat={t.deadline_category} />
                </td>

                {/* Action */}
                <td className="px-3 py-3.5">
                  <ArrowRight className="h-4 w-4 text-slate-300" />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const PORTAL_COLORS: Record<string, string> = {
  gem:    "bg-blue-50 text-blue-700 border-blue-200",
  ungm:   "bg-violet-50 text-violet-700 border-violet-200",
  undp:   "bg-cyan-50 text-cyan-700 border-cyan-200",
  wb:     "bg-emerald-50 text-emerald-700 border-emerald-200",
  adb:    "bg-orange-50 text-orange-700 border-orange-200",
  afdb:   "bg-amber-50 text-amber-700 border-amber-200",
  eu:     "bg-indigo-50 text-indigo-700 border-indigo-200",
  usaid:  "bg-red-50 text-red-700 border-red-200",
};

function PortalBadge({ name }: { name: string }) {
  const key = (name || "").toLowerCase().replace(/[^a-z]/g, "");
  const cls = Object.entries(PORTAL_COLORS).find(([k]) => key.includes(k))?.[1]
    ?? "bg-slate-100 text-slate-600 border-slate-200";
  return (
    <span className={cn("text-[10px] font-semibold uppercase tracking-wide px-2 py-0.5 rounded border whitespace-nowrap", cls)}>
      {portalLabel(name)}
    </span>
  );
}

function ScoreBadge({ score }: { score: number }) {
  return (
    <span className={cn(
      "inline-flex items-center justify-center w-10 h-7 text-xs font-bold rounded border",
      score >= 80 ? "bg-emerald-50 text-emerald-700 border-emerald-200" :
      score >= 60 ? "bg-amber-50 text-amber-700 border-amber-200" :
      "bg-slate-100 text-slate-600 border-slate-200"
    )}>
      {score}
    </span>
  );
}

function DeadlineChip({ cat }: { cat: string }) {
  const cfg = {
    urgent:  { label: "Urgent",  cls: "bg-red-50 text-red-700 border-red-200" },
    soon:    { label: "Soon",    cls: "bg-amber-50 text-amber-700 border-amber-200" },
    normal:  { label: "Normal",  cls: "bg-slate-100 text-slate-600 border-slate-200" },
    unknown: { label: "Unknown", cls: "bg-slate-100 text-slate-400 border-slate-200" },
  }[cat] ?? { label: cat, cls: "bg-slate-100 text-slate-500 border-slate-200" };

  return (
    <span className={cn("inline-flex items-center px-2 py-0.5 text-xs font-medium border rounded", cfg.cls)}>
      {cfg.label}
    </span>
  );
}
