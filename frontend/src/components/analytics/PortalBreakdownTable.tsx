"use client";

import { useState } from "react";
import { ChevronUp, ChevronDown } from "lucide-react";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { PortalFreshnessIndicator } from "@/components/portals/PortalFreshnessIndicator";
import { FitScoreBar } from "@/components/tenders/FitScoreBar";
import { portalLabel } from "@/lib/constants";
import type { PortalStats } from "@/lib/api-types";

type SortKey = keyof PortalStats;

interface PortalBreakdownTableProps {
  portals: PortalStats[];
}

export function PortalBreakdownTable({ portals }: PortalBreakdownTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("total_tenders");
  const [sortAsc, setSortAsc] = useState(false);

  const sorted = [...portals].sort((a, b) => {
    const av = a[sortKey] ?? 0;
    const bv = b[sortKey] ?? 0;
    const cmp = av < bv ? -1 : av > bv ? 1 : 0;
    return sortAsc ? cmp : -cmp;
  });

  const handleSort = (key: SortKey) => {
    if (key === sortKey) setSortAsc(p => !p);
    else { setSortKey(key); setSortAsc(false); }
  };

  const SortIcon = ({ k }: { k: SortKey }) =>
    sortKey === k
      ? sortAsc ? <ChevronUp className="w-4 h-4 ml-1 inline text-indigo-500" /> : <ChevronDown className="w-4 h-4 ml-1 inline text-indigo-500" />
      : null;

  const TH = ({ children, k }: { children: React.ReactNode; k: SortKey }) => (
    <TableHead
      className="cursor-pointer select-none whitespace-nowrap text-slate-500 font-black tracking-widest uppercase text-xs hover:bg-slate-50 transition-colors py-4"
      onClick={() => handleSort(k)}
    >
      {children}<SortIcon k={k} />
    </TableHead>
  );

  return (
    <Table>
      <TableHeader className="bg-white">
        <TableRow className="border-b border-slate-100">
          <TH k="portal">Portal</TH>
          <TH k="total_tenders">Total</TH>
          <TH k="new_last_7_days">New (7d)</TH>
          <TH k="avg_fit_score">Avg Fit</TH>
          <TH k="high_fit_count">High Match</TH>
          <TH k="last_scraped_at">Last Scraped</TH>
        </TableRow>
      </TableHeader>
      <TableBody>
        {sorted.map(p => (
          <TableRow key={p.portal} className="border-b border-slate-100 hover:bg-slate-50/50 transition-colors">
            <TableCell className="font-bold text-slate-800 py-4">{portalLabel(p.portal)}</TableCell>
            <TableCell className="font-mono text-slate-600">{p.total_tenders.toLocaleString()}</TableCell>
            <TableCell className="font-mono font-bold text-emerald-600">+{p.new_last_7_days}</TableCell>
            <TableCell>
              <div className="flex items-center gap-3 min-w-[120px]">
                <FitScoreBar label="" score={p.avg_fit_score} className="flex-1" />
                <span className="text-xs font-black text-slate-500 w-8 text-right font-mono">
                  {p.avg_fit_score.toFixed(0)}
                </span>
              </div>
            </TableCell>
            <TableCell>
              <span className="bg-indigo-50 text-indigo-600 px-2.5 py-1 rounded-md font-bold text-xs inline-flex items-center justify-center min-w-[2rem]">
                {p.high_fit_count}
              </span>
            </TableCell>
            <TableCell>
              <PortalFreshnessIndicator lastScrapedAt={p.last_scraped_at} showLabel />
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
