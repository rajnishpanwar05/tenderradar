"use client";
import { ExternalLink, Calendar, Building2 } from "lucide-react";
import { cn } from "@/lib/utils";
import type { TenderSearchResult } from "@/lib/api-types";
import { FitBucketBadge } from "./FitBucketBadge";
import { SectorBadge } from "./SectorBadge";
import { DeadlineChip } from "./DeadlineChip";
import { PortalIcon } from "./PortalIcon";
import { handleTenderClick } from "@/lib/tender-links";

interface TenderCardProps {
  tender: TenderSearchResult;
  onClick?: () => void;
}

const SCORE_COLOR = (s: number) =>
  s >= 75 ? "text-slate-900 border-slate-300 bg-slate-100"
  : s >= 60 ? "text-slate-700 border-slate-300 bg-slate-50"
  : "text-slate-500 border-slate-200 bg-white";

const SCORE_GLOW = (s: number) =>
  s >= 75 ? ""
  : s >= 60 ? ""
  : "";

export function TenderCard({ tender, onClick }: TenderCardProps) {
  const score = tender.fit_score ?? 0;

  return (
    <div
      className={cn(
        "group relative flex flex-col rounded-2xl border border-slate-200 bg-white",
        "p-4 transition-all duration-200 overflow-hidden shadow-sm",
        "hover:bg-slate-50 hover:border-slate-300 hover:shadow-md",
        score >= 75 && "border-slate-300",
        score >= 60 && score < 75 && "border-slate-200",
        onClick && "cursor-pointer",
        SCORE_GLOW(score)
      )}
      onClick={onClick}
      role={onClick ? "button" : undefined}
      tabIndex={onClick ? 0 : undefined}
    >
      {/* Score bar — top edge */}
      {score > 0 && (
        <div className="absolute top-0 left-0 h-[2px] rounded-t-xl transition-all duration-300"
          style={{
            width: `${score}%`,
            background: "linear-gradient(90deg, #0f172a, #475569)"
          }}
        />
      )}

      {/* Header row */}
      <div className="flex items-start justify-between gap-2 mb-3">
        <PortalIcon portal={tender.source_portal} showLabel />
        <div className="flex items-center gap-2 flex-shrink-0">
          {/* Score badge */}
          <div className={cn(
            "flex items-center justify-center w-9 h-9 rounded-lg border font-bold text-sm font-mono",
            SCORE_COLOR(score)
          )}>
            {score}
          </div>
          <button
            type="button"
            onClick={e => handleTenderClick(e, tender)}
            className="p-1.5 rounded-lg text-slate-400 opacity-0 group-hover:opacity-100 transition-all hover:bg-slate-100 hover:text-slate-900"
            title="Open tender"
          >
            <ExternalLink className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      {/* Title */}
      <h3 className="mb-1.5 line-clamp-2 text-sm font-semibold leading-snug text-slate-900">
        {tender.title_clean || tender.title}
      </h3>

      {/* Org + country */}
      {(tender.organization || tender.country) && (
        <div className="flex items-center gap-1.5 mb-3">
          <Building2 className="w-3 h-3 text-slate-400 flex-shrink-0" />
          <p className="text-xs text-slate-500 line-clamp-1">
            {[tender.organization, tender.country].filter(Boolean).join(" · ")}
          </p>
        </div>
      )}

      {/* Sector */}
      {tender.primary_sector && (
        <div className="mb-3">
          <SectorBadge sector={tender.primary_sector} />
        </div>
      )}

      {/* Footer */}
      <div className="mt-auto pt-3 border-t border-slate-200 flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5">
          <Calendar className="w-3 h-3 text-slate-400" />
          <DeadlineChip deadline={tender.deadline} isExpired={tender.is_expired} />
        </div>
        <FitBucketBadge bucket={tender.fit_bucket} score={tender.fit_score} showScore={false} />
      </div>
    </div>
  );
}
