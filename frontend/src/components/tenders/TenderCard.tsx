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
  s >= 75 ? "text-amber-400 border-amber-500/30 bg-amber-500/10"
  : s >= 60 ? "text-blue-400 border-blue-500/30 bg-blue-500/10"
  : "text-white/40 border-white/[0.1] bg-white/[0.04]";

const SCORE_GLOW = (s: number) =>
  s >= 75 ? "hover:glow-amber"
  : s >= 60 ? "hover:glow-blue"
  : "";

export function TenderCard({ tender, onClick }: TenderCardProps) {
  const score = tender.fit_score ?? 0;

  return (
    <div
      className={cn(
        "group relative flex flex-col rounded-xl border border-white/[0.07] bg-white/[0.025]",
        "p-4 transition-all duration-200 overflow-hidden",
        "hover:bg-white/[0.045] hover:border-white/[0.12]",
        score >= 75 && "border-amber-500/15 hover:border-amber-500/25",
        score >= 60 && score < 75 && "border-blue-500/10 hover:border-blue-500/20",
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
            background: score >= 75
              ? "linear-gradient(90deg, #f59e0b, #f97316)"
              : score >= 60
              ? "linear-gradient(90deg, #3b82f6, #06b6d4)"
              : "rgba(255,255,255,0.12)"
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
            className="p-1.5 rounded-lg text-white/20 opacity-0 group-hover:opacity-100 transition-all hover:bg-white/[0.08] hover:text-white/60"
            title="Open tender"
          >
            <ExternalLink className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      {/* Title */}
      <h3 className="mb-1.5 line-clamp-2 text-sm font-semibold leading-snug text-white/85">
        {tender.title_clean || tender.title}
      </h3>

      {/* Org + country */}
      {(tender.organization || tender.country) && (
        <div className="flex items-center gap-1.5 mb-3">
          <Building2 className="w-3 h-3 text-white/20 flex-shrink-0" />
          <p className="text-xs text-white/35 line-clamp-1">
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
      <div className="mt-auto pt-3 border-t border-white/[0.06] flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5">
          <Calendar className="w-3 h-3 text-white/20" />
          <DeadlineChip deadline={tender.deadline} isExpired={tender.is_expired} />
        </div>
        <FitBucketBadge bucket={tender.fit_bucket} score={tender.fit_score} showScore={false} />
      </div>
    </div>
  );
}
