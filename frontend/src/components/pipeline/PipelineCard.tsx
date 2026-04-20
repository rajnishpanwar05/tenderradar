"use client";

import { ExternalLink, User, Calendar, Zap } from "lucide-react";
import { Draggable } from "@hello-pangea/dnd";
import type { PipelineEntry } from "@/lib/api-types";
import { PriorityBadge } from "@/components/tenders/PriorityBadge";
import { sectorLabel } from "@/lib/constants";
import { cn } from "@/lib/utils";
import { useRouter } from "next/navigation";
import { handleTenderClick } from "@/lib/tender-links";

interface Props {
  entry:   PipelineEntry;
  index:   number;
  onEdit?: (entry: PipelineEntry) => void;
}

export function PipelineCard({ entry, index, onEdit }: Props) {
  const router = useRouter();

  const truncatedTitle = entry.title.length > 80
    ? entry.title.slice(0, 80) + "…"
    : entry.title;

  return (
    <Draggable draggableId={entry.tender_id} index={index}>
      {(provided, snapshot) => (
        <div
          ref={provided.innerRef}
          {...provided.draggableProps}
          {...provided.dragHandleProps}
          className={cn(
            "group relative flex flex-col gap-2 rounded-xl border bg-card p-3.5 shadow-sm",
            "transition-shadow cursor-grab active:cursor-grabbing select-none",
            snapshot.isDragging
              ? "shadow-lg ring-2 ring-primary/30 rotate-1"
              : "hover:shadow-md hover:border-primary/30",
          )}
          onClick={() => router.push(`/tenders/${encodeURIComponent(entry.tender_id)}`)}
        >
          {/* Priority badge + external link */}
          <div className="flex items-center justify-between gap-2">
            <PriorityBadge score={entry.priority_score} size="sm" />
            {entry.url && (
              <button
                type="button"
                onClick={(e) => handleTenderClick(e as any, entry)}
                className="text-muted-foreground opacity-0 group-hover:opacity-100 hover:text-foreground transition-opacity"
                aria-label="Open tender"
              >
                <ExternalLink className="h-3.5 w-3.5" />
              </button>
            )}
          </div>

          {/* Title */}
          <p className="text-[13px] font-medium leading-snug line-clamp-2">
            {truncatedTitle}
          </p>

          {/* Organization */}
          {entry.organization && entry.organization !== "unknown" && (
            <p className="text-[11px] text-muted-foreground truncate">
              {entry.organization}
            </p>
          )}

          {/* Sector chip */}
          {entry.sector && entry.sector !== "unknown" && (
            <span className="inline-flex w-fit items-center rounded-full bg-primary/8 px-2 py-0.5 text-[10px] font-medium text-primary">
              {sectorLabel(entry.sector)}
            </span>
          )}

          {/* Footer row */}
          <div className="mt-0.5 flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground">
            {entry.owner && (
              <span className="flex items-center gap-1">
                <User className="h-3 w-3" />
                {entry.owner}
              </span>
            )}
            {entry.proposal_deadline && (
              <span className="flex items-center gap-1">
                <Calendar className="h-3 w-3" />
                {new Date(entry.proposal_deadline).toLocaleDateString("en-GB", {
                  day: "numeric", month: "short", year: "numeric",
                })}
              </span>
            )}
          </div>

          {/* Insight strip */}
          {entry.opportunity_insight && (
            <p className="mt-0.5 flex gap-1 text-[11px] italic text-muted-foreground line-clamp-1">
              <Zap className="h-3 w-3 shrink-0 mt-0.5 text-amber-400" />
              {entry.opportunity_insight}
            </p>
          )}

          {/* Edit button */}
          {onEdit && (
            <button
              onClick={(e) => { e.stopPropagation(); onEdit(entry); }}
              className="absolute bottom-3 right-3 hidden group-hover:flex items-center gap-1 rounded-md border bg-background px-2 py-0.5 text-[11px] text-muted-foreground hover:text-foreground transition-colors shadow-sm"
            >
              Edit
            </button>
          )}
        </div>
      )}
    </Draggable>
  );
}
