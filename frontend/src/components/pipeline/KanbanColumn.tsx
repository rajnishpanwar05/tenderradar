"use client";

import { Droppable } from "@hello-pangea/dnd";
import type { PipelineEntry } from "@/lib/api-types";
import type { PipelineColId } from "@/lib/constants";
import { PipelineCard } from "./PipelineCard";
import { cn } from "@/lib/utils";

interface Props {
  id:       PipelineColId;
  label:    string;
  color:    string;
  accent:   string;
  entries:  PipelineEntry[];
  onEdit?:  (entry: PipelineEntry) => void;
}

export function KanbanColumn({ id, label, color, accent, entries, onEdit }: Props) {
  return (
    <div className="flex w-64 shrink-0 flex-col gap-2 lg:w-[220px] xl:w-60">
      {/* Column header */}
      <div className={cn(
        "sticky top-0 z-10 flex items-center justify-between rounded-lg border px-3 py-2 bg-white",
        color, accent,
      )}>
        <span className="text-sm font-semibold text-slate-900">{label}</span>
        <span className="flex h-5 w-5 items-center justify-center rounded-full bg-slate-100 text-[11px] font-semibold tabular-nums text-slate-700 shadow-sm">
          {entries.length}
        </span>
      </div>

      {/* Drop zone */}
      <Droppable droppableId={id}>
        {(provided, snapshot) => (
          <div
            ref={provided.innerRef}
            {...provided.droppableProps}
            className={cn(
              "flex min-h-[120px] flex-col gap-2 rounded-xl p-1.5 transition-colors",
              snapshot.isDraggingOver
                ? "bg-slate-100 ring-2 ring-slate-300"
                : "bg-slate-50/80",
            )}
          >
            {entries.map((entry, i) => (
              <PipelineCard
                key={entry.tender_id}
                entry={entry}
                index={i}
                onEdit={onEdit}
              />
            ))}
            {provided.placeholder}

            {entries.length === 0 && !snapshot.isDraggingOver && (
              <div className="flex h-20 items-center justify-center rounded-lg border-2 border-dashed border-slate-200 text-[11px] text-slate-400">
                Drop here
              </div>
            )}
          </div>
        )}
      </Droppable>
    </div>
  );
}
