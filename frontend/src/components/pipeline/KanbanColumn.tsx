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
        "sticky top-0 z-10 flex items-center justify-between rounded-lg border px-3 py-2",
        color, accent,
      )}>
        <span className="text-sm font-semibold">{label}</span>
        <span className="flex h-5 w-5 items-center justify-center rounded-full bg-background text-[11px] font-bold tabular-nums shadow-sm">
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
                ? "bg-primary/5 ring-2 ring-primary/20"
                : "bg-muted/30",
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
              <div className="flex h-20 items-center justify-center rounded-lg border-2 border-dashed border-muted-foreground/20 text-[11px] text-muted-foreground/50">
                Drop here
              </div>
            )}
          </div>
        )}
      </Droppable>
    </div>
  );
}
