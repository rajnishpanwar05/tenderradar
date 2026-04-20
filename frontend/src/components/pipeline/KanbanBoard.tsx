"use client";

import { useState, useCallback } from "react";
import { DragDropContext, type DropResult } from "@hello-pangea/dnd";
import type { PipelineEntry, PipelineStatus, PipelineUpdateRequest } from "@/lib/api-types";
import { PIPELINE_COLUMNS, type PipelineColId } from "@/lib/constants";
import { KanbanColumn } from "./KanbanColumn";
import { EditEntryModal } from "./EditEntryModal";
import { apiClient } from "@/lib/api";
import { toast } from "sonner";

interface Props {
  entries:  PipelineEntry[];
  onMutate: () => void;
}

export function KanbanBoard({ entries, onMutate }: Props) {
  const [optimistic, setOptimistic] = useState<PipelineEntry[]>(entries);
  const [editTarget, setEditTarget] = useState<PipelineEntry | null>(null);

  // Keep local state in sync when SWR refetches
  const currentEntries = optimistic.length > 0 ? optimistic : entries;

  // Build columns map
  const columns: Record<PipelineColId, PipelineEntry[]> = {
    discovered:           [],
    shortlisted:          [],
    proposal_in_progress: [],
    submitted:            [],
    won:                  [],
    lost:                 [],
  };
  for (const e of currentEntries) {
    const col = columns[e.status as PipelineColId];
    if (col) col.push(e);
  }

  const handleDragEnd = useCallback(async (result: DropResult) => {
    const { source, destination, draggableId } = result;
    if (!destination) return;
    if (
      source.droppableId === destination.droppableId &&
      source.index === destination.index
    ) return;

    const newStatus = destination.droppableId as PipelineStatus;

    // Optimistic update
    setOptimistic(prev =>
      prev.map(e =>
        e.tender_id === draggableId ? { ...e, status: newStatus } : e
      )
    );

    try {
      await apiClient.client.updatePipeline({
        tender_id: draggableId,
        status:    newStatus,
      } satisfies PipelineUpdateRequest);

      // Record outcome for won/lost moves — feeds the ML feedback loop
      if (newStatus === "won" || newStatus === "lost") {
        try {
          await apiClient.client.recordOutcome(draggableId, newStatus, "bid");
        } catch {
          // Non-fatal — outcome recording failure should never block the UI
        }
      }

      onMutate();
    } catch {
      // Revert on failure
      setOptimistic(entries);
      toast.error("Failed to move card — please try again");
    }
  }, [entries, onMutate]);

  const handleEdit = useCallback((entry: PipelineEntry) => {
    setEditTarget(entry);
  }, []);

  const handleSaveEdit = useCallback(async (req: PipelineUpdateRequest) => {
    try {
      await apiClient.client.updatePipeline(req);
      toast.success("Pipeline entry updated");
      setEditTarget(null);
      onMutate();
    } catch {
      toast.error("Failed to save changes");
    }
  }, [onMutate]);

  return (
    <>
      <DragDropContext onDragEnd={handleDragEnd}>
        <div className="flex gap-3 overflow-x-auto pb-6">
          {PIPELINE_COLUMNS.map((col) => (
            <KanbanColumn
              key={col.id}
              id={col.id}
              label={col.label}
              color={col.color}
              accent={col.accent}
              entries={columns[col.id]}
              onEdit={handleEdit}
            />
          ))}
        </div>
      </DragDropContext>

      {editTarget && (
        <EditEntryModal
          entry={editTarget}
          onClose={() => setEditTarget(null)}
          onSave={handleSaveEdit}
        />
      )}
    </>
  );
}
