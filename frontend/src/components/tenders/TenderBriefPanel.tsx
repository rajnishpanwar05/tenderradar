"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  Sparkles,
  Loader2,
  AlertTriangle,
  ExternalLink,
  CheckCircle2,
  ListChecks,
} from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { apiClient } from "@/lib/api";
import type { CopilotResponse, TenderRecord } from "@/lib/api-types";
import { cn } from "@/lib/utils";
import { handleTenderClick } from "@/lib/tender-links";

type PanelState = "loading" | "done" | "error";

interface TenderBriefPanelProps {
  tender: TenderRecord;
}

function safeList(val: unknown): string[] {
  if (Array.isArray(val)) {
    return val.map((v) => String(v)).filter(Boolean);
  }
  return [];
}

export function TenderBriefPanel({ tender }: TenderBriefPanelProps) {
  const [state, setState] = useState<PanelState>("loading");
  const [result, setResult] = useState<CopilotResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pipelineMsg, setPipelineMsg] = useState<string | null>(null);

  const loadBrief = useCallback(async () => {
    setState("loading");
    setError(null);
    try {
      const data = await apiClient.client.getCopilotRecommendation(tender.tender_id, "deep");
      setResult(data);
      setState("done");
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to load AI summary.");
      setState("error");
    }
  }, [tender.tender_id]);

  useEffect(() => {
    loadBrief();
  }, [loadBrief]);

  const extraction = (result?.extraction || {}) as Record<string, unknown>;
  const summary = String(extraction.scope_summary || "").trim();
  const deliverables = safeList(extraction.key_deliverables);
  const nextSteps = result?.strategy ?? [];

  const fallbackSummary = useMemo(() => {
    if (summary) return summary;
    const desc = tender.description || tender.deep_scope || "";
    return desc ? desc.slice(0, 900) : "";
  }, [summary, tender.description, tender.deep_scope]);

  async function handleAddToPipeline() {
    setPipelineMsg(null);
    try {
      await apiClient.client.updatePipeline({
        tender_id: tender.tender_id,
        status: "discovered",
      });
      setPipelineMsg("Added to pipeline.");
    } catch (err: unknown) {
      setPipelineMsg(err instanceof Error ? err.message : "Failed to add to pipeline.");
    }
  }

  return (
    <Card className="shadow-sm">
      <CardContent className="p-5 space-y-4">
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-slate-700" />
          <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            AI Summary
          </h3>
        </div>

        {state === "loading" && (
          <div className="rounded-lg border border-slate-200 bg-white p-4 text-center">
            <Loader2 className="mx-auto h-5 w-5 animate-spin text-slate-700" />
            <p className="mt-2 text-xs text-slate-500">
              Generating a bid-ready summary…
            </p>
          </div>
        )}

        {state === "error" && (
          <div className="rounded-lg border border-slate-200 bg-white p-3 text-xs text-slate-600">
            <div className="flex items-start gap-2">
              <AlertTriangle className="mt-0.5 h-4 w-4 text-slate-500" />
              <div>
                <p className="font-semibold">AI brief unavailable</p>
                <p className="mt-1 text-slate-500">{error}</p>
                <Button variant="outline" size="sm" className="mt-2" onClick={loadBrief}>
                  Retry
                </Button>
              </div>
            </div>
          </div>
        )}

        {state === "done" && (
          <div className="space-y-3 text-sm">
            <div>
              <p className="font-semibold">Summary</p>
              <p className={cn("mt-1 text-slate-500", !fallbackSummary && "italic")}>
                {fallbackSummary || "No summary available yet."}
              </p>
            </div>

            {deliverables.length > 0 && (
              <div>
                <p className="font-semibold">Key Deliverables</p>
                <ul className="mt-1 space-y-1">
                  {deliverables.map((item, i) => (
                    <li key={i} className="flex items-start gap-2 text-slate-500">
                      <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 text-slate-700" />
                      <span>{item}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {nextSteps.length > 0 && (
              <div>
                <p className="font-semibold">Next Steps</p>
                <ul className="mt-1 space-y-1">
                  {nextSteps.map((step, i) => (
                    <li key={i} className="flex items-start gap-2 text-slate-500">
                      <ListChecks className="mt-0.5 h-3.5 w-3.5 text-slate-700" />
                      <span>{step}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}

        <div className="space-y-2 pt-1">
          <Button className="w-full bg-slate-900 text-white hover:bg-slate-800" onClick={(e) => handleTenderClick(e, tender)}>
            <ExternalLink className="mr-2 h-4 w-4" />
            Open Official Tender
          </Button>
          <Button variant="outline" className="w-full border-slate-200 bg-white text-slate-700 hover:bg-slate-50" onClick={handleAddToPipeline}>
            Add to Pipeline
          </Button>
          {pipelineMsg && (
            <p className="text-xs text-slate-500">{pipelineMsg}</p>
          )}
          <Link
            href="/pipeline"
            className="inline-flex w-full items-center justify-center rounded-md border border-slate-200 bg-white px-3 py-2 text-xs font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-50 transition-colors"
          >
            Open Project Workspace
          </Link>
        </div>
      </CardContent>
    </Card>
  );
}
